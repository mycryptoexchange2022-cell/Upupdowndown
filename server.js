// ============================================================
//  BTC LADDER BOT — server.js  v4 (PRICE FIX)
//  Polymarket 15-min BTC Up/Down Windows
//
//  Strategy: previous window winner → active ladder next window
//  Entry 0.05–0.90 | Buy 100 shares every -0.05 drop
//  Sell all at avg+0.10 | Re-entry at lastSell-0.05
//
//  FIXES v4:
//  1. CLOB book bids sorted desc — pick bids[0] (highest bid)
//  2. Gamma API /markets/{id} used as 2nd price source
//  3. WS message handles both price-tick AND book-snapshot formats
//  4. Market discovery uses /markets?active=true broad search + slug filter
//  5. Sim price fallback kicks in immediately when no real prices in 5s
//  6. REST poll adds gamma midpoint as secondary when CLOB fails
//  7. Dashboard: price change % fixed (delta/prev*100)
//  8. Chart y-axis auto-scales around real data
//  9. WS subscription correct assets_ids format
// 10. Realistic demo sim: realistic BTC binary market prices (0.35-0.65 range)
// ============================================================

'use strict';

const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const path      = require('path');
const cors      = require('cors');
const fetch     = require('node-fetch');

// ============================================================
//  CONSTANTS
// ============================================================
const DEMO_CAPITAL      = 2000;
const ENTRY_MIN         = 0.05;
const ENTRY_MAX         = 0.90;
const BUY_STEP          = 0.05;
const SELL_PROFIT       = 0.10;
const REENTRY_STEP      = 0.05;
const SHARES_PER_BUY    = 100;

const WS_PING_MS        = 20_000;
const REST_POLL_MS      = 4_000;
const GAMMA_POLL_MS     = 6_000;   // secondary price source
const SIM_FALLBACK_MS   = 8_000;   // start sim if no real price within this
const WS_RECONNECT_BASE = 3_000;
const WS_RECONNECT_MAX  = 30_000;

const POLY_API = 'https://gamma-api.polymarket.com';
const CLOB_API = 'https://clob.polymarket.com';
const POLY_WS  = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';

// ============================================================
//  STATE
// ============================================================
let S = {
  capital:         DEMO_CAPITAL,
  startCapital:    DEMO_CAPITAL,
  totalPnl:        0,
  windowsTraded:   0,
  wins:            0,
  losses:          0,
  lastResolution:  null,
  currentWindow:   null,
  nextWindow:      null,
  upLadder:        null,
  downLadder:      null,
  upPrice:         null,
  downPrice:       null,
  upTokenId:       null,
  downTokenId:     null,
  windowStatus:    'searching',
  windowHistory:   [],
  tradeLog:        [],
  priceHistory:    { up: [], down: [] },
  lastPriceUpdate: null,
  botStatus:       'starting',
  wsConnected:     false,
  wsReconnects:    0,
  errors:          [],
  activeLadders:   'both',
  priceSource:     'none',   // 'ws' | 'clob' | 'gamma' | 'sim'
};

let polyWs         = null;
let wsAlive        = false;
let wsPingTimer    = null;
let priceTimer     = null;
let gammaPriceTimer= null;
let simTimer       = null;
let simFallbackTimer = null;
let dashClients    = new Set();
let simPrice       = { up: 0.52, down: 0.48 };

// ============================================================
//  LADDER HELPERS
// ============================================================
function makeLadder(side) {
  return { side, positions: [], lastSoldPrice: null, totalShares: 0, totalCost: 0 };
}

function ladderAvg(l) {
  return l.totalShares === 0 ? 0 : l.totalCost / l.totalShares;
}

function ladderNextBuy(l) {
  if (l.positions.length === 0) return null;
  const lowest = Math.min(...l.positions.map(p => p.price));
  return parseFloat((lowest - BUY_STEP).toFixed(4));
}

function ladderReEntry(l) {
  if (l.lastSoldPrice === null) return ENTRY_MAX;
  return parseFloat((l.lastSoldPrice - REENTRY_STEP).toFixed(4));
}

// ============================================================
//  TICK ONE LADDER
// ============================================================
function tickLadder(ladder, price, capital) {
  const actions = [];
  price = parseFloat(price.toFixed(4));

  // SELL: price >= avg + SELL_PROFIT
  if (ladder.totalShares > 0) {
    const avg = ladderAvg(ladder);
    if (price >= parseFloat((avg + SELL_PROFIT).toFixed(4))) {
      const proceeds = ladder.totalShares * price;
      const pnl      = proceeds - ladder.totalCost;
      capital += proceeds;
      actions.push({
        type: 'SELL', side: ladder.side.toUpperCase(), price,
        shares: ladder.totalShares, avg: parseFloat(avg.toFixed(4)),
        pnl: parseFloat(pnl.toFixed(2)),
        msg: `✅ SELL ALL (${ladder.side.toUpperCase()}): ${ladder.totalShares} sh @ $${price} | avg $${avg.toFixed(4)} | PnL $${pnl.toFixed(2)}`,
      });
      ladder.lastSoldPrice = price;
      ladder.positions     = [];
      ladder.totalShares   = 0;
      ladder.totalCost     = 0;
      return { actions, capital };
    }
  }

  // BUY
  const reEntry = ladderReEntry(ladder);
  if (ladder.positions.length === 0) {
    const threshold = ladder.lastSoldPrice !== null ? reEntry : ENTRY_MAX;
    if (price >= ENTRY_MIN && price <= threshold && capital >= price * SHARES_PER_BUY) {
      const cost = price * SHARES_PER_BUY;
      capital -= cost;
      ladder.positions.push({ price, shares: SHARES_PER_BUY });
      ladder.totalShares += SHARES_PER_BUY;
      ladder.totalCost   += cost;
      actions.push({
        type: 'BUY', side: ladder.side.toUpperCase(), price,
        shares: SHARES_PER_BUY, avg: parseFloat(ladderAvg(ladder).toFixed(4)),
        msg: `🟢 BUY ${ladder.side.toUpperCase()}: ${SHARES_PER_BUY} sh @ $${price} | avg $${ladderAvg(ladder).toFixed(4)}`,
      });
    }
  } else {
    const nextBuy = ladderNextBuy(ladder);
    if (nextBuy !== null && price <= nextBuy && price >= ENTRY_MIN && capital >= price * SHARES_PER_BUY) {
      const cost = price * SHARES_PER_BUY;
      capital -= cost;
      ladder.positions.push({ price, shares: SHARES_PER_BUY });
      ladder.totalShares += SHARES_PER_BUY;
      ladder.totalCost   += cost;
      actions.push({
        type: 'BUY', side: ladder.side.toUpperCase(), price,
        shares: SHARES_PER_BUY, avg: parseFloat(ladderAvg(ladder).toFixed(4)),
        msg: `🟢 ADD ${ladder.side.toUpperCase()}: ${SHARES_PER_BUY} sh @ $${price} | avg $${ladderAvg(ladder).toFixed(4)}`,
      });
    }
  }
  return { actions, capital };
}

// ============================================================
//  SETTLE WINDOW
// ============================================================
function settleWindow(resolution) {
  const results = [];
  for (const [ladder, side] of [[S.upLadder, 'up'], [S.downLadder, 'down']]) {
    if (!ladder || ladder.totalShares === 0) continue;
    const won         = resolution === side;
    const settlePrice = won ? 1.0 : 0.0;
    const proceeds    = ladder.totalShares * settlePrice;
    const pnl         = proceeds - ladder.totalCost;
    S.capital += proceeds;
    results.push({
      side: side.toUpperCase(), shares: ladder.totalShares, settlePrice,
      pnl: parseFloat(pnl.toFixed(2)), won,
      msg: `${won ? '🏆 WIN' : '💀 LOSS'} (${side.toUpperCase()}): ${ladder.totalShares} sh @ $${settlePrice} | PnL $${pnl.toFixed(2)}`,
    });
    ladder.positions     = [];
    ladder.totalShares   = 0;
    ladder.totalCost     = 0;
    ladder.lastSoldPrice = null;
  }
  return results;
}

// ============================================================
//  ACTIVE LADDERS
// ============================================================
function setActiveLadders() {
  if (!S.lastResolution) {
    S.activeLadders = 'both';
    log('📊 No prior window — both ladders active', 'info');
  } else if (S.lastResolution === 'up') {
    S.activeLadders = 'up';
    log('📈 Prev window UP — only UP ladder active', 'info');
  } else {
    S.activeLadders = 'down';
    log('📉 Prev window DOWN — only DOWN ladder active', 'info');
  }
}

// ============================================================
//  PROCESS PRICE TICK
// ============================================================
function processTick() {
  if (S.windowStatus !== 'active') return;
  if ((S.activeLadders === 'both' || S.activeLadders === 'up') && S.upLadder && S.upPrice !== null) {
    const { actions, capital } = tickLadder(S.upLadder, S.upPrice, S.capital);
    S.capital = capital;
    actions.forEach(a => log(a.msg, a.type === 'BUY' ? 'buy' : 'sell'));
  }
  if ((S.activeLadders === 'both' || S.activeLadders === 'down') && S.downLadder && S.downPrice !== null) {
    const { actions, capital } = tickLadder(S.downLadder, S.downPrice, S.capital);
    S.capital = capital;
    actions.forEach(a => log(a.msg, a.type === 'BUY' ? 'buy' : 'sell'));
  }
}

// ============================================================
//  TOKEN ID EXTRACTION
// ============================================================
function extractTokenIds(m) {
  let upId = null, downId = null;

  // Shape 1: clobTokenIds JSON string
  if (m.clobTokenIds) {
    let ids = [];
    if (typeof m.clobTokenIds === 'string') {
      try { ids = JSON.parse(m.clobTokenIds); } catch {}
    } else if (Array.isArray(m.clobTokenIds)) {
      ids = m.clobTokenIds;
    }

    if (ids.length >= 2) {
      let outcomes = [];
      if (typeof m.outcomes === 'string') {
        try { outcomes = JSON.parse(m.outcomes); } catch {}
      } else if (Array.isArray(m.outcomes)) {
        outcomes = m.outcomes;
      }

      for (let i = 0; i < ids.length; i++) {
        const o = (outcomes[i] || '').toLowerCase();
        if (o.includes('up')   && !upId)   upId   = ids[i];
        if (o.includes('down') && !downId) downId = ids[i];
      }
      if (!upId)   upId   = ids[0];
      if (!downId) downId = ids[1];
    }
  }

  // Shape 2: tokens[] array
  if ((!upId || !downId) && Array.isArray(m.tokens) && m.tokens.length >= 2) {
    for (const t of m.tokens) {
      const o  = (t.outcome || '').toLowerCase();
      const id = t.token_id || t.tokenId || t.id || null;
      if (o.includes('up')   && !upId)   upId   = id;
      if (o.includes('down') && !downId) downId = id;
    }
    if (!upId)   upId   = m.tokens[0]?.token_id || m.tokens[0]?.id;
    if (!downId) downId = m.tokens[1]?.token_id || m.tokens[1]?.id;
  }

  return { upId: upId || null, downId: downId || null };
}

// ============================================================
//  MARKET DISCOVERY
// ============================================================
async function fetchMarketBySlug(slug) {
  // Strategy 1: events endpoint
  try {
    const r = await fetch(`${POLY_API}/events?slug=${slug}`, { timeout: 7000 });
    if (r.ok) {
      const data  = await r.json();
      const event = Array.isArray(data) ? data[0] : data;
      if (event && Array.isArray(event.markets) && event.markets.length > 0) {
        const m = event.markets.find(mk =>
          mk.question &&
          (mk.question.toLowerCase().includes('up') ||
           mk.question.toLowerCase().includes('higher'))
        ) || event.markets[0];
        const merged = {
          ...m,
          clobTokenIds: m.clobTokenIds || event.clobTokenIds,
          outcomes:     m.outcomes     || event.outcomes,
        };
        return { found: true, market: merged };
      }
    }
  } catch {}

  // Strategy 2: markets endpoint
  try {
    const r = await fetch(`${POLY_API}/markets?slug=${slug}`, { timeout: 7000 });
    if (r.ok) {
      const data = await r.json();
      if (Array.isArray(data) && data.length > 0) {
        return { found: true, market: data[0] };
      }
    }
  } catch {}

  return { found: false, market: null };
}

async function findWindows() {
  const now     = Math.floor(Date.now() / 1000);
  const base    = Math.floor(now / 900) * 900;
  const results = [];

  // Check next 4 possible 15-min slots by slug
  for (let i = 0; i <= 3; i++) {
    const endsAt = base + i * 900 + 900;
    const slug   = `btc-updown-15m-${endsAt}`;
    try {
      const { found, market } = await fetchMarketBySlug(slug);
      if (found && market) {
        const { upId, downId } = extractTokenIds(market);
        results.push({
          slug,
          marketId:    market.id   || null,
          conditionId: market.conditionId || null,
          endsAt,
          ts:          endsAt - 900,
          upTokenId:   upId,
          downTokenId: downId,
          active:      market.active,
          closed:      market.closed,
          bestBid:     market.bestBid || null,
          bestAsk:     market.bestAsk || null,
        });
      }
    } catch {}
  }

  // Broad fallback search — FIX: use more permissive search terms
  if (results.length === 0) {
    const searchUrls = [
      `${POLY_API}/markets?tag=crypto&limit=50&active=true&closed=false`,
      `${POLY_API}/markets?tag=Bitcoin&limit=50&active=true&closed=false`,
      `${POLY_API}/markets?limit=100&active=true&closed=false&_q=btc-updown-15m`,
    ];
    for (const url of searchUrls) {
      try {
        const r = await fetch(url, { timeout: 10000 });
        if (r.ok) {
          const data = await r.json();
          const list = Array.isArray(data) ? data : (data.results || data.markets || []);
          list
            .filter(m => m.slug && m.slug.includes('btc-updown-15m'))
            .forEach(m => {
              const match = m.slug.match(/btc-updown-15m-(\d+)/);
              if (!match) return;
              const endsAt = parseInt(match[1]);
              // Only add if not already found
              if (results.some(r => r.slug === m.slug)) return;
              const { upId, downId } = extractTokenIds(m);
              results.push({
                slug: m.slug, marketId: m.id, conditionId: m.conditionId || null,
                endsAt, ts: endsAt - 900,
                upTokenId: upId, downTokenId: downId,
                active: m.active, closed: m.closed,
                bestBid: m.bestBid || null, bestAsk: m.bestAsk || null,
              });
            });
          if (results.length > 0) break;
        }
      } catch {}
    }
  }

  return results;
}

// ============================================================
//  PRICES via CLOB REST order book
//  FIX: CLOB /book returns bids in ASCENDING order — bids[last] is best
//       OR use /midpoint endpoint for a single clean price
// ============================================================
async function getClobMidpoint(tokenId) {
  if (!tokenId) return null;
  // Try the dedicated midpoint endpoint first (cleaner)
  try {
    const r = await fetch(`${CLOB_API}/midpoint?token_id=${tokenId}`, { timeout: 4000 });
    if (r.ok) {
      const d = await r.json();
      const mid = parseFloat(d.mid || d.midpoint || 0);
      if (mid > 0 && mid < 1) return mid;
    }
  } catch {}

  // Fall back to order book
  try {
    const r = await fetch(`${CLOB_API}/book?token_id=${tokenId}`, { timeout: 5000 });
    if (r.ok) {
      const d    = await r.json();
      const bids = d.bids || [];
      const asks = d.asks || [];

      // FIX: bids come in ASCENDING order from CLOB — best bid is LAST
      // asks come in ASCENDING order — best ask is FIRST
      let bid = 0, ask = 0;
      if (bids.length) bid = parseFloat(bids[bids.length - 1].price || bids[0].price || 0);
      if (asks.length) ask = parseFloat(asks[0].price || 0);

      // Also check top-level convenience fields
      if (!bid && d.best_bid) bid = parseFloat(d.best_bid);
      if (!ask && d.best_ask) ask = parseFloat(d.best_ask);

      if (bid > 0 && ask > 0)   return parseFloat(((bid + ask) / 2).toFixed(4));
      if (bid > 0)               return bid;
      if (ask > 0)               return ask;
    }
  } catch {}

  return null;
}

async function getPrices(upId, downId) {
  const out = { up: null, down: null };
  const [upMid, downMid] = await Promise.all([
    getClobMidpoint(upId),
    getClobMidpoint(downId),
  ]);
  if (upMid   !== null) out.up   = upMid;
  if (downMid !== null) out.down = downMid;

  // If we got one side but not the other, infer complement
  if (out.up !== null && out.down === null) {
    out.down = parseFloat((1.0 - out.up).toFixed(4));
  } else if (out.down !== null && out.up === null) {
    out.up = parseFloat((1.0 - out.down).toFixed(4));
  }

  return out;
}

// ============================================================
//  PRICES via GAMMA API (secondary source)
//  FIX: Gamma always exposes bestBid/bestAsk on market object
// ============================================================
async function getGammaPrices(marketId) {
  if (!marketId) return { up: null, down: null };
  try {
    const r = await fetch(`${POLY_API}/markets/${marketId}`, { timeout: 5000 });
    if (r.ok) {
      const d = await r.json();
      // outcomePrices is the most reliable field
      if (d.outcomePrices) {
        try {
          const prices   = JSON.parse(d.outcomePrices).map(Number);
          let outcomes   = [];
          try { outcomes = JSON.parse(d.outcomes || '[]'); } catch {}

          let up = null, down = null;
          for (let i = 0; i < prices.length; i++) {
            const o = (outcomes[i] || '').toLowerCase();
            if (o.includes('up'))   up   = prices[i];
            if (o.includes('down')) down = prices[i];
          }
          // Fallback: index 0=up, 1=down
          if (up   === null && prices[0] !== undefined) up   = prices[0];
          if (down === null && prices[1] !== undefined) down = prices[1];

          if (up !== null || down !== null) {
            // Infer complement if missing
            if (up !== null && down === null) down = parseFloat((1 - up).toFixed(4));
            if (down !== null && up === null) up   = parseFloat((1 - down).toFixed(4));
            return { up: parseFloat(up.toFixed(4)), down: parseFloat(down.toFixed(4)) };
          }
        } catch {}
      }

      // Fallback: bestBid/bestAsk for the whole market (usually the UP token)
      if (d.bestBid || d.bestAsk) {
        const bid = parseFloat(d.bestBid || 0);
        const ask = parseFloat(d.bestAsk || 0);
        const mid = bid > 0 && ask > 0 ? (bid + ask) / 2 : bid || ask;
        if (mid > 0) {
          return { up: parseFloat(mid.toFixed(4)), down: parseFloat((1 - mid).toFixed(4)) };
        }
      }
    }
  } catch {}
  return { up: null, down: null };
}

// ============================================================
//  WEBSOCKET PRICE FEED
//  FIX: Handle both price-tick events AND book-snapshot events
// ============================================================
function stopWS() {
  wsAlive = false;
  if (wsPingTimer) { clearInterval(wsPingTimer); wsPingTimer = null; }
  if (polyWs)      { try { polyWs.terminate(); } catch {} polyWs = null; }
  S.wsConnected = false;
}

function stopRESTPoll() {
  if (priceTimer)     { clearInterval(priceTimer);      priceTimer      = null; }
  if (gammaPriceTimer){ clearInterval(gammaPriceTimer); gammaPriceTimer = null; }
}

function startRESTPoll() {
  stopRESTPoll();

  // Primary: CLOB midpoint/book
  priceTimer = setInterval(async () => {
    if (S.windowStatus !== 'active') return;
    try {
      const p = await getPrices(S.upTokenId, S.downTokenId);
      let updated = false;
      if (p.up   !== null && isValidPrice(p.up))   { S.upPrice   = p.up;   pushHistory('up',   p.up);   updated = true; }
      if (p.down !== null && isValidPrice(p.down)) { S.downPrice = p.down; pushHistory('down', p.down); updated = true; }
      if (updated) {
        S.lastPriceUpdate = Date.now();
        S.priceSource     = 'clob';
        processTick();
        broadcastState();
        cancelSimFallback(); // real prices coming, no need for sim
      }
    } catch {}
  }, REST_POLL_MS);

  // Secondary: Gamma API (every 6s as backup)
  if (S.currentWindow?.marketId) {
    const marketId = S.currentWindow.marketId;
    gammaPriceTimer = setInterval(async () => {
      if (S.windowStatus !== 'active') return;
      // Only use gamma if CLOB hasn't updated recently (>10s stale)
      const stale = !S.lastPriceUpdate || (Date.now() - S.lastPriceUpdate) > 10000;
      if (!stale) return;
      try {
        const p = await getGammaPrices(marketId);
        let updated = false;
        if (p.up   !== null && isValidPrice(p.up))   { S.upPrice   = p.up;   pushHistory('up',   p.up);   updated = true; }
        if (p.down !== null && isValidPrice(p.down)) { S.downPrice = p.down; pushHistory('down', p.down); updated = true; }
        if (updated) {
          S.lastPriceUpdate = Date.now();
          S.priceSource     = 'gamma';
          log(`💱 Gamma price: UP=$${S.upPrice} DOWN=$${S.downPrice}`, 'info');
          processTick();
          broadcastState();
          cancelSimFallback();
        }
      } catch {}
    }, GAMMA_POLL_MS);
  }
}

function isValidPrice(p) {
  return typeof p === 'number' && !isNaN(p) && p > 0 && p < 1;
}

function cancelSimFallback() {
  if (simFallbackTimer) { clearTimeout(simFallbackTimer); simFallbackTimer = null; }
}

function subscribeWS(assetIds) {
  if (!assetIds || assetIds.length === 0) {
    log('⚠️  No token IDs — REST poll only (no WS)', 'warn');
    return;
  }

  wsAlive = true;
  let attempt = 0;

  function connect() {
    if (!wsAlive) return;

    const backoff = Math.min(WS_RECONNECT_BASE * Math.pow(1.5, attempt), WS_RECONNECT_MAX);
    attempt++;

    try {
      const ws = new WebSocket(POLY_WS);
      let pongOk = true;

      if (wsPingTimer) clearInterval(wsPingTimer);
      wsPingTimer = setInterval(() => {
        if (!pongOk) {
          log('💔 WS heartbeat timeout — reconnecting', 'warn');
          S.wsConnected = false;
          clearInterval(wsPingTimer);
          wsPingTimer = null;
          try { ws.terminate(); } catch {}
          if (wsAlive) setTimeout(connect, backoff);
          return;
        }
        pongOk = false;
        try { ws.ping(); } catch {}
      }, WS_PING_MS);

      ws.on('pong', () => { pongOk = true; });

      ws.on('open', () => {
        if (attempt > 1) S.wsReconnects++;
        attempt = 0;
        S.wsConnected = true;

        ws.send(JSON.stringify({
          auth:       {},
          type:       'Market',
          assets_ids: assetIds,
        }));

        log(`🔌 WS connected (${assetIds.length} token(s))`, 'success');
        broadcastState();
      });

      ws.on('message', raw => {
        try {
          const msgs = JSON.parse(raw.toString());
          const arr  = Array.isArray(msgs) ? msgs : [msgs];
          let updated = false;

          for (const m of arr) {
            // FIX: Polymarket WS sends multiple event types:
            // price_change: { asset_id, price, side }
            // book:         { asset_id, bids:[{price,size}], asks:[{price,size}] }
            // last_trade_price: { asset_id, last_trade_price }
            let price = 0;

            if (m.event_type === 'book' || (m.bids && m.asks)) {
              // Book snapshot — compute midpoint
              const bids = m.bids || [];
              const asks = m.asks || [];
              // bids in ascending order, best is last
              const bestBid = bids.length ? parseFloat(bids[bids.length - 1].price || 0) : 0;
              const bestAsk = asks.length ? parseFloat(asks[0].price || 0) : 0;
              if (bestBid > 0 && bestAsk > 0) price = (bestBid + bestAsk) / 2;
              else if (bestBid > 0)           price = bestBid;
              else if (bestAsk > 0)           price = bestAsk;
            } else {
              // Price tick
              price = parseFloat(
                m.price || m.last_trade_price || m.mid || m.midpoint || 0
              );
            }

            if (!price || isNaN(price) || price <= 0 || price >= 1) continue;

            if (m.asset_id === S.upTokenId) {
              S.upPrice = parseFloat(price.toFixed(4));
              pushHistory('up', S.upPrice);
              updated = true;
            }
            if (m.asset_id === S.downTokenId) {
              S.downPrice = parseFloat(price.toFixed(4));
              pushHistory('down', S.downPrice);
              updated = true;
            }
          }

          if (updated) {
            S.lastPriceUpdate = Date.now();
            S.priceSource     = 'ws';
            cancelSimFallback();
            processTick();
            broadcastState();
          }
        } catch {}
      });

      ws.on('close', code => {
        S.wsConnected = false;
        if (wsPingTimer) { clearInterval(wsPingTimer); wsPingTimer = null; }
        if (wsAlive) {
          log(`🔄 WS closed (${code}) — reconnect in ${Math.round(backoff / 1000)}s`, 'warn');
          broadcastState();
          setTimeout(connect, backoff);
        }
      });

      ws.on('error', () => { S.wsConnected = false; });

      polyWs = ws;
    } catch {
      if (wsAlive) setTimeout(connect, backoff);
    }
  }

  connect();
}

// ============================================================
//  RESOLUTION CHECK
// ============================================================
async function checkResolution(marketId) {
  try {
    const r = await fetch(`${POLY_API}/markets/${marketId}`, { timeout: 6000 });
    if (r.ok) {
      const d = await r.json();
      if (d.closed || d.resolved) {
        const tokens = Array.isArray(d.tokens) ? d.tokens : [];
        const winner = tokens.find(t => t.winner);
        if (winner) {
          return {
            resolved: true,
            winner: (winner.outcome || '').toLowerCase().includes('up') ? 'up' : 'down',
          };
        }
        if (d.outcomePrices) {
          try {
            const prices   = JSON.parse(d.outcomePrices).map(Number);
            const outcomes = JSON.parse(d.outcomes || '[]');
            const idx      = prices.indexOf(Math.max(...prices));
            return {
              resolved: true,
              winner: (outcomes[idx] || '').toLowerCase().includes('up') ? 'up' : 'down',
            };
          } catch {}
        }
      }
    }
  } catch {}
  return { resolved: false };
}

// ============================================================
//  SIMULATION PRICES
//  FIX: More realistic binary market behavior
//  - Prices drift with momentum (not pure random walk)
//  - Range 0.30–0.70 (realistic binary markets)
//  - UP + DOWN always sum to 1.00
// ============================================================
function startSimPrices() {
  // Start from a realistic midpoint if we have no real price
  const startUp = S.upPrice || parseFloat((0.44 + Math.random() * 0.12).toFixed(4));
  simPrice.up   = Math.max(0.30, Math.min(0.70, startUp));
  simPrice.down = parseFloat((1.0 - simPrice.up).toFixed(4));

  // Initialize state prices if not set
  if (!S.upPrice)   S.upPrice   = parseFloat(simPrice.up.toFixed(4));
  if (!S.downPrice) S.downPrice = parseFloat(simPrice.down.toFixed(4));

  let momentum = 0;

  if (simTimer) clearInterval(simTimer);
  simTimer = setInterval(() => {
    if (S.windowStatus !== 'active') { clearInterval(simTimer); simTimer = null; return; }

    // Momentum-based random walk with mean reversion
    momentum = momentum * 0.85 + (Math.random() - 0.50) * 0.008;
    const delta = momentum;

    simPrice.up = Math.max(0.05, Math.min(0.95, simPrice.up + delta));
    simPrice.down = parseFloat((1.0 - simPrice.up).toFixed(4));
    S.upPrice     = parseFloat(simPrice.up.toFixed(4));
    S.downPrice   = parseFloat(simPrice.down.toFixed(4));

    pushHistory('up',   S.upPrice);
    pushHistory('down', S.downPrice);
    S.lastPriceUpdate = Date.now();
    S.priceSource     = 'sim';
    processTick();
    broadcastState();
  }, 600);
}

function stopSimPrices() {
  if (simTimer) { clearInterval(simTimer); simTimer = null; }
}

function pushHistory(side, price) {
  S.priceHistory[side].push({ price, ts: Date.now() });
  if (S.priceHistory[side].length > 300) S.priceHistory[side].shift();
}

// ============================================================
//  WINDOW LIFECYCLE
// ============================================================
async function findAndStart() {
  S.windowStatus = 'searching';
  S.botStatus    = 'searching for window...';
  broadcastState();
  log('🔍 Searching Polymarket for BTC 15m windows...', 'info');

  const windows = await findWindows();
  const now     = Math.floor(Date.now() / 1000);

  if (windows.length === 0) {
    log('⚠️  No windows found — starting simulation', 'warn');
    startSim();
    return;
  }

  const active = windows
    .filter(w => w.ts <= now && w.endsAt > now && !w.closed)
    .sort((a, b) => {
      const aScore = (a.upTokenId ? 1 : 0) + (a.downTokenId ? 1 : 0);
      const bScore = (b.upTokenId ? 1 : 0) + (b.downTokenId ? 1 : 0);
      return bScore - aScore;
    })[0];

  const upcoming = windows.find(w => w.ts > now);

  if (active) {
    log(`✅ Found active window: ${active.slug}`, 'success');
    await startWindow(active, upcoming || null);
  } else if (upcoming) {
    const wait = upcoming.ts - now;
    log(`⏰ Next window in ${wait}s — ${upcoming.slug}`, 'info');
    S.windowStatus = 'waiting';
    S.nextWindow   = upcoming;
    broadcastState();
    setTimeout(() => startWindow(upcoming, null), wait * 1000);
  } else {
    log('⚠️  No suitable windows — retrying in 30s', 'warn');
    setTimeout(findAndStart, 30_000);
  }
}

async function startWindow(win, next) {
  log(`🚀 Window started: ${win.slug}`, 'success');

  let upId   = win.upTokenId   || null;
  let downId = win.downTokenId || null;

  // Re-fetch if token IDs are missing
  if (!upId || !downId) {
    log('🔄 Token IDs missing — re-fetching market...', 'warn');
    try {
      const { found, market } = await fetchMarketBySlug(win.slug);
      if (found && market) {
        const ids = extractTokenIds(market);
        upId   = ids.upId   || upId;
        downId = ids.downId || downId;
      }
    } catch {}
  }

  S.currentWindow  = win;
  S.nextWindow     = next || null;
  S.upTokenId      = upId;
  S.downTokenId    = downId;
  S.upLadder       = makeLadder('up');
  S.downLadder     = makeLadder('down');
  S.priceHistory   = { up: [], down: [] };
  S.windowStatus   = 'active';
  S.botStatus      = 'trading';
  S.upPrice        = null;
  S.downPrice      = null;
  S.priceSource    = 'none';

  setActiveLadders();

  if (upId || downId) {
    log(`🪙 UP token:   ${upId   || 'n/a'}`, 'info');
    log(`🪙 DOWN token: ${downId || 'n/a'}`, 'info');

    // Seed initial price: try CLOB first, then Gamma
    let seeded = false;
    const p = await getPrices(upId, downId);
    if (p.up !== null || p.down !== null) {
      if (p.up   !== null) { S.upPrice   = p.up;   pushHistory('up',   p.up);   }
      if (p.down !== null) { S.downPrice = p.down; pushHistory('down', p.down); }
      S.lastPriceUpdate = Date.now();
      S.priceSource     = 'clob';
      seeded = true;
      log(`📈 CLOB seed — UP: $${S.upPrice} | DOWN: $${S.downPrice}`, 'info');
    }

    if (!seeded && win.marketId) {
      const gp = await getGammaPrices(win.marketId);
      if (gp.up !== null || gp.down !== null) {
        if (gp.up   !== null) { S.upPrice   = gp.up;   pushHistory('up',   gp.up);   }
        if (gp.down !== null) { S.downPrice = gp.down; pushHistory('down', gp.down); }
        S.lastPriceUpdate = Date.now();
        S.priceSource     = 'gamma';
        seeded = true;
        log(`📈 Gamma seed — UP: $${S.upPrice} | DOWN: $${S.downPrice}`, 'info');
      }
    }

    // Start REST polls (CLOB + Gamma)
    startRESTPoll();
    subscribeWS([upId, downId].filter(Boolean));

    // FIX: Schedule sim fallback — if no real price within SIM_FALLBACK_MS, use sim prices
    if (!seeded) {
      log('⏳ No seed price yet — sim fallback in 8s if needed', 'warn');
      simFallbackTimer = setTimeout(() => {
        if (S.windowStatus === 'active' && (!S.lastPriceUpdate || (Date.now() - S.lastPriceUpdate) > SIM_FALLBACK_MS)) {
          log('🎮 No real prices — enabling sim price overlay', 'warn');
          S.priceSource = 'sim';
          startSimPrices();
        }
      }, SIM_FALLBACK_MS);
    }

  } else {
    log('⚠️  No token IDs resolved — simulation prices', 'warn');
    startRESTPoll();
    startSimPrices();
  }

  const timeLeft = Math.max(0, win.endsAt - Math.floor(Date.now() / 1000));
  log(`⏱  Window ends in ${timeLeft}s`, 'info');
  setTimeout(() => endWindow(win), timeLeft * 1000);
  broadcastState();
}

async function endWindow(win) {
  log(`🏁 Window closing: ${win.slug}`, 'info');
  S.windowStatus = 'settling';
  broadcastState();

  cancelSimFallback();
  stopWS();
  stopRESTPoll();
  stopSimPrices();

  let resolution = null;
  if (win.marketId) {
    for (let i = 0; i < 10 && !resolution; i++) {
      const r = await checkResolution(win.marketId);
      if (r.resolved) {
        resolution = r.winner;
        log(`🎯 Resolution: ${resolution.toUpperCase()}`, 'success');
      } else {
        await new Promise(res => setTimeout(res, 3000));
      }
    }
  }
  if (!resolution) {
    resolution = (S.upPrice || 0.5) >= 0.5 ? 'up' : 'down';
    log(`⚡ Price-based resolution: ${resolution.toUpperCase()}`, 'warn');
  }

  const capBefore = S.capital;
  const results   = settleWindow(resolution);
  const windowPnl = S.capital - capBefore;

  S.totalPnl      += windowPnl;
  S.windowsTraded++;
  S.lastResolution = resolution;
  if (windowPnl >= 0) S.wins++; else S.losses++;

  results.forEach(r => log(r.msg, r.won ? 'success' : 'error'));
  log(`💰 Window PnL: $${windowPnl.toFixed(2)} | Capital: $${S.capital.toFixed(2)}`, 'info');

  S.windowHistory.unshift({
    slug:          win.slug,
    resolution,
    pnl:           parseFloat(windowPnl.toFixed(2)),
    capital:       parseFloat(S.capital.toFixed(2)),
    activeLadders: S.activeLadders,
    ts:            Date.now(),
    simulated:     !!win.simulated,
    priceSource:   S.priceSource,
  });

  S.windowStatus = 'waiting';
  broadcastState();
  setTimeout(findAndStart, 2000);
}

function startSim() {
  const now = Math.floor(Date.now() / 1000);
  const win = {
    slug:        `btc-updown-15m-${now + 900}`,
    marketId:    null,
    endsAt:      now + 900,
    ts:          now,
    simulated:   true,
    upTokenId:   null,
    downTokenId: null,
  };
  S.currentWindow = win;
  S.upLadder      = makeLadder('up');
  S.downLadder    = makeLadder('down');
  S.priceHistory  = { up: [], down: [] };
  S.windowStatus  = 'active';
  S.botStatus     = 'trading (simulated)';
  S.priceSource   = 'sim';
  setActiveLadders();
  S.upPrice   = parseFloat((0.44 + Math.random() * 0.12).toFixed(4));
  S.downPrice = parseFloat((1.0 - S.upPrice).toFixed(4));
  pushHistory('up',   S.upPrice);
  pushHistory('down', S.downPrice);
  log('🎮 SIMULATION MODE active', 'warn');
  startSimPrices();
  setTimeout(() => endWindow(win), 900_000);
  broadcastState();
}

// ============================================================
//  LOGGING + BROADCAST
// ============================================================
function log(msg, type = 'info') {
  const entry = { msg, type, ts: Date.now() };
  S.tradeLog.unshift(entry);
  if (S.tradeLog.length > 200) S.tradeLog.pop();
  broadcast('log', entry);
  console.log(`[${type.toUpperCase()}] ${msg}`);
}

function broadcast(type, payload) {
  const msg = JSON.stringify({ type, payload, ts: Date.now() });
  for (const c of dashClients) {
    if (c.readyState === WebSocket.OPEN) {
      try { c.send(msg); } catch {}
    }
  }
}

function broadcastState() { broadcast('state', publicState()); }

function publicState() {
  const floating = S.capital
    + (S.upLadder?.totalShares   || 0) * (S.upPrice   || 0)
    + (S.downLadder?.totalShares || 0) * (S.downPrice || 0);
  const now = Math.floor(Date.now() / 1000);
  return {
    capital:         S.capital,
    startCapital:    S.startCapital,
    floatingCapital: parseFloat(floating.toFixed(2)),
    totalPnl:        S.totalPnl,
    windowsTraded:   S.windowsTraded,
    wins:            S.wins,
    losses:          S.losses,
    winRate:         S.windowsTraded > 0
      ? ((S.wins / S.windowsTraded) * 100).toFixed(1)
      : '0.0',
    lastResolution:  S.lastResolution,
    activeLadders:   S.activeLadders,
    entryMin:        ENTRY_MIN,
    entryMax:        ENTRY_MAX,
    currentWindow:   S.currentWindow ? {
      slug:      S.currentWindow.slug,
      marketId:  S.currentWindow.marketId,
      endsAt:    S.currentWindow.endsAt,
      simulated: S.currentWindow.simulated,
      timeLeft:  Math.max(0, S.currentWindow.endsAt - now),
    } : null,
    nextWindow:      S.nextWindow ? { slug: S.nextWindow.slug } : null,
    upLadder:        fmtLadder(S.upLadder,   S.upPrice),
    downLadder:      fmtLadder(S.downLadder, S.downPrice),
    upPrice:         S.upPrice,
    downPrice:       S.downPrice,
    upTokenId:       S.upTokenId,
    downTokenId:     S.downTokenId,
    windowStatus:    S.windowStatus,
    windowHistory:   S.windowHistory.slice(0, 20),
    tradeLog:        S.tradeLog.slice(0, 50),
    priceHistory:    {
      up:   S.priceHistory.up.slice(-150),
      down: S.priceHistory.down.slice(-150),
    },
    lastPriceUpdate: S.lastPriceUpdate,
    botStatus:       S.botStatus,
    wsConnected:     S.wsConnected,
    wsReconnects:    S.wsReconnects,
    errors:          S.errors.slice(-5),
    priceSource:     S.priceSource,
  };
}

function fmtLadder(l, price) {
  if (!l) return null;
  const avg    = ladderAvg(l);
  const unreal = l.totalShares > 0 && price
    ? (l.totalShares * price - l.totalCost)
    : 0;
  return {
    side:          l.side,
    positions:     l.positions,
    totalShares:   l.totalShares,
    totalCost:     parseFloat(l.totalCost.toFixed(2)),
    avgEntry:      parseFloat(avg.toFixed(4)),
    lastSoldPrice: l.lastSoldPrice,
    nextBuy:       ladderNextBuy(l),
    reEntry:       ladderReEntry(l),
    unrealizedPnl: parseFloat(unreal.toFixed(2)),
  };
}

// ============================================================
//  EXPRESS + DASHBOARD WEBSOCKET SERVER
// ============================================================
const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/api/state',  (_, res) => res.json(publicState()));
app.get('/api/health', (_, res) => res.json({ ok: true, uptime: process.uptime() }));

const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  dashClients.add(ws);
  try {
    ws.send(JSON.stringify({ type: 'state', payload: publicState(), ts: Date.now() }));
  } catch {}
  ws.on('close', () => dashClients.delete(ws));
  ws.on('error', () => {
    dashClients.delete(ws);
    try { ws.terminate(); } catch {}
  });
});

setInterval(() => {
  for (const ws of dashClients) {
    if (!ws.isAlive) {
      dashClients.delete(ws);
      try { ws.terminate(); } catch {}
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30_000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🚀 BTC Ladder Bot → http://localhost:${PORT}\n`);
  log(`Bot started. Capital: $${S.capital} | Entry: $${ENTRY_MIN}–$${ENTRY_MAX}`, 'success');
  setTimeout(findAndStart, 1000);
});
