// ============================================================
//  BTC LADDER BOT — All-in-one server
//  Polymarket 15-min BTC Up/Down Windows
//  Logic: Previous window winner determines active ladder
//  Entry 0.05-0.90, buy 100 every -0.05,
//  sell all at avg+0.10, re-entry at lastSell-0.05
//
//  FIXES v3:
//  1. clobTokenIds is a JSON STRING — must JSON.parse() it
//  2. Market discovery uses EVENTS endpoint (/events?slug=) as primary,
//     falls back to /markets?slug= and broad tag search
//  3. WS code 1006 loop fixed: empty assetIds caused immediate rejection
//     by Polymarket → WS now skips subscribe if no IDs, falls to REST poll
//  4. WS subscription payload fixed: { type:'Market', assets_ids:[...] }
//     (Polymarket CLOB WS expects assets_ids not markets)
//  5. REST poll runs independently as a safety net at all times
//  6. Entry range: ENTRY_MIN=0.05, ENTRY_MAX=0.90 (restored per user)
//  7. Sim prices: UP+DOWN always complement to 1.00
//  8. Exponential WS backoff, heartbeat ping/pong, wsAlive cleanup
//  9. Dashboard WS heartbeat to detect dead browser connections
// ============================================================

'use strict';

const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const path       = require('path');
const cors       = require('cors');
const fetch      = require('node-fetch');

// ============================================================
//  CONSTANTS
// ============================================================
const DEMO_CAPITAL   = 2000;
const ENTRY_MIN      = 0.05;
const ENTRY_MAX      = 0.90;
const BUY_STEP       = 0.05;
const SELL_PROFIT    = 0.10;
const REENTRY_STEP   = 0.05;
const SHARES_PER_BUY = 100;

const WS_PING_MS        = 20_000;
const REST_POLL_MS      = 4_000;
const WS_RECONNECT_BASE = 3_000;
const WS_RECONNECT_MAX  = 30_000;

const POLY_API = 'https://gamma-api.polymarket.com';
const CLOB_API = 'https://clob.polymarket.com';
// Polymarket CLOB WebSocket — correct subscription endpoint
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
};

let polyWs       = null;
let wsAlive      = false;
let wsPingTimer  = null;
let priceTimer   = null;
let simTimer     = null;
let dashClients  = new Set();
let simPrice     = { up: 0.55, down: 0.45 };

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
//  PROCESS ONE LADDER TICK
// ============================================================
function tickLadder(ladder, price, capital) {
  const actions = [];
  price = parseFloat(price.toFixed(4));

  // SELL: price rose SELL_PROFIT above average entry
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
//  KEY FIX: clobTokenIds is a JSON *string* — must JSON.parse()
//  e.g.  m.clobTokenIds = '["123456...","789012..."]'
//  Also handles tokens[] array and outcomes[] pairing
// ============================================================
function extractTokenIds(m) {
  let upId = null, downId = null;

  // ── Shape 1: clobTokenIds is a JSON string (most common for 15m markets) ──
  if (m.clobTokenIds) {
    let ids = [];
    if (typeof m.clobTokenIds === 'string') {
      try { ids = JSON.parse(m.clobTokenIds); } catch {}
    } else if (Array.isArray(m.clobTokenIds)) {
      ids = m.clobTokenIds;
    }

    if (ids.length >= 2) {
      // Try to pair with outcomes array
      let outcomes = [];
      if (typeof m.outcomes === 'string') {
        try { outcomes = JSON.parse(m.outcomes); } catch {}
      } else if (Array.isArray(m.outcomes)) {
        outcomes = m.outcomes;
      }

      for (let i = 0; i < ids.length; i++) {
        const o = (outcomes[i] || '').toLowerCase();
        if (o.includes('up'))   { upId   = ids[i]; continue; }
        if (o.includes('down')) { downId = ids[i]; continue; }
      }
      // If outcomes didn't match labels, fall back to index 0=up, 1=down
      if (!upId)   upId   = ids[0];
      if (!downId) downId = ids[1];
    }
  }

  // ── Shape 2: tokens[] array with outcome labels ──
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
//  Three strategies tried in order:
//  1. Gamma /events?slug= (Polymarket event slug — what the URL shows)
//  2. Gamma /markets?slug= (market-level slug, same format)
//  3. Broad tag search fallback
// ============================================================
async function fetchMarketBySlug(slug) {
  // Strategy 1: events endpoint (Polymarket URLs are /event/<slug>)
  try {
    const r = await fetch(`${POLY_API}/events?slug=${slug}`, { timeout: 7000 });
    if (r.ok) {
      const data = await r.json();
      const event = Array.isArray(data) ? data[0] : data;
      if (event && Array.isArray(event.markets) && event.markets.length > 0) {
        // An event contains multiple markets; we want the one with UP/DOWN outcomes
        const m = event.markets.find(mk =>
          mk.question && (mk.question.toLowerCase().includes('up') || mk.question.toLowerCase().includes('higher'))
        ) || event.markets[0];
        // Merge event-level fields so extractTokenIds sees everything
        const merged = { ...m, clobTokenIds: m.clobTokenIds || event.clobTokenIds };
        return { found: true, market: merged, eventId: event.id };
      }
    }
  } catch {}

  // Strategy 2: markets endpoint with slug param
  try {
    const r = await fetch(`${POLY_API}/markets?slug=${slug}`, { timeout: 7000 });
    if (r.ok) {
      const data = await r.json();
      if (Array.isArray(data) && data.length > 0) {
        return { found: true, market: data[0], eventId: null };
      }
    }
  } catch {}

  return { found: false };
}

async function findWindows() {
  const now    = Math.floor(Date.now() / 1000);
  const base   = Math.floor(now / 900) * 900;
  const results = [];

  // Try the next 4 possible 15-min windows
  for (let i = 0; i <= 3; i++) {
    const endsAt = base + i * 900 + 900;
    const slug   = `btc-updown-15m-${endsAt}`;
    try {
      const { found, market } = await fetchMarketBySlug(slug);
      if (found && market) {
        const { upId, downId } = extractTokenIds(market);
        results.push({
          slug,
          marketId:    market.id,
          conditionId: market.conditionId || null,
          endsAt,
          ts:          endsAt - 900,
          upTokenId:   upId,
          downTokenId: downId,
          active:      market.active,
          closed:      market.closed,
          bestBid:     market.bestBid   || null,
          bestAsk:     market.bestAsk   || null,
        });
      }
    } catch {}
  }

  // Broad fallback: search active BTC markets
  if (results.length === 0) {
    try {
      const r = await fetch(`${POLY_API}/markets?tag=BTC&limit=50&active=true&closed=false`, { timeout: 10000 });
      if (r.ok) {
        const data = await r.json();
        (data || [])
          .filter(m => m.slug && m.slug.includes('btc-updown-15m'))
          .forEach(m => {
            const match = m.slug.match(/btc-updown-15m-(\d+)/);
            if (!match) return;
            const endsAt = parseInt(match[1]);
            const { upId, downId } = extractTokenIds(m);
            results.push({
              slug: m.slug, marketId: m.id, conditionId: m.conditionId || null,
              endsAt, ts: endsAt - 900,
              upTokenId: upId, downTokenId: downId,
              active: m.active, closed: m.closed,
              bestBid: m.bestBid || null, bestAsk: m.bestAsk || null,
            });
          });
      }
    } catch {}
  }

  return results;
}

// ============================================================
//  PRICES — REST
//  Uses CLOB /book endpoint; also seeds from bestBid/bestAsk
//  on the market object as an instant fallback
// ============================================================
async function getPrices(upId, downId) {
  const out = { up: null, down: null };
  for (const [id, key] of [[upId, 'up'], [downId, 'down']]) {
    if (!id) continue;
    try {
      const r = await fetch(`${CLOB_API}/book?token_id=${id}`, { timeout: 5000 });
      if (r.ok) {
        const d    = await r.json();
        const bids = d.bids || [];
        const asks = d.asks || [];
        let bid = bids.length ? parseFloat(bids[0].price) : 0;
        let ask = asks.length ? parseFloat(asks[0].price) : 0;
        // Flat field fallback
        if (!bid && d.best_bid) bid = parseFloat(d.best_bid);
        if (!ask && d.best_ask) ask = parseFloat(d.best_ask);
        if (bid > 0 && ask > 0) out[key] = parseFloat(((bid + ask) / 2).toFixed(4));
        else if (bid > 0)       out[key] = bid;
        else if (ask > 0)       out[key] = ask;
      }
    } catch {}
  }
  return out;
}

// ============================================================
//  PRICES — WebSocket
//  FIX: subscription payload must use `assets_ids` (not `markets`)
//  FIX: skip WS entirely if no token IDs — avoids code 1006 loop
// ============================================================
function stopWS() {
  wsAlive = false;
  if (wsPingTimer) { clearInterval(wsPingTimer); wsPingTimer = null; }
  if (polyWs) { try { polyWs.terminate(); } catch {} polyWs = null; }
  S.wsConnected = false;
}

function stopRESTPoll() {
  if (priceTimer) { clearInterval(priceTimer); priceTimer = null; }
}

function startRESTPoll() {
  stopRESTPoll();
  priceTimer = setInterval(async () => {
    if (S.windowStatus !== 'active') return;
    try {
      const p = await getPrices(S.upTokenId, S.downTokenId);
      let updated = false;
      if (p.up   !== null) { S.upPrice   = p.up;   pushHistory('up',   p.up);   updated = true; }
      if (p.down !== null) { S.downPrice = p.down; pushHistory('down', p.down); updated = true; }
      if (updated) {
        S.lastPriceUpdate = Date.now();
        processTick();
        broadcastState();
      }
    } catch {}
  }, REST_POLL_MS);
}

function subscribeWS(assetIds) {
  // If no real token IDs, skip WS — use REST poll + simulation prices
  if (!assetIds || assetIds.length === 0) {
    log('⚠️  No token IDs — REST poll + simulation prices active', 'warn');
    startSimPrices();
    return;
  }

  wsAlive = true;
  let attempt = 0;

  // REST poll runs independently as fallback at all times
  startRESTPoll();

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
          log('💔 WS ping timeout — reconnecting', 'warn');
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
        attempt = 0;
        S.wsConnected  = true;
        S.wsReconnects += attempt > 0 ? 1 : 0;

        // ── CRITICAL FIX: Polymarket CLOB WS uses assets_ids not markets ──
        ws.send(JSON.stringify({
          auth:       {},
          type:       'Market',
          assets_ids: assetIds,     // ← correct field name
        }));

        log(`🔌 WS connected | tokens: ${assetIds.join(', ').substring(0, 60)}...`, 'success');
        broadcastState();
      });

      ws.on('message', raw => {
        try {
          const msgs = JSON.parse(raw.toString());
          const arr  = Array.isArray(msgs) ? msgs : [msgs];
          let updated = false;

          for (const m of arr) {
            // Price can be in: price, best_ask, last_trade_price, mid
            let price = parseFloat(
              m.price || m.best_ask || m.last_trade_price || m.mid || 0
            );
            if (!price || isNaN(price) || price <= 0) continue;

            if (m.asset_id === S.upTokenId) {
              S.upPrice = price; pushHistory('up', price); updated = true;
            }
            if (m.asset_id === S.downTokenId) {
              S.downPrice = price; pushHistory('down', price); updated = true;
            }
          }

          if (updated) {
            S.lastPriceUpdate = Date.now();
            processTick();
            broadcastState();
          }
        } catch {}
      });

      ws.on('close', (code) => {
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
//  SIMULATION PRICES — UP+DOWN always sum to 1.00
// ============================================================
function startSimPrices() {
  simPrice.up   = parseFloat((0.44 + Math.random() * 0.12).toFixed(4));
  simPrice.down = parseFloat((1.0 - simPrice.up).toFixed(4));
  if (simTimer) clearInterval(simTimer);
  simTimer = setInterval(() => {
    if (S.windowStatus !== 'active') { clearInterval(simTimer); simTimer = null; return; }
    const delta  = (Math.random() - 0.49) * 0.014;
    simPrice.up  = Math.max(0.05, Math.min(0.95, simPrice.up + delta));
    simPrice.down = parseFloat((1.0 - simPrice.up).toFixed(4));
    S.upPrice    = parseFloat(simPrice.up.toFixed(4));
    S.downPrice  = parseFloat(simPrice.down.toFixed(4));
    pushHistory('up',   S.upPrice);
    pushHistory('down', S.downPrice);
    S.lastPriceUpdate = Date.now();
    processTick();
    broadcastState();
  }, 700);
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

  // Prefer windows that have token IDs
  const active = windows
    .filter(w => w.ts <= now && w.endsAt > now && !w.closed)
    .sort((a, b) => {
      // Prefer ones with both token IDs
      const aHas = (a.upTokenId && a.downTokenId) ? 1 : 0;
      const bHas = (b.upTokenId && b.downTokenId) ? 1 : 0;
      return bHas - aHas;
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

  // If token IDs are missing, attempt a re-fetch of just this market
  if (!upId || !downId) {
    log('⚠️  Token IDs incomplete — re-fetching market...', 'warn');
    try {
      const { found, market } = await fetchMarketBySlug(win.slug);
      if (found && market) {
        const ids = extractTokenIds(market);
        upId   = ids.upId   || upId;
        downId = ids.downId || downId;
        // Also seed price from market object if available
        if (market.bestBid && market.bestAsk) {
          const mid = parseFloat(((parseFloat(market.bestBid) + parseFloat(market.bestAsk)) / 2).toFixed(4));
          if (!isNaN(mid) && mid > 0) {
            S.upPrice   = mid;
            S.downPrice = parseFloat((1.0 - mid).toFixed(4));
            log(`💰 Seeded price from market: UP=$${S.upPrice} DOWN=$${S.downPrice}`, 'info');
          }
        }
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

  setActiveLadders();

  if (upId || downId) {
    log(`🪙 UP token:   ${upId   || 'n/a'}`, 'info');
    log(`🪙 DOWN token: ${downId || 'n/a'}`, 'info');
    // Seed initial price via REST before WS connects
    const p = await getPrices(upId, downId);
    if (p.up   !== null) { S.upPrice   = p.up;   pushHistory('up',   p.up);   }
    if (p.down !== null) { S.downPrice = p.down; pushHistory('down', p.down); }
    if (p.up !== null || p.down !== null) {
      log(`📈 Initial prices — UP: $${S.upPrice} | DOWN: $${S.downPrice}`, 'info');
    }
    subscribeWS([upId, downId].filter(Boolean));
  } else {
    log('⚠️  No token IDs found — simulation mode', 'warn');
    startSimPrices();
    startRESTPoll(); // still try REST in case IDs appear later
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

  stopWS();
  stopRESTPoll();
  if (simTimer) { clearInterval(simTimer); simTimer = null; }

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
    slug: win.slug, resolution,
    pnl: parseFloat(windowPnl.toFixed(2)),
    capital: parseFloat(S.capital.toFixed(2)),
    activeLadders: S.activeLadders,
    ts: Date.now(), simulated: !!win.simulated,
  });

  S.windowStatus = 'waiting';
  broadcastState();
  setTimeout(findAndStart, 2000);
}

function startSim() {
  const now = Math.floor(Date.now() / 1000);
  const win = {
    slug: `btc-updown-15m-${now + 900}`, marketId: null,
    endsAt: now + 900, ts: now, simulated: true,
    upTokenId: null, downTokenId: null,
  };
  S.currentWindow = win;
  S.upLadder      = makeLadder('up');
  S.downLadder    = makeLadder('down');
  S.priceHistory  = { up: [], down: [] };
  S.windowStatus  = 'active';
  S.botStatus     = 'trading (simulated)';
  setActiveLadders();
  S.upPrice   = 0.55;
  S.downPrice = 0.45;
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
    winRate:         S.windowsTraded > 0 ? ((S.wins / S.windowsTraded) * 100).toFixed(1) : '0.0',
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
  };
}

function fmtLadder(l, price) {
  if (!l) return null;
  const avg    = ladderAvg(l);
  const unreal = l.totalShares > 0 && price ? (l.totalShares * price - l.totalCost) : 0;
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
//  EXPRESS + DASHBOARD WS
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
  try { ws.send(JSON.stringify({ type: 'state', payload: publicState(), ts: Date.now() })); } catch {}
  ws.on('close', () => dashClients.delete(ws));
  ws.on('error', () => { dashClients.delete(ws); try { ws.terminate(); } catch {} });
});

// Heartbeat: ping dashboard clients every 30s, drop dead ones
setInterval(() => {
  for (const ws of dashClients) {
    if (!ws.isAlive) { dashClients.delete(ws); try { ws.terminate(); } catch {} continue; }
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
