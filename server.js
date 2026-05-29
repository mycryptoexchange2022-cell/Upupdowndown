// ============================================================
//  BTC LADDER BOT — All-in-one server
//  Polymarket 15-min BTC Up/Down Windows
//  Logic: Previous window winner determines active ladder
//  No stop loss. Entry 0.55-0.90, buy 100 every -0.05,
//  sell all at avg+0.10, re-entry at lastSell-0.05
// ============================================================

const express  = require('express');
const http     = require('http');
const WebSocket = require('ws');
const path     = require('path');
const cors     = require('cors');
const fetch    = require('node-fetch');

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

const POLY_API = 'https://gamma-api.polymarket.com';
const CLOB_API = 'https://clob.polymarket.com';
const POLY_WS  = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';

// ============================================================
//  STATE
// ============================================================
let S = {
  capital:        DEMO_CAPITAL,
  startCapital:   DEMO_CAPITAL,
  totalPnl:       0,
  windowsTraded:  0,
  wins:           0,
  losses:         0,
  lastResolution: null,

  currentWindow:  null,
  nextWindow:     null,
  upLadder:       null,
  downLadder:     null,
  upPrice:        null,
  downPrice:      null,
  upTokenId:      null,
  downTokenId:    null,

  windowStatus:   'searching',
  windowHistory:  [],
  tradeLog:       [],
  priceHistory:   { up: [], down: [] },
  lastPriceUpdate: null,
  botStatus:      'starting',
  wsConnected:    false,
  errors:         [],
  activeLadders:  'both',
};

let polyWs      = null;
let priceTimer  = null;
let simTimer    = null;
let dashClients = new Set();
let simPrice    = { up: 0.62, down: 0.38 };

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
  if (l.lastSoldPrice === null) return ENTRY_MIN;
  return parseFloat((l.lastSoldPrice - REENTRY_STEP).toFixed(4));
}

// ============================================================
//  PROCESS ONE LADDER TICK
// ============================================================
function tickLadder(ladder, price, capital) {
  const actions = [];
  price = parseFloat(price.toFixed(4));

  // SELL: price rose 0.10 above average entry
  if (ladder.totalShares > 0) {
    const avg = ladderAvg(ladder);
    if (price >= parseFloat((avg + SELL_PROFIT).toFixed(4))) {
      const proceeds = ladder.totalShares * price;
      const pnl = proceeds - ladder.totalCost;
      capital += proceeds;
      actions.push({ type:'SELL', side:ladder.side.toUpperCase(), price, shares:ladder.totalShares,
        avg: parseFloat(avg.toFixed(4)), pnl: parseFloat(pnl.toFixed(2)),
        msg:`✅ SELL ALL (${ladder.side.toUpperCase()}): ${ladder.totalShares} sh @ $${price} | avg $${avg.toFixed(4)} | PnL $${pnl.toFixed(2)}` });
      ladder.lastSoldPrice = price;
      ladder.positions     = [];
      ladder.totalShares   = 0;
      ladder.totalCost     = 0;
      return { actions, capital };
    }
  }

  // BUY: first entry or ladder step-down
  const reEntry = ladderReEntry(ladder);

  if (ladder.positions.length === 0) {
    const threshold = ladder.lastSoldPrice !== null ? reEntry : ENTRY_MAX;
    if (price >= ENTRY_MIN && price <= threshold && capital >= price * SHARES_PER_BUY) {
      const cost = price * SHARES_PER_BUY;
      capital -= cost;
      ladder.positions.push({ price, shares: SHARES_PER_BUY });
      ladder.totalShares += SHARES_PER_BUY;
      ladder.totalCost   += cost;
      actions.push({ type:'BUY', side:ladder.side.toUpperCase(), price, shares:SHARES_PER_BUY,
        avg: parseFloat(ladderAvg(ladder).toFixed(4)),
        msg:`🟢 BUY ${ladder.side.toUpperCase()}: ${SHARES_PER_BUY} sh @ $${price} | avg $${ladderAvg(ladder).toFixed(4)}` });
    }
  } else {
    const nextBuy = ladderNextBuy(ladder);
    if (nextBuy !== null && price <= nextBuy && price >= ENTRY_MIN && capital >= price * SHARES_PER_BUY) {
      const cost = price * SHARES_PER_BUY;
      capital -= cost;
      ladder.positions.push({ price, shares: SHARES_PER_BUY });
      ladder.totalShares += SHARES_PER_BUY;
      ladder.totalCost   += cost;
      actions.push({ type:'BUY', side:ladder.side.toUpperCase(), price, shares:SHARES_PER_BUY,
        avg: parseFloat(ladderAvg(ladder).toFixed(4)),
        msg:`🟢 ADD ${ladder.side.toUpperCase()}: ${SHARES_PER_BUY} sh @ $${price} | avg $${ladderAvg(ladder).toFixed(4)}` });
    }
  }

  return { actions, capital };
}

// ============================================================
//  SETTLE WINDOW
// ============================================================
function settleWindow(resolution) {
  const results = [];
  for (const [ladder, side] of [[S.upLadder,'up'],[S.downLadder,'down']]) {
    if (!ladder || ladder.totalShares === 0) continue;
    const won = resolution === side;
    const settlePrice = won ? 1.0 : 0.0;
    const proceeds    = ladder.totalShares * settlePrice;
    const pnl         = proceeds - ladder.totalCost;
    S.capital += proceeds;
    results.push({ side:side.toUpperCase(), shares:ladder.totalShares, settlePrice, pnl:parseFloat(pnl.toFixed(2)), won,
      msg:`${won?'🏆 WIN':'💀 LOSS'} (${side.toUpperCase()}): ${ladder.totalShares} sh @ $${settlePrice} | PnL $${pnl.toFixed(2)}` });
    ladder.positions     = [];
    ladder.totalShares   = 0;
    ladder.totalCost     = 0;
    ladder.lastSoldPrice = null;
  }
  return results;
}

// ============================================================
//  WHICH LADDERS ARE ACTIVE THIS WINDOW?
// ============================================================
function setActiveLadders() {
  if (!S.lastResolution) {
    S.activeLadders = 'both';
    log('📊 No prior window — both ladders active', 'info');
  } else if (S.lastResolution === 'up') {
    S.activeLadders = 'up';
    log('📈 Prev window UP — only UP ladder active this window', 'info');
  } else {
    S.activeLadders = 'down';
    log('📉 Prev window DOWN — only DOWN ladder active this window', 'info');
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
//  POLYMARKET API
// ============================================================
async function findWindows() {
  try {
    const now  = Math.floor(Date.now() / 1000);
    const base = Math.floor(now / 900) * 900;
    const results = [];

    for (let i = 0; i <= 3; i++) {
      const endsAt = base + i * 900 + 900;
      const slug   = `btc-updown-15m-${endsAt}`;
      try {
        const r = await fetch(`${POLY_API}/markets?slug=${slug}`, { timeout: 6000 });
        if (r.ok) {
          const data = await r.json();
          if (data && data.length > 0) {
            const m = data[0];
            results.push({ slug, marketId:m.id, endsAt, ts: endsAt-900,
              tokens: m.tokens||[], active:m.active, closed:m.closed });
          }
        }
      } catch {}
    }

    // Broad search fallback
    if (results.length === 0) {
      const r = await fetch(`${POLY_API}/markets?tag=BTC&limit=30&active=true`, { timeout: 8000 });
      if (r.ok) {
        const data = await r.json();
        (data || []).filter(m => m.slug?.includes('btc-updown-15m')).forEach(m => {
          const match = m.slug.match(/btc-updown-15m-(\d+)/);
          if (match) {
            const endsAt = parseInt(match[1]);
            results.push({ slug:m.slug, marketId:m.id, endsAt, ts:endsAt-900,
              tokens:m.tokens||[], active:m.active, closed:m.closed });
          }
        });
      }
    }
    return results;
  } catch(e) { return []; }
}

async function getPrices(upId, downId) {
  const out = { up: null, down: null };
  for (const [id, key] of [[upId,'up'],[downId,'down']]) {
    if (!id) continue;
    try {
      const r = await fetch(`${CLOB_API}/book?token_id=${id}`, { timeout: 4000 });
      if (r.ok) {
        const d = await r.json();
        const bids = d.bids||[], asks = d.asks||[];
        const b = bids.length ? parseFloat(bids[0].price) : 0;
        const a = asks.length ? parseFloat(asks[0].price) : 0;
        out[key] = b && a ? (b+a)/2 : b||a||0.5;
      }
    } catch {}
  }
  return out;
}

async function checkResolution(marketId) {
  try {
    const r = await fetch(`${POLY_API}/markets/${marketId}`, { timeout: 6000 });
    if (r.ok) {
      const d = await r.json();
      if (d.closed || d.resolved) {
        const winner = (d.tokens||[]).find(t => t.winner);
        if (winner) return { resolved:true, winner: winner.outcome?.toLowerCase().includes('up') ? 'up':'down' };
        if (d.outcomePrices) {
          const prices   = JSON.parse(d.outcomePrices);
          const outcomes = JSON.parse(d.outcomes||'[]');
          const idx      = prices.map(Number).indexOf(Math.max(...prices.map(Number)));
          return { resolved:true, winner: outcomes[idx]?.toLowerCase().includes('up') ? 'up':'down' };
        }
      }
    }
  } catch {}
  return { resolved:false };
}

// ============================================================
//  WEBSOCKET PRICE FEED
// ============================================================
function subscribeWS(assetIds) {
  if (!assetIds.length) { startSimPrices(); return; }

  let alive = true;
  function connect() {
    try {
      const ws = new WebSocket(POLY_WS);
      ws.on('open', () => {
        S.wsConnected = true;
        ws.send(JSON.stringify({ auth:{}, markets:assetIds, type:'Market' }));
        log('🔌 WebSocket connected to Polymarket', 'success');
      });
      ws.on('message', raw => {
        try {
          const msgs = JSON.parse(raw.toString());
          const arr  = Array.isArray(msgs) ? msgs : [msgs];
          let updated = false;
          for (const m of arr) {
            const price = parseFloat(m.price || m.best_ask || 0);
            if (!price) continue;
            if (m.asset_id === S.upTokenId)   { S.upPrice   = price; pushHistory('up',  price); updated = true; }
            if (m.asset_id === S.downTokenId) { S.downPrice = price; pushHistory('down',price); updated = true; }
          }
          if (updated) { S.lastPriceUpdate = Date.now(); processTick(); broadcastState(); }
        } catch {}
      });
      ws.on('close', () => {
        S.wsConnected = false;
        if (alive) setTimeout(connect, 3000);
      });
      ws.on('error', () => { S.wsConnected = false; });
      polyWs = ws;
      if (priceTimer) clearInterval(priceTimer);
      priceTimer = setInterval(async () => {
        if (S.windowStatus !== 'active') return;
        const p = await getPrices(S.upTokenId, S.downTokenId);
        if (p.up   !== null) { S.upPrice   = p.up;   pushHistory('up',  p.up);   }
        if (p.down !== null) { S.downPrice = p.down; pushHistory('down',p.down); }
        S.lastPriceUpdate = Date.now();
        processTick();
        broadcastState();
      }, 4000);
    } catch(e) { if (alive) setTimeout(connect, 5000); }
  }
  connect();
  return () => { alive=false; if(polyWs) polyWs.close(); if(priceTimer) clearInterval(priceTimer); };
}

// ============================================================
//  SIMULATION MODE
// ============================================================
function startSimPrices() {
  simPrice.up   = 0.56 + Math.random() * 0.20;
  simPrice.down = 0.56 + Math.random() * 0.20;
  if (simTimer) clearInterval(simTimer);
  simTimer = setInterval(() => {
    if (S.windowStatus !== 'active') { clearInterval(simTimer); return; }
    simPrice.up   = Math.max(0.30, Math.min(0.95, simPrice.up   + (Math.random()-0.49)*0.014));
    simPrice.down = Math.max(0.30, Math.min(0.95, simPrice.down + (Math.random()-0.49)*0.014));
    S.upPrice     = parseFloat(simPrice.up.toFixed(4));
    S.downPrice   = parseFloat(simPrice.down.toFixed(4));
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
    log('⚠️  No live windows found — starting simulation', 'warn');
    startSim();
    return;
  }

  const active   = windows.find(w => w.ts <= now && w.endsAt > now && !w.closed);
  const upcoming = windows.find(w => w.ts > now);

  if (active) {
    log(`✅ Found active window: ${active.slug}`, 'success');
    await startWindow(active, upcoming);
  } else if (upcoming) {
    const wait = upcoming.ts - now;
    log(`⏰ Next window in ${wait}s — ${upcoming.slug}`, 'info');
    S.windowStatus = 'waiting';
    S.nextWindow   = upcoming;
    broadcastState();
    setTimeout(() => startWindow(upcoming), wait * 1000);
  } else {
    log('⚠️  No windows found — retrying in 30s', 'warn');
    setTimeout(findAndStart, 30000);
  }
}

async function startWindow(win, next) {
  log(`🚀 Window started: ${win.slug}`, 'success');
  const tokens = win.tokens || [];
  let upId = null, downId = null;
  for (const t of tokens) {
    const o = (t.outcome||'').toLowerCase();
    if (o.includes('up'))   upId   = t.token_id || t.id;
    if (o.includes('down')) downId = t.token_id || t.id;
  }
  if (!upId   && tokens[0]) upId   = tokens[0].token_id || tokens[0].id;
  if (!downId && tokens[1]) downId = tokens[1].token_id || tokens[1].id;

  S.currentWindow  = win;
  S.nextWindow     = next || null;
  S.upTokenId      = upId;
  S.downTokenId    = downId;
  S.upLadder       = makeLadder('up');
  S.downLadder     = makeLadder('down');
  S.priceHistory   = { up:[], down:[] };
  S.windowStatus   = 'active';
  S.botStatus      = 'trading';

  setActiveLadders();

  if (upId || downId) {
    const p = await getPrices(upId, downId);
    if (p.up   !== null) S.upPrice   = p.up;
    if (p.down !== null) S.downPrice = p.down;
    subscribeWS([upId, downId].filter(Boolean));
  } else {
    log('⚠️  No token IDs — using simulation', 'warn');
    startSimPrices();
  }

  const timeLeft = Math.max(0, win.endsAt - Math.floor(Date.now()/1000));
  log(`⏱  Window ends in ${timeLeft}s`, 'info');
  setTimeout(() => endWindow(win), timeLeft * 1000);
  broadcastState();
}

async function endWindow(win) {
  log(`🏁 Window closing: ${win.slug}`, 'info');
  S.windowStatus = 'settling';
  broadcastState();

  if (polyWs)    { try { polyWs.close(); } catch {} polyWs = null; }
  if (priceTimer){ clearInterval(priceTimer); priceTimer = null; }
  if (simTimer)  { clearInterval(simTimer);  simTimer  = null;  }

  let resolution = null;
  if (win.marketId) {
    for (let i = 0; i < 10 && !resolution; i++) {
      const r = await checkResolution(win.marketId);
      if (r.resolved) { resolution = r.winner; log(`🎯 Resolution: ${resolution.toUpperCase()}`, 'success'); }
      else await new Promise(r => setTimeout(r, 3000));
    }
  }
  if (!resolution) {
    resolution = (S.upPrice||0.5) >= 0.5 ? 'up' : 'down';
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
    pnl:  parseFloat(windowPnl.toFixed(2)),
    capital: parseFloat(S.capital.toFixed(2)),
    activeLadders: S.activeLadders,
    ts: Date.now(), simulated: !!win.simulated
  });

  S.windowStatus = 'waiting';
  broadcastState();
  setTimeout(findAndStart, 2000);
}

function startSim() {
  const now = Math.floor(Date.now()/1000);
  const win = { slug:`btc-updown-15m-${now+900}`, marketId:null, endsAt:now+900, ts:now, simulated:true, tokens:[] };
  S.currentWindow  = win;
  S.upLadder       = makeLadder('up');
  S.downLadder     = makeLadder('down');
  S.priceHistory   = { up:[], down:[] };
  S.windowStatus   = 'active';
  S.botStatus      = 'trading (simulated)';
  setActiveLadders();
  S.upPrice   = 0.62;
  S.downPrice = 0.62;
  log('🎮 SIMULATION MODE active', 'warn');
  startSimPrices();
  setTimeout(() => endWindow(win), 900000);
  broadcastState();
}

// ============================================================
//  LOGGING + BROADCAST
// ============================================================
function log(msg, type='info') {
  const entry = { msg, type, ts: Date.now() };
  S.tradeLog.unshift(entry);
  if (S.tradeLog.length > 200) S.tradeLog.pop();
  broadcast('log', entry);
  console.log(`[${type.toUpperCase()}] ${msg}`);
}

function broadcast(type, payload) {
  const msg = JSON.stringify({ type, payload, ts: Date.now() });
  for (const c of dashClients) { if (c.readyState === WebSocket.OPEN) c.send(msg); }
}

function broadcastState() { broadcast('state', publicState()); }

function publicState() {
  const floating = S.capital + (S.upLadder?.totalShares||0)*(S.upPrice||0)
                             + (S.downLadder?.totalShares||0)*(S.downPrice||0);
  const now = Math.floor(Date.now()/1000);
  return {
    capital:         S.capital,
    startCapital:    S.startCapital,
    floatingCapital: parseFloat(floating.toFixed(2)),
    totalPnl:        S.totalPnl,
    windowsTraded:   S.windowsTraded,
    wins:            S.wins,
    losses:          S.losses,
    winRate:         S.windowsTraded > 0 ? ((S.wins/S.windowsTraded)*100).toFixed(1) : '0.0',
    lastResolution:  S.lastResolution,
    activeLadders:   S.activeLadders,
    currentWindow:   S.currentWindow ? {
      slug: S.currentWindow.slug, marketId: S.currentWindow.marketId,
      endsAt: S.currentWindow.endsAt, simulated: S.currentWindow.simulated,
      timeLeft: Math.max(0, S.currentWindow.endsAt - now),
    } : null,
    nextWindow:      S.nextWindow ? { slug: S.nextWindow.slug } : null,
    upLadder:        fmtLadder(S.upLadder, S.upPrice),
    downLadder:      fmtLadder(S.downLadder, S.downPrice),
    upPrice:         S.upPrice,
    downPrice:       S.downPrice,
    upTokenId:       S.upTokenId,
    downTokenId:     S.downTokenId,
    windowStatus:    S.windowStatus,
    windowHistory:   S.windowHistory.slice(0,20),
    tradeLog:        S.tradeLog.slice(0,50),
    priceHistory:    { up: S.priceHistory.up.slice(-150), down: S.priceHistory.down.slice(-150) },
    lastPriceUpdate: S.lastPriceUpdate,
    botStatus:       S.botStatus,
    wsConnected:     S.wsConnected,
    errors:          S.errors.slice(-5),
  };
}

function fmtLadder(l, price) {
  if (!l) return null;
  const avg    = ladderAvg(l);
  const unreal = l.totalShares > 0 && price ? (l.totalShares * price - l.totalCost) : 0;
  return {
    side: l.side, positions: l.positions, totalShares: l.totalShares,
    totalCost:     parseFloat(l.totalCost.toFixed(2)),
    avgEntry:      parseFloat(avg.toFixed(4)),
    lastSoldPrice: l.lastSoldPrice,
    nextBuy:       ladderNextBuy(l),
    reEntry:       ladderReEntry(l),
    unrealizedPnl: parseFloat(unreal.toFixed(2)),
  };
}

// ============================================================
//  EXPRESS + WS SERVER
// ============================================================
const app    = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/api/state',  (_, res) => res.json(publicState()));
app.get('/api/health', (_, res) => res.json({ ok:true, uptime: process.uptime() }));

const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path:'/ws' });

wss.on('connection', ws => {
  dashClients.add(ws);
  ws.send(JSON.stringify({ type:'state', payload: publicState(), ts: Date.now() }));
  ws.on('close',  () => dashClients.delete(ws));
  ws.on('error',  () => dashClients.delete(ws));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🚀 BTC Ladder Bot → http://localhost:${PORT}\n`);
  log(`Bot started. Capital: $${S.capital}`, 'success');
  setTimeout(findAndStart, 1000);
});
