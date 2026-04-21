'use strict';

const $ = (id) => document.getElementById(id);

const state = {
  cfg: null,
  quote: null,
  promoEndsAt: null,
  timerInterval: null,
};

const LS_TX_COUNT = 'promoExchange.txCount';
const LS_PROMO_END = 'promoExchange.promoEndsAt';

const getTxCount = () => {
  const v = Number(localStorage.getItem(LS_TX_COUNT));
  return Number.isFinite(v) && v >= 0 ? Math.floor(v) : 0;
};
const setTxCount = (n) => localStorage.setItem(LS_TX_COUNT, String(n));

const fmtUsdt = (n) => (Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: 6 }) : '–');
const fmtCoin = (n) => (Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: 8 }) : '–');
const fmtPct = (n) => `${Number(n).toLocaleString(undefined, { maximumFractionDigits: 3 })}%`;

const bonusPctForIndex = (cfg, idx) => {
  const t = cfg.bonus.tiersPct || [];
  if (idx >= 0 && idx < t.length) return t[idx];
  return cfg.bonus.defaultPct || 0;
};

async function loadConfig() {
  const r = await fetch('/api/config');
  if (!r.ok) throw new Error('Failed to load config');
  state.cfg = await r.json();
  applyBranding();
  renderTiers();
  renderSocials();
  renderCounter();
  renderPromoHeader();
  setupPromoTimer();
  recalcQuoteLocal();
}

function applyBranding() {
  const { coin, rate, deposit } = state.cfg;
  document.documentElement.style.setProperty('--accent', coin.accent);
  document.documentElement.style.setProperty('--accent-2', coin.accent2);
  $('brandName').textContent = coin.name;
  $('brandLogo').src = coin.logoUrl;
  $('footerName').textContent = coin.name;
  $('footerLogo').src = coin.logoUrl;
  $('assetFromLogo').src = coin.logoUrl;
  $('heroTicker').textContent = coin.ticker;
  $('assetFromTicker').textContent = coin.ticker;
  document.querySelectorAll('.rateTicker, .brandTicker').forEach((el) => { el.textContent = coin.ticker; });
  $('networkChip').textContent = deposit.network || 'TON';
  $('minCoin').textContent = rate.minCoin ? fmtCoin(rate.minCoin) : '–';
  $('maxCoin').textContent = rate.maxCoin ? fmtCoin(rate.maxCoin) : '∞';
  const heroRate = $('heroRate');
  if (heroRate) heroRate.textContent = rate.baseUsdtPerCoin.toLocaleString(undefined, { maximumFractionDigits: 8 });
  document.title = `Swap ${coin.ticker} → USDT`;
}

function renderTiers() {
  const list = $('tiersList');
  list.innerHTML = '';
  const tiers = state.cfg.bonus.tiersPct || [];
  const idx = getTxCount();
  tiers.forEach((pct, i) => {
    const li = document.createElement('li');
    li.innerHTML = `<span class="num">Exchange #${i + 1}</span><span class="pct">${fmtPct(pct)}</span>`;
    if (i === idx) li.classList.add('active');
    list.appendChild(li);
  });
  $('tiersDefault').textContent = fmtPct(state.cfg.bonus.defaultPct || 0);
}

function renderSocials() {
  const s = state.cfg.social;
  const items = [
    ['Telegram', s.telegram],
    ['X / Twitter', s.twitter],
    ['Website', s.website],
    ['DexScreener', s.dexscreener],
    ['GeckoTerminal', s.geckoterminal],
    ['GitHub', s.github],
  ].filter(([, url]) => Boolean(url));
  const nav = $('socials');
  nav.innerHTML = '';
  items.forEach(([label, url]) => {
    const a = document.createElement('a');
    a.href = url; a.target = '_blank'; a.rel = 'noopener noreferrer';
    a.textContent = label;
    nav.appendChild(a);
  });
}

function renderCounter() {
  $('txCount').textContent = String(getTxCount());
  const pct = bonusPctForIndex(state.cfg, getTxCount());
  $('bonusPctLabel').textContent = fmtPct(pct);
}

function renderPromoHeader() {
  const { promo } = state.cfg;
  $('promoHeadline').textContent = promo.headline || 'Promo';
  $('promoSubtext').textContent = promo.subtext || '';
}

function setupPromoTimer() {
  const { promo } = state.cfg;
  if (!promo.durationMinutes || promo.durationMinutes <= 0) {
    $('timerBox').hidden = true;
    return;
  }
  $('timerBox').hidden = false;
  const savedEnd = Number(localStorage.getItem(LS_PROMO_END));
  if (Number.isFinite(savedEnd) && savedEnd > Date.now()) {
    state.promoEndsAt = savedEnd;
    startTimerTick();
    showTimerActive();
  } else {
    showTimerIdle();
  }

  $('startTimerBtn').addEventListener('click', () => {
    const end = Date.now() + promo.durationMinutes * 60 * 1000;
    state.promoEndsAt = end;
    localStorage.setItem(LS_PROMO_END, String(end));
    startTimerTick();
    showTimerActive();
  });
  $('resetTimerBtn').addEventListener('click', () => {
    state.promoEndsAt = null;
    localStorage.removeItem(LS_PROMO_END);
    stopTimerTick();
    $('timerValue').textContent = `${String(promo.durationMinutes).padStart(2, '0')}:00`;
    showTimerIdle();
  });
  $('timerValue').textContent = `${String(promo.durationMinutes).padStart(2, '0')}:00`;
}

function showTimerActive() {
  $('startTimerBtn').hidden = true;
  $('resetTimerBtn').hidden = false;
}
function showTimerIdle() {
  $('startTimerBtn').hidden = false;
  $('resetTimerBtn').hidden = true;
}
function startTimerTick() {
  stopTimerTick();
  const tick = () => {
    if (!state.promoEndsAt) return;
    const ms = Math.max(0, state.promoEndsAt - Date.now());
    const s = Math.floor(ms / 1000);
    const mm = String(Math.floor(s / 60)).padStart(2, '0');
    const ss = String(s % 60).padStart(2, '0');
    $('timerValue').textContent = `${mm}:${ss}`;
    if (ms <= 0) {
      stopTimerTick();
      state.promoEndsAt = null;
      localStorage.removeItem(LS_PROMO_END);
      showTimerIdle();
    }
  };
  tick();
  state.timerInterval = setInterval(tick, 500);
}
function stopTimerTick() {
  if (state.timerInterval) clearInterval(state.timerInterval);
  state.timerInterval = null;
}

function recalcQuoteLocal() {
  const amt = Number($('amountCoin').value);
  const idx = getTxCount();
  const pct = bonusPctForIndex(state.cfg, idx);
  const rate = state.cfg.rate.baseUsdtPerCoin * (1 + pct / 100);
  $('effectiveRate').textContent = rate.toLocaleString(undefined, { maximumFractionDigits: 8 });
  $('bonusPctLabel').textContent = fmtPct(pct);

  const valid =
    Number.isFinite(amt) && amt > 0 &&
    amt >= (state.cfg.rate.minCoin || 0) &&
    (!state.cfg.rate.maxCoin || amt <= state.cfg.rate.maxCoin);

  if (!valid) {
    $('amountUsdt').value = '';
    $('breakdown').hidden = true;
    $('swapBtn').disabled = true;
    return;
  }

  const base = amt * state.cfg.rate.baseUsdtPerCoin;
  const bonus = base * (pct / 100);
  const total = base + bonus;
  $('amountUsdt').value = fmtUsdt(total);
  $('baseUsdt').textContent = fmtUsdt(base);
  $('bonusUsdt').textContent = fmtUsdt(bonus);
  $('totalUsdt').textContent = fmtUsdt(total);
  $('bonusPctInline').textContent = fmtPct(pct);
  $('breakdown').hidden = false;
  $('swapBtn').disabled = false;
}

async function fetchQuote() {
  const amt = Number($('amountCoin').value);
  const idx = getTxCount();
  const r = await fetch('/api/quote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ amountCoin: amt, txIndex: idx }),
  });
  if (!r.ok) {
    const err = await r.json().catch(() => ({ error: 'Quote failed' }));
    alert(err.error || 'Quote failed');
    return null;
  }
  return r.json();
}

async function onGetDeposit() {
  const q = await fetchQuote();
  if (!q) return;
  state.quote = q;
  $('depositAmount').textContent = fmtCoin(q.amountCoin);
  $('depositAddress').textContent = q.deposit.address || '(not configured)';
  $('depositBox').hidden = false;
  $('depositBox').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function onCopyAddress() {
  const text = $('depositAddress').textContent;
  if (!text || text === '(not configured)') return;
  navigator.clipboard.writeText(text).then(() => {
    const btn = $('copyBtn'); const old = btn.textContent;
    btn.textContent = 'Copied!'; setTimeout(() => { btn.textContent = old; }, 1200);
  }).catch(() => {});
}

function onMarkDone() {
  setTxCount(getTxCount() + 1);
  renderCounter();
  renderTiers();
  recalcQuoteLocal();
  $('depositBox').hidden = true;
  $('amountCoin').value = '';
  $('amountUsdt').value = '';
  $('breakdown').hidden = true;
  $('swapBtn').disabled = true;
}

function onResetCount() {
  setTxCount(0);
  renderCounter();
  renderTiers();
  recalcQuoteLocal();
}

function onMax() {
  if (state.cfg.rate.maxCoin) {
    $('amountCoin').value = String(state.cfg.rate.maxCoin);
    recalcQuoteLocal();
  }
}

document.addEventListener('DOMContentLoaded', () => {
  $('amountCoin').addEventListener('input', recalcQuoteLocal);
  $('swapBtn').addEventListener('click', onGetDeposit);
  $('copyBtn').addEventListener('click', onCopyAddress);
  $('markDoneBtn').addEventListener('click', onMarkDone);
  $('resetCountBtn').addEventListener('click', onResetCount);
  $('maxBtn').addEventListener('click', onMax);
  loadConfig().catch((e) => {
    console.error(e);
    alert('Failed to load site configuration.');
  });
});
