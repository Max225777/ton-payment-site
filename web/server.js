'use strict';

const path = require('path');
const express = require('express');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const num = (v, def) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};

const parseTiers = (raw) => {
  if (!raw) return [];
  return String(raw)
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n));
};

const config = () => ({
  coin: {
    name: process.env.COIN_NAME || 'YourCoin',
    ticker: process.env.COIN_TICKER || 'YCN',
    logoUrl: process.env.COIN_LOGO_URL || '/logo.svg',
    accent: process.env.ACCENT_COLOR || '#00c2ff',
    accent2: process.env.ACCENT_COLOR_2 || '#7b5bff',
  },
  rate: {
    baseUsdtPerCoin: num(process.env.BASE_RATE_USDT_PER_COIN, 0.0001),
    minCoin: num(process.env.MIN_COIN_AMOUNT, 0),
    maxCoin: num(process.env.MAX_COIN_AMOUNT, Number.POSITIVE_INFINITY),
  },
  bonus: {
    tiersPct: parseTiers(process.env.BONUS_TIERS_PCT),
    defaultPct: num(process.env.BONUS_DEFAULT_PCT, 0),
  },
  promo: {
    durationMinutes: num(process.env.PROMO_DURATION_MINUTES, 0),
    headline: process.env.PROMO_HEADLINE || '',
    subtext: process.env.PROMO_SUBTEXT || '',
  },
  deposit: {
    address: process.env.DEPOSIT_ADDRESS_COIN || '',
    network: process.env.DEPOSIT_NETWORK || 'TON',
  },
  social: {
    telegram: process.env.SOCIAL_TELEGRAM || '',
    twitter: process.env.SOCIAL_TWITTER || '',
    website: process.env.SOCIAL_WEBSITE || '',
    dexscreener: process.env.SOCIAL_DEXSCREENER || '',
    geckoterminal: process.env.SOCIAL_GECKOTERMINAL || '',
    github: process.env.SOCIAL_GITHUB || '',
  },
});

const bonusPctForIndex = (cfg, txIndex) => {
  const tiers = cfg.bonus.tiersPct;
  if (txIndex < 0) return 0;
  if (txIndex < tiers.length) return tiers[txIndex];
  return cfg.bonus.defaultPct;
};

app.get('/api/config', (_req, res) => {
  const cfg = config();
  res.json({
    coin: cfg.coin,
    rate: {
      baseUsdtPerCoin: cfg.rate.baseUsdtPerCoin,
      minCoin: cfg.rate.minCoin,
      maxCoin: Number.isFinite(cfg.rate.maxCoin) ? cfg.rate.maxCoin : null,
    },
    bonus: cfg.bonus,
    promo: cfg.promo,
    deposit: cfg.deposit,
    social: cfg.social,
  });
});

app.post('/api/quote', (req, res) => {
  const cfg = config();
  const amountCoin = Number(req.body?.amountCoin);
  const txIndex = Math.max(0, Math.floor(Number(req.body?.txIndex) || 0));

  if (!Number.isFinite(amountCoin) || amountCoin <= 0) {
    return res.status(400).json({ error: 'amountCoin must be a positive number' });
  }
  if (amountCoin < cfg.rate.minCoin) {
    return res.status(400).json({ error: `Minimum amount is ${cfg.rate.minCoin} ${cfg.coin.ticker}` });
  }
  if (Number.isFinite(cfg.rate.maxCoin) && amountCoin > cfg.rate.maxCoin) {
    return res.status(400).json({ error: `Maximum amount is ${cfg.rate.maxCoin} ${cfg.coin.ticker}` });
  }

  const baseUsdt = amountCoin * cfg.rate.baseUsdtPerCoin;
  const bonusPct = bonusPctForIndex(cfg, txIndex);
  const bonusUsdt = baseUsdt * (bonusPct / 100);
  const totalUsdt = baseUsdt + bonusUsdt;

  res.json({
    amountCoin,
    txIndex,
    bonusPct,
    baseUsdt,
    bonusUsdt,
    totalUsdt,
    effectiveRateUsdtPerCoin: cfg.rate.baseUsdtPerCoin * (1 + bonusPct / 100),
    deposit: cfg.deposit,
  });
});

app.get('/healthz', (_req, res) => res.json({ ok: true }));

const port = num(process.env.PORT, 3000);
app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`[exchange-promo] listening on http://localhost:${port}`);
});
