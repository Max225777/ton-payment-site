# Promo exchange landing (TON ecosystem)

A simple landing page that presents a token → USDT swap with **configurable
per-transaction bonus tiers** (e.g. 1st swap +2.5 %, 2nd +1.75 %, 3rd +3.65 %).
All rates, tiers, branding and deposit addresses come from `.env` — no logic is
hidden in the code.

> This is a UI / quote prototype. It does not move any funds on-chain. Hooking
> it up to a real backend wallet and payout flow is your responsibility and
> must be done transparently (the displayed rate must equal the paid rate).

## Setup

```bash
cd web
cp .env.example .env
# edit .env: branding, base rate, bonus tiers, deposit address, socials
npm install
npm start
# open http://localhost:3000
```

## Configuration (`.env`)

| Key | Purpose |
| --- | --- |
| `COIN_NAME`, `COIN_TICKER`, `COIN_LOGO_URL` | Branding shown in UI |
| `ACCENT_COLOR`, `ACCENT_COLOR_2` | Gradient accent colors |
| `BASE_RATE_USDT_PER_COIN` | Base exchange rate |
| `MIN_COIN_AMOUNT`, `MAX_COIN_AMOUNT` | Order limits |
| `BONUS_TIERS_PCT` | **Comma-separated** bonus % per exchange number |
| `BONUS_DEFAULT_PCT` | Bonus after the tier list is exhausted |
| `PROMO_DURATION_MINUTES` | Timer length (0 to hide the timer) |
| `PROMO_HEADLINE`, `PROMO_SUBTEXT` | Promo panel copy |
| `DEPOSIT_ADDRESS_COIN`, `DEPOSIT_NETWORK` | Receiving wallet shown to users |
| `SOCIAL_*` | Footer social links |

### Bonus tiers example

```
BONUS_TIERS_PCT=2.5,1.75,3.65
BONUS_DEFAULT_PCT=0
```

- Exchange #1 → rate × 1.025
- Exchange #2 → rate × 1.0175
- Exchange #3 → rate × 1.0365
- Exchange #4+ → base rate (0 % bonus)

The exchange counter is stored in the visitor's `localStorage`
(`promoExchange.txCount`). Users can reset it themselves from the UI.

## Endpoints

- `GET /api/config` – returns all public site configuration
- `POST /api/quote` – `{ amountCoin, txIndex }` → returns `{ baseUsdt, bonusUsdt, totalUsdt, effectiveRateUsdtPerCoin, deposit }`
- `GET /healthz` – liveness probe

## Integrity notes

- The rate shown to the user **is** the rate computed on the backend from
  `BASE_RATE_USDT_PER_COIN` and `BONUS_TIERS_PCT`. If you integrate a real
  payout flow, pay out exactly `totalUsdt` for a given order.
- Do not add hidden rate changes based on order size, user history, or
  destination — that's not a promo, that's fraud, and it isn't supported here.
- If you need size-based slippage, add it to `BONUS_TIERS_PCT` or extend the
  config explicitly and show it in the UI before the user confirms.
