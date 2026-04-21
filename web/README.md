# DUST Promo Exchange — Swap DUST → USDT

Community-style landing page for a token → USDT swap with **configurable
per-transaction bonus tiers** (e.g. 1st swap +2.5 %, 2nd +1.75 %, 3rd +3.65 %).

Works in two modes without any code changes:

| Mode | Source of truth | Hosts |
| --- | --- | --- |
| **Static**  | `public/config.json` | GitHub Pages, Netlify, Cloudflare Pages, S3, any static host |
| **Server**  | `.env` via `server.js` | Vercel, Render, Fly, Railway, VPS, Docker |

The frontend first tries `./api/config` (server mode) and automatically falls
back to `./config.json` (static mode).

> This is a UI + quote prototype. It does not move funds on-chain. Any real
> payout flow you wire up must honor the rates the UI shows up-front.

---

## Quick start — local (server mode)

```bash
cd web
cp .env.example .env
# edit .env as needed
npm install
npm start
# open http://localhost:3000
```

## Quick start — local (static mode)

No Node required — just any static file server pointing at `web/public`:

```bash
cd web/public
python3 -m http.server 8000
# open http://localhost:8000
```

Edit `public/config.json` to change branding, rate, bonus tiers, deposit
address, social links.

---

## Deploy targets

### 1. GitHub Pages (static, free)

A workflow is already provided at `.github/workflows/pages.yml`. It publishes
`web/public/` whenever that folder changes on `main` or this feature branch.

Enable it once:

1. Repo Settings → **Pages** → Build and deployment → Source: **GitHub Actions**.
2. Push any change under `web/public/` (or trigger the workflow manually
   from the Actions tab → "Deploy web/public to GitHub Pages" → Run workflow).
3. Site URL will be shown in the Actions run summary
   (`https://<user>.github.io/<repo>/`).

Site subpath is handled: all assets are referenced via `./` so it works under
`/<repo>/` without extra config.

### 2. Netlify (static)

`web/netlify.toml` is provided. Connect the repo, Netlify picks it up; no
build step needed — it serves `web/public/` directly.

### 3. Cloudflare Pages (static)

- Build command: *(empty)*
- Build output: `web/public`

### 4. Vercel (server mode with `.env`)

`web/vercel.json` is provided. Connect the repo, set the "Root Directory" to
`web/`. Add the variables from `.env.example` in Project → Settings →
Environment Variables.

### 5. Render (server mode)

`web/render.yaml` is provided. `New → Blueprint` and point Render at the
repo; add the env vars from `.env.example` in the dashboard.

### 6. Docker / any VPS

```bash
cd web
docker build -t promo-exchange .
docker run --rm -p 3000:3000 --env-file .env promo-exchange
```

---

## Configuration

### Static mode — `public/config.json`

```json
{
  "coin":   { "name": "DUST", "ticker": "DUST", "logoUrl": "./logo.svg", "accent": "#f4b840", "accent2": "#ff7a1a" },
  "rate":   { "baseUsdtPerCoin": 0.0001, "minCoin": 100, "maxCoin": 1000000 },
  "bonus":  { "tiersPct": [2.5, 1.75, 3.65], "defaultPct": 0 },
  "promo":  { "durationMinutes": 5, "headline": "Limited 5-minute boost", "subtext": "..." },
  "deposit":{ "address": "EQxxx...", "network": "TON" },
  "social": { "telegram": "...", "twitter": "...", "website": "", "dexscreener": "", "geckoterminal": "", "github": "" }
}
```

### Server mode — `.env`

See `.env.example`. Key switches:

| Key | Purpose |
| --- | --- |
| `COIN_*`, `ACCENT_*` | Branding |
| `BASE_RATE_USDT_PER_COIN` | Base exchange rate |
| `MIN_COIN_AMOUNT`, `MAX_COIN_AMOUNT` | Order limits |
| `BONUS_TIERS_PCT` | Comma-separated bonus % per exchange number |
| `BONUS_DEFAULT_PCT` | Bonus after the list is exhausted |
| `PROMO_DURATION_MINUTES` | Timer length (0 hides it) |
| `PROMO_HEADLINE`, `PROMO_SUBTEXT` | Promo panel copy |
| `DEPOSIT_ADDRESS_COIN`, `DEPOSIT_NETWORK` | Receiving wallet |
| `SOCIAL_*` | Footer links |

### Bonus tiers — what they do

```
BONUS_TIERS_PCT=2.5,1.75,3.65
BONUS_DEFAULT_PCT=0
```

- Exchange #1 → base rate × 1.025
- Exchange #2 → base rate × 1.0175
- Exchange #3 → base rate × 1.0365
- Exchange #4+ → base rate (0 % bonus)

The exchange counter is stored in the visitor's `localStorage`
(`promoExchange.txCount`). Users can reset it from the UI.

---

## Endpoints (server mode only)

- `GET /api/config` – returns the same shape as `config.json`
- `POST /api/quote` – `{ amountCoin, txIndex }` → `{ baseUsdt, bonusUsdt, totalUsdt, effectiveRateUsdtPerCoin, deposit }`
- `GET /healthz` – liveness probe

In static mode the frontend computes these values itself from `config.json`.

---

## Integrity notes

- The rate shown to the user **is** the rate used. Any real payout flow must
  pay `totalUsdt` exactly for a given order.
- Don't add hidden rate changes based on order size, user history, or any
  off-screen signal — that isn't a promo, that's fraud, and it's not
  supported here.
- If you want size-based slippage, make it a visible extra config key and
  display it in the breakdown before the user confirms.
