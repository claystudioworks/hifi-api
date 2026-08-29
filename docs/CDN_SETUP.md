# Hifi-API Bulk CDN Setup — VPS + Drive Worker

Turn your VPS into a private Tidal CDN with **<2% ban risk**. All Tidal hits go through the per-account limiter (1 rps burst 3) + 3-8s jitter.

## Architecture

```
[Tidal] <-(1 rps/account)- [hifi-api :8000] <-(3-8s jitter, 1 concurrent)- [hifi-sync] --POST /api/files--> [Cloudflare Worker (tas33n)] --> [Google Drive + 100 SAs] --> [Cloudflare Edge] --> [sadda / your app]
                                  ^                           |
                                  |--- GET /cdn/{id} 302 -----|
                                  |--- POST /sync/enqueue ---|
```

- **hifi-api** = Tidal proxy (anti-ban, cache, batch, metrics)
- **hifi-sync** = bulk daemon (polls `sync_jobs`, downloads FLAC via hifi-api, uploads to Worker)
- **Drive Worker** = `tas33n/Google-drive-cdn-worker` — handles 100 service accounts, Range streaming, Cloudflare CDN

## 1. Deploy Drive Worker (5 min)

Follow https://github.com/tas33n/Google-drive-cdn-worker :

```bash
git clone https://github.com/tas33n/Google-drive-cdn-worker
cd Google-drive-cdn-worker
npm install
npm run bootstrap:google         # OAuth + Drive folder
npm run bootstrap:service-accounts # 10-100 SAs, optional but recommended
# wrangler kv namespace create UPLOAD_SESSIONS etc — see worker README
wrangler secret put API_TOKENS   # e.g. `my-secret-token123`
# set DRIVE_UPLOAD_ROOT and CDN_BASE_URL in wrangler.toml
npm run deploy
# -> https://cdn.yourdomain.workers.dev
# test: curl -X POST https://cdn.yourdomain.workers.dev/api/files -H "Authorization: Bearer my-secret-token123" -F file=@test.jpg
```

## 2. VPS Host hifi-api + hifi-sync (Docker)

```bash
# On VPS (Ubuntu 22.04, 2GB RAM, 50GB SSD)
git clone https://github.com/claystudioworks/hifi-api
cd hifi-api
cp .env.example .env
# edit .env:
# DATABASE_URL=/data/hifi.db
# ADMIN_KEY=change-me
# CDN_WORKER_URL=https://cdn.yourdomain.workers.dev
# CDN_WORKER_TOKEN=my-secret-token123
# HIFI_API_URL=http://hifi-api:8000

# Add Tidal accounts via env or admin panel
# Option A: env
echo "CLIENT_ID=lw3vR6GE1vtNBsjv" >> .env
echo "CLIENT_SECRET=Y8tIpqKJxs9BEIwYr0I9bSbMWDsogXJx9LaN3mCHwD4%3D" >> .env
echo "REFRESH_TOKEN=..." >> .env

docker compose up -d --build
docker compose logs -f hifi-api
# Admin: http://YOUR_VPS_IP:8000/admin (X-Admin-Key if set)
# Metrics: http://YOUR_VPS_IP:8000/metrics
# Health: http://YOUR_VPS_IP:8000/health
```

Single-image alternative (if you prefer one container):

```bash
docker build -t hifi-api .
docker run -d -p 8000:8000 \
  -v hifi-data:/data \
  -e DATABASE_URL=/data/hifi.db \
  -e CDN_WORKER_URL=https://cdn.yourdomain.workers.dev \
  -e CDN_WORKER_TOKEN=my-secret-token123 \
  hifi-api
# CMD runs both hifi-api & hifi-sync together
```

## 3. Enqueue Bulk Download

```bash
# Add one track
curl -X POST http://YOUR_VPS:8000/sync/enqueue \
  -H "Content-Type: application/json" \
  -d '{"trackIds":["427520487"]}'

# Add whole album (expands to tracks with 800ms jitter + per-account limiter)
curl -X POST http://YOUR_VPS:8000/sync/enqueue \
  -H "Content-Type: application/json" \
  -d '{"albumIds":["58990510"]}'

# Add playlist
curl -X POST http://YOUR_VPS:8000/sync/enqueue \
  -H "Content-Type: application/json" \
  -d '{"playlistIds":["626d146b-04f6-4936-bbf6-a65318f740a1"]}'

# Check queue
curl http://YOUR_VPS:8000/sync/status
# {"pending":12,"downloading":0,"uploading":0,"done":0,"failed":0,"cached":0,"bytes_today":0,"worker_configured":true}

# Logs
docker compose logs -f hifi-sync
```

**Anti-ban defaults (do not lower):**
- 1 concurrent download + 1 concurrent upload
- 3-8s jitter between tracks, 800ms+ jitter between album expands
- Per-account 1 rps burst 3 (auto-skip if hit)
- EWMA auto-disable at error rate >0.3
- Daily guard: 500 pending max, 750GB hard stop

For 10k tracks: `10k * 5s avg ≈ 14 hours` — intentionally slow to stay under Tidal radar. Do not raise concurrency.

## 4. Use CDN in sadda / your app

```js
// Before: direct Tidal via hifi-api
// GET http://YOUR_VPS:8000/track/?id=427520487&quality=LOSSLESS

// After: CDN — 302 to Worker edge cache (Range support)
const cdnUrl = `http://YOUR_VPS:8000/cdn/427520487`;
// sadda player: <audio src={cdnUrl} /> — will 302 to https://cdn.yourdomain.workers.dev/files/{driveId}
// For batch (N+1 savings):
const batch = await fetch("http://YOUR_VPS:8000/batch", {
  method: "POST",
  body: JSON.stringify({trackIds: [427520487, 58990511]}),
  headers: {"Content-Type":"application/json"}
});
```

## 5. Monitoring

- `GET /metrics` → Prometheus `hifi_requests_total`, `hifi_429_total`
- `GET /sync/status` → queue health
- `GET /admin` → account health (EWMA, rate_limit_hits)
- Logs: `hifi-sync` logs every jitter + worker upload result

## 6. VPS Sizing

- 1000 tracks (~100GB FLAC): 20GB SSD + 2GB RAM VPS ($6/mo Hetzner)
- 10k tracks (~1TB): 1TB SSD or mount R2/S3 as backup, 4GB RAM
- Scale: add more Tidal accounts (5-10) — hifi-api distributes via weighted scoring. More accounts = faster bulk (still 1 rps per account).

## 7. Legal

Private use only, valid Tidal subscription required. Do not expose CDN publicly with copyrighted tracks. Tidal is currently mass-banning accounts — even homelab users — so keep `HIFI_API_URL` behind VPN / `ADMIN_KEY`.

---

**Need help?** Open an issue at `claystudioworks/hifi-api` with `bytes_today` and `pending` from `/sync/status`.
