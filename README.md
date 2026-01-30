# Garmin to Notion Sync (Optimized)

Sync your Garmin Connect data to Notion databases automatically via GitHub Actions.

## ⚡ Performance Optimizations

This version includes major performance improvements:

| Original | Optimized | Improvement |
|----------|-----------|-------------|
| 4 separate logins | 1 unified login | 4x fewer auth calls |
| 10,000+ activities | Last 7 days only | ~99% faster |
| 730 days sleep/steps | Last 7 days only | ~99% faster |
| ~25-40 min runtime | ~1-2 min runtime | 15-20x faster |

## 🔐 Authentication Methods

### Option 1: Tokenstore (Recommended)

Uses persisted OAuth tokens - no email/password login at each run.

**Setup:**
1. Take your existing Garmin OAuth token JSON (the format with `oauth1_token` and `oauth2_token`)
2. Encode it to base64:
   ```bash
   cat your_token.json | base64 -w 0
   ```
3. Add as GitHub Secret: `GARMIN_TOKENSTORE_B64`

**Benefits:**
- Faster (no login handshake)
- More reliable (no Cloudflare blocks)
- Works even if Garmin changes login flow

### Option 2: Email/Password (Fallback)

Traditional login - used if tokenstore is missing or invalid.

**Secrets needed:**
- `GARMIN_EMAIL`
- `GARMIN_PASSWORD`

## 📋 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SYNC_DAYS` | `7` | Number of days to sync |
| `SYNC_ALL` | `false` | Set to `true` for full history sync |
| `GARMIN_TOKENSTORE_PATH` | `~/.garminconnect/tokens.json` | Path to tokenstore |

## 🚀 Setup

### 1. Create Notion Databases

Create databases for:
- **Activities** (main workout tracking)
- **Personal Records** (PRs)
- **Daily Steps** (optional)
- **Sleep Data** (optional)

### 2. Configure GitHub Secrets

| Secret | Required | Description |
|--------|----------|-------------|
| `GARMIN_TOKENSTORE_B64` | ⭐ Recommended | Base64-encoded OAuth tokens |
| `GARMIN_EMAIL` | Fallback | Garmin Connect email |
| `GARMIN_PASSWORD` | Fallback | Garmin Connect password |
| `NOTION_TOKEN` | ✅ Yes | Notion integration token |
| `NOTION_DB_ID` | ✅ Yes | Activities database ID |
| `NOTION_PR_DB_ID` | Optional | Personal Records database ID |
| `NOTION_STEPS_DB_ID` | Optional | Daily Steps database ID |
| `NOTION_SLEEP_DB_ID` | Optional | Sleep database ID |

### 3. Enable GitHub Actions

The workflow runs daily at 1:00 UTC by default.

**Manual run with options:**
1. Go to Actions → "Sync Garmin to Notion"
2. Click "Run workflow"
3. Choose:
   - **sync_all**: Check for full history sync
   - **sync_days**: Enter number of days (e.g., 30)

## 📁 Files

| File | Purpose |
|------|---------|
| `sync.py` | **Main entry point** - unified sync with single login |
| `requirements.txt` | Pinned dependencies |
| `garmin-activities.py` | Standalone activities sync (legacy) |
| `sleep-data.py` | Standalone sleep sync (legacy) |
| `daily-steps.py` | Standalone steps sync (legacy) |
| `personal-records.py` | Standalone PR sync (legacy) |

## 🔧 Dependencies

```
garminconnect==0.2.38
garth==0.5.18
notion-client==2.2.1
pytz==2024.1
lxml>=4.6.0,<5.0
python-dotenv>=1.0.0
```

## 🐛 Troubleshooting

### "OAuth1 token error" or "Not Found"
→ Your tokenstore may be expired. Re-generate from a fresh Garmin login.

### "Login failed" with email/password
→ Garmin may be blocking. Switch to tokenstore method.

### Full History Sync
For initial setup or recovery:
```
workflow_dispatch → sync_all = true
```

---

*Based on the original garmin-to-notion project with tokenstore + unified sync optimizations.*
