# Spotify Logger

> Automatically track your Spotify listening history every 3 hours and store it in your own database.

**Quick Navigation:** [Jump to setup](#quick-start)

## What it does

- **Save More History**: Spotify only shows your last 50 songs. This tool keeps them all so you don't lose your history.

- **Data for Anything**: It provides a clean data source for any project — like this [dashboard](https://github.com/tyh-1/spotify-dashboard) or your own custom analysis.

- **Search with SQL**: Since everything is in your PostgreSQL, you can use SQL to filter or find any song you've played.

## Features

- **Runs automatically** every 3 hours via GitHub Actions
- **Your data stays private** - stored in your own Supabase database
- **Completely free** - uses GitHub and Supabase free tiers
- **Low maintenance** - just set it up once

## How it works

```
┌──────────────────────┐
│  Local Setup         │  (One-time)
│  OAuth → Get Token   │
└──────────┬───────────┘
           │
           ↓
┌──────────────────────┐
│  GitHub Actions      │   Every 3 hours
└──────────┬───────────┘
           │
           ↓
┌──────────────────────┐
│  Python Pipeline     │
│  1. Fetch (API)      │
│  2. Check New Tracks │  <-- Compare timestamp with Cache
│  3. Buffer in Cache  │  <-- Accumulate new data
│  4. Batch Flush      │  <-- Write to 5 tables if threshold met
└──────────┬───────────┘
           │
           ↓
┌──────────────────────┐
│  PostgreSQL (DB)     │
│  - Cache Table       │
│  - 5 Main Tables     │
└──────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.9+
- [Spotify Developer Account](https://developer.spotify.com/dashboard)
  - You'll need: `Client ID` and `Client Secret` (from Spotify), and a Redirect URI (you choose)
  - Steps:
    1. Create a new app
    2. Fill in app name and description (any name is fine)
    3. Set Redirect URI to `http://127.0.0.1:1410/callback` (use `127.0.0.1`, not `localhost` - Spotify requires this format. Port can be any available port)
    4. Copy your `Client ID` and `Client Secret`

- [Supabase Account](https://supabase.com) (free tier)
  - You'll need: database connection `URI`
  - Steps:
    1. Create a new project
    2. Enter a project name
    3. Save the database password
    4. Get your database connection `URI`
       - (a) Click the "Connect" button in Supabase Dashboard
       - (b) Stay on the "Connection String" tab
       - (c) Keep Type as "URI" and Source as "Primary Database"
       - (d) **Change Method from "Direct connection" to "Transaction pooler"**
       - (e) Copy the URI (e.g., `postgresql://postgres.project_id:[YOUR_PASSWORD]@aws-...`)
       - (f) Replace the password in the URI

- GitHub Account (for automated data collection)

### Local Setup

1. **Clone the repository**

```bash
   git clone https://github.com/tyh-1/spotify-logger.git
   cd spotify-logger
```

2. **(Optional) Create a virtual environment**

```bash
   python -m venv .venv
```

Activate the environment:

- **Windows:** `.venv\Scripts\activate`
- **Mac/Linux:** `source .venv/bin/activate`

3. **Install dependencies**

```bash
   pip install -r requirements.txt
```

4. **Configure environment variables**

   Create a folder named `env` inside `spotify-logger` folder, then create a file named `.env` inside `env` folder.

```env
   # file: env/.env
   # Replace with your actual values
   SPOTIFY_CLIENT_ID = your_spotify_client_id
   SPOTIFY_CLIENT_SECRET = your_spotify_client_secret
   SPOTIFY_REDIRECT_URI = "http://127.0.0.1:1410/callback"
   SUPABASE_URI = your_supabase_uri
```

5. **First-time authentication**

```bash
   python -m spotify_log.spotify_auth_code_flow
```

- Your browser will open asking you to log in to Spotify
- Click "Agree" to authorize the app
- You'll see "Authorization successful!"
- A `token.json` file will be created in `env/`
- **Open `token.json` and copy the `refresh_token` value** (you'll need this for GitHub Actions)

**Important:** Keep this file secure - it contains your Spotify access credentials

6. **Run the script**

```bash
   # Main script (runs automatically every 3 hours via GitHub Actions)
   # You can run it once locally to verify everything works
   python main.py

   # Update artist genres (run manually when needed)
   # Artist metadata is relatively stable, so periodic updates aren't necessary
   python update_artist_genres.py
```

### GitHub Actions Setup (Optional - for automated collection)

1. **Push to GitHub**

```bash
   git push origin main
```

**Note:** Ensure `env/` is in `.gitignore`

2. **Add secrets** (Settings → Secrets and variables → Actions)
   - `SPOTIFY_CLIENT_ID`
   - `SPOTIFY_CLIENT_SECRET`
   - `REFRESH_TOKEN` (from Local Setup step 5)
   - `SUPABASE_URI` (complete URI with password)

3. **Enable workflow**
   - Actions tab → Enable workflows
   - Runs every 3 hours automatically
   - Can trigger manually via "Run workflow" button

   **Tip:** To change frequency, edit the `cron` schedule in `.github/workflows/sync.yml`
