# Med Articles Bot

Telegram bot and ingestion pipeline for medical articles from top journals in PubMed.

## What the project does

1. Fetches recent PubMed articles from configured journals.
2. Keeps only articles with abstracts.
3. Stores raw article metadata in SQLite.
4. Summarizes each abstract in English (LLM call #1).
5. Translates title + English summary to Russian (LLM call #2).
6. Stores RU/EN summaries and formatted Telegram message in DB.
7. Sends daily digest to subscribed Telegram users (previous day articles).

## Main behavior

- **Dedup by PMID:** already existing PMIDs are skipped on next runs.
- **Date normalization:** publication dates like `2026-Feb-17` are normalized to `2026-02-17`.
- **Daily schedule:** bot sends once per day for **previous day**.
- **Bootstrap:** on first bot start, it fetches/summarizes last 7 days.
- **Subscription control:** users can subscribe/unsubscribe from Telegram.

## Project files

- `pubmed_fetcher.py` — PubMed API fetch + XML parsing + date normalization.
- `summarize_ru.py` — LLM summarization/translation pipeline + progress/tokens output.
- `telegram_bot.py` — Telegram long-polling bot, scheduler, commands/buttons.
- `db.py` — SQLite schema and DB operations.
- `main.py` — one-off pipeline run.
- `config.py` — journals and fetch settings.

## Database (SQLite)

Main table: `articles`

Stores:
- PubMed metadata (`pmid`, title, abstract, journal, publication_date, links)
- LLM fields (`summary_en`, `title_ru`, `summary_ru`, `tg_message_html`)
- Process timestamps (`fetched_at`, `summarized_at`, `sent_at`)

Bot-related tables:
- `subscribers` — who is subscribed
- `delivery_log` — daily per-user delivery history
- `fetch_runs` — which day was fetched already
- `bot_state` — simple key-value state (e.g. first 7-day bootstrap done)

## Environment variables (`.env`)

Required:
- `PUBMED_API_KEY=...`
- `GEMINI_API_KEY=...`
- `TELEGRAM_BOT_TOKEN=...`

Optional:
- `BOT_TIMEZONE=Europe/Paris`
- `BOT_DAILY_HOUR=9`
- `BOT_DAILY_MINUTE=0`

## Run

### One-off pipeline

```bash
"/Users/ivmakiv/Documents/Study Aivancity/project AI clinic/AIclinicproject2/Doctor_Bot/.venv/bin/python" \
"/Users/ivmakiv/Documents/Study Aivancity/project AI clinic/AIclinicproject2/Doctor_Bot/main.py"
```

### Telegram bot

```bash
"/Users/ivmakiv/Documents/Study Aivancity/project AI clinic/AIclinicproject2/Doctor_Bot/.venv/bin/python" \
"/Users/ivmakiv/Documents/Study Aivancity/project AI clinic/AIclinicproject2/Doctor_Bot/telegram_bot.py"
```

## Telegram user actions

Buttons:
- `Подписаться`
- `Отписаться`
- `Статьи за неделю`

Commands:
- `/start`
- `/subscribe`
- `/unsubscribe`
- `/week`
- `/status`

## How to add this README to GitHub repo

From project folder:

```bash
cd "/Users/ivmakiv/Documents/Study Aivancity/project AI clinic/AIclinicproject2/Doctor_Bot"

git add README.md .gitignore
# or add everything you changed
# git add .

git commit -m "Add README with architecture and run instructions"
git push
```

If remote is not configured yet:

```bash
git remote add origin https://github.com/solgooodanalytics-star/Med_articles_bot.git
git branch -M main
git push -u origin main
```
