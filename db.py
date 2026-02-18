# db.py
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/bot.db")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(exist_ok=True, parents=True)
    with sqlite3.connect(db_path) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            pmid TEXT PRIMARY KEY,

            journal TEXT,
            publication_date TEXT,

            title_en TEXT,
            abstract_en TEXT,
            summary_en TEXT,
            authors_json TEXT,

            doi TEXT,
            link TEXT,
            pubmed_url TEXT,
            doi_url TEXT,

            fetched_at TEXT,

            title_ru TEXT,
            abstract_ru TEXT,
            summary_ru TEXT,
            tg_message_html TEXT,
            summarized_at TEXT,

            sent_at TEXT
        )
        """)
        # Lightweight migration for existing DBs created before abstract_ru existed.
        _ensure_column(con, "articles", "summary_en", "TEXT")
        _ensure_column(con, "articles", "abstract_ru", "TEXT")
        con.execute("CREATE INDEX IF NOT EXISTS idx_articles_fetched_at ON articles(fetched_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_articles_summarized_at ON articles(summarized_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_articles_sent_at ON articles(sent_at)")

        con.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY,
            is_active INTEGER NOT NULL DEFAULT 1,
            username TEXT,
            first_name TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_subscribers_active ON subscribers(is_active)")

        con.execute("""
        CREATE TABLE IF NOT EXISTS delivery_log (
            chat_id INTEGER NOT NULL,
            target_date TEXT NOT NULL,
            article_count INTEGER NOT NULL DEFAULT 0,
            sent_at TEXT NOT NULL,
            PRIMARY KEY(chat_id, target_date)
        )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_delivery_date ON delivery_log(target_date)")

        con.execute("""
        CREATE TABLE IF NOT EXISTS fetch_runs (
            target_date TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            fetched_count INTEGER NOT NULL DEFAULT 0,
            fetched_at TEXT NOT NULL
        )
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        con.commit()


def upsert_raw_articles(articles: list[dict], db_path: Path = DB_PATH) -> int:
    """
    Insert/update fetched articles (English). Keeps RU fields if already summarized.
    """
    if not articles:
        return 0

    init_db(db_path)
    now = _utc_now_iso()

    rows = []
    for a in articles:
        pmid = a.get("pmid")
        if not pmid:
            continue

        links = a.get("links") or {}
        doi = a.get("doi")
        pubmed_url = links.get("pubmed")
        doi_url = links.get("doi")
        link = doi_url or pubmed_url

        rows.append((
            pmid,
            a.get("journal"),
            a.get("publication_date"),
            a.get("title"),
            a.get("abstract"),
            json.dumps(a.get("authors", []), ensure_ascii=False),
            doi,
            link,
            pubmed_url,
            doi_url,
            now
        ))

    with sqlite3.connect(db_path) as con:
        con.executemany("""
        INSERT INTO articles (
            pmid, journal, publication_date, title_en, abstract_en, authors_json,
            doi, link, pubmed_url, doi_url, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pmid) DO UPDATE SET
            journal=excluded.journal,
            publication_date=excluded.publication_date,
            title_en=excluded.title_en,
            abstract_en=excluded.abstract_en,
            authors_json=excluded.authors_json,
            doi=excluded.doi,
            link=excluded.link,
            pubmed_url=excluded.pubmed_url,
            doi_url=excluded.doi_url,
            fetched_at=excluded.fetched_at
        """, rows)
        con.commit()

    return len(rows)


def get_unsummarized(
    limit: int = 100,
    db_path: Path = DB_PATH
) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("""
            SELECT *
            FROM articles
            WHERE summarized_at IS NULL
              AND abstract_en IS NOT NULL
              AND LENGTH(TRIM(abstract_en)) > 0
            ORDER BY fetched_at DESC
            LIMIT ?
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]


def get_existing_pmids(pmids: list[str], db_path: Path = DB_PATH) -> set[str]:
    if not pmids:
        return set()

    init_db(db_path)
    unique = sorted({p for p in pmids if p})
    if not unique:
        return set()

    placeholders = ",".join("?" for _ in unique)
    with sqlite3.connect(db_path) as con:
        cur = con.execute(
            f"SELECT pmid FROM articles WHERE pmid IN ({placeholders})",
            unique
        )
        return {str(r[0]) for r in cur.fetchall()}


def get_unsummarized_for_pmids(pmids: list[str], limit: int = 100, db_path: Path = DB_PATH) -> list[dict]:
    if not pmids:
        return []

    init_db(db_path)
    unique = sorted({p for p in pmids if p})
    if not unique:
        return []

    placeholders = ",".join("?" for _ in unique)
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute(
            f"""
            SELECT *
            FROM articles
            WHERE summarized_at IS NULL
              AND abstract_en IS NOT NULL
              AND LENGTH(TRIM(abstract_en)) > 0
              AND pmid IN ({placeholders})
            ORDER BY fetched_at DESC
            LIMIT ?
            """,
            (*unique, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def mark_summarized(
    pmid: str,
    title_ru: str,
    summary_en: str,
    summary_ru: str,
    tg_message_html: str,
    abstract_ru: str | None = None,
    db_path: Path = DB_PATH
) -> None:
    init_db(db_path)
    now = _utc_now_iso()
    with sqlite3.connect(db_path) as con:
        con.execute("""
            UPDATE articles
            SET title_ru = ?,
                summary_en = ?,
                abstract_ru = ?,
                summary_ru = ?,
                tg_message_html = ?,
                summarized_at = ?
            WHERE pmid = ?
        """, (title_ru, summary_en, abstract_ru, summary_ru, tg_message_html, now, pmid))
        con.commit()


def get_unsent(limit: int = 50, db_path: Path = DB_PATH) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("""
            SELECT *
            FROM articles
            WHERE summarized_at IS NOT NULL
              AND sent_at IS NULL
            ORDER BY summarized_at ASC
            LIMIT ?
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]


def mark_sent(pmid: str, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    now = _utc_now_iso()
    with sqlite3.connect(db_path) as con:
        con.execute("UPDATE articles SET sent_at = ? WHERE pmid = ?", (now, pmid))
        con.commit()


def upsert_subscriber(
    chat_id: int,
    is_active: bool = True,
    username: str | None = None,
    first_name: str | None = None,
    db_path: Path = DB_PATH
) -> None:
    init_db(db_path)
    now = _utc_now_iso()
    with sqlite3.connect(db_path) as con:
        con.execute("""
            INSERT INTO subscribers (chat_id, is_active, username, first_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                is_active = excluded.is_active,
                username = COALESCE(excluded.username, subscribers.username),
                first_name = COALESCE(excluded.first_name, subscribers.first_name),
                updated_at = excluded.updated_at
        """, (chat_id, 1 if is_active else 0, username, first_name, now, now))
        con.commit()


def set_subscription(chat_id: int, is_active: bool, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    now = _utc_now_iso()
    with sqlite3.connect(db_path) as con:
        con.execute("""
            INSERT INTO subscribers (chat_id, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
        """, (chat_id, 1 if is_active else 0, now, now))
        con.commit()


def is_subscribed(chat_id: int, db_path: Path = DB_PATH) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.execute("SELECT is_active FROM subscribers WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        return bool(row and int(row[0]) == 1)


def get_active_subscribers(db_path: Path = DB_PATH) -> list[int]:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.execute("SELECT chat_id FROM subscribers WHERE is_active = 1 ORDER BY chat_id ASC")
        return [int(r[0]) for r in cur.fetchall()]


def mark_delivery(chat_id: int, target_date: str, article_count: int, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    now = _utc_now_iso()
    with sqlite3.connect(db_path) as con:
        con.execute("""
            INSERT INTO delivery_log (chat_id, target_date, article_count, sent_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, target_date) DO UPDATE SET
                article_count = excluded.article_count,
                sent_at = excluded.sent_at
        """, (chat_id, target_date, article_count, now))
        con.commit()


def was_delivered(chat_id: int, target_date: str, db_path: Path = DB_PATH) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.execute(
            "SELECT 1 FROM delivery_log WHERE chat_id = ? AND target_date = ? LIMIT 1",
            (chat_id, target_date),
        )
        return cur.fetchone() is not None


def mark_fetch_run(target_date: str, mode: str, fetched_count: int, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    now = _utc_now_iso()
    with sqlite3.connect(db_path) as con:
        con.execute("""
            INSERT INTO fetch_runs (target_date, mode, fetched_count, fetched_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(target_date) DO UPDATE SET
                mode = excluded.mode,
                fetched_count = excluded.fetched_count,
                fetched_at = excluded.fetched_at
        """, (target_date, mode, fetched_count, now))
        con.commit()


def has_fetch_run(target_date: str, db_path: Path = DB_PATH) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.execute("SELECT 1 FROM fetch_runs WHERE target_date = ? LIMIT 1", (target_date,))
        return cur.fetchone() is not None


def set_state(key: str, value: str, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        con.execute("""
            INSERT INTO bot_state (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        con.commit()


def get_state(key: str, db_path: Path = DB_PATH) -> str | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def get_summarized_by_date(target_date: str, db_path: Path = DB_PATH) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("""
            SELECT *
            FROM articles
            WHERE summarized_at IS NOT NULL
              AND substr(COALESCE(publication_date, ''), 1, 10) = ?
            ORDER BY journal ASC, title_en ASC
        """, (target_date,))
        return [dict(r) for r in cur.fetchall()]


def get_summarized_between_dates(date_from: str, date_to: str, db_path: Path = DB_PATH) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("""
            SELECT *
            FROM articles
            WHERE summarized_at IS NOT NULL
              AND substr(COALESCE(publication_date, ''), 1, 10) >= ?
              AND substr(COALESCE(publication_date, ''), 1, 10) <= ?
            ORDER BY publication_date DESC, journal ASC, title_en ASC
        """, (date_from, date_to))
        return [dict(r) for r in cur.fetchall()]


def get_article_counts(db_path: Path = DB_PATH) -> dict[str, int]:
    init_db(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN summarized_at IS NOT NULL THEN 1 ELSE 0 END) AS summarized
            FROM articles
        """)
        row = cur.fetchone()
        total = int(row[0] or 0)
        summarized = int(row[1] or 0)
        return {
            "total": total,
            "summarized": summarized,
            "pending": max(total - summarized, 0),
        }
