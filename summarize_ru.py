import json
import os
import random
import re
import time
from datetime import datetime
from html import escape as html_escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs):
        return False

try:
    from google import genai
    from google.genai import types
except ModuleNotFoundError:
    genai = None
    types = None

from db import (
    get_existing_pmids,
    get_unsummarized_for_pmids,
    mark_summarized,
    upsert_raw_articles,
)
from pubmed_fetcher import fetch_top_journal_articles_last_days


MODEL_ID = "gemini-2.5-flash"

# Step 1: summarize EN abstract to about 1000 chars.
SUMMARY_TARGET_CHARS = 1000

OUT_DIR = Path("out")
OUT_DIR.mkdir(exist_ok=True)
RAW_DIR = OUT_DIR / "raw_gemini"
RAW_DIR.mkdir(exist_ok=True)

REQUEST_DELAY_SEC = 0.35
MAX_MODEL_RETRIES = 6


TokenStats = dict[str, int]


def zero_tokens() -> TokenStats:
    return {"input": 0, "output": 0, "total": 0}


def add_tokens(acc: TokenStats, delta: TokenStats) -> None:
    acc["input"] += int(delta.get("input", 0) or 0)
    acc["output"] += int(delta.get("output", 0) or 0)
    acc["total"] += int(delta.get("total", 0) or 0)


def build_en_summary_prompt(abstract_en: str) -> str:
    return f"""
Summarize the abstract in English.

Rules:
- Use only the facts in the abstract.
- Plain text only, no markdown, no bullets.
- Target length is about {SUMMARY_TARGET_CHARS} characters.
- Keep it concise but complete (do not end with ellipsis).

Return strictly in this format:
EN_SUMMARY:
...

ABSTRACT (EN):
{abstract_en}
""".strip()


def build_translate_prompt(title_en: str, summary_en: str) -> str:
    return f"""
Translate to Russian:
1) Article title
2) English summary

Rules:
- Preserve meaning and clinical details.
- Translate the full summary completely, without omissions or shortening.
- Plain text only, no markdown, no bullets.

Return strictly in this format:
RU_TITLE: ...
RU_SUMMARY:
...

TITLE (EN): {title_en}

SUMMARY (EN):
{summary_en}
""".strip()


def parse_sections(text: str, keys: tuple[str, ...]) -> dict[str, str | None]:
    out: dict[str, str | None] = {k: None for k in keys}
    if not text:
        return out

    raw = text.replace("\r\n", "\n").strip()
    sections: dict[str, list[list[str]]] = {k: [] for k in keys}
    current_key: str | None = None
    current_block: list[str] = []
    key_set = set(keys)

    def flush_block() -> None:
        nonlocal current_key, current_block
        if current_key is None:
            return
        if current_block:
            sections[current_key].append(current_block)
        current_key = None
        current_block = []

    for line in raw.splitlines():
        m = re.match(r"^([A-Z_]+):\s*(.*)$", line)
        if m and m.group(1) in key_set:
            flush_block()
            current_key = m.group(1)
            current_block = []
            head = m.group(2).strip()
            if head:
                current_block.append(head)
            continue

        if current_key:
            current_block.append(line)

    flush_block()

    for k in keys:
        candidates = []
        for block in sections.get(k, []):
            value = "\n".join(block).strip()
            if value:
                candidates.append(value)
        if candidates:
            best = max(candidates, key=len)
            out[k] = re.sub(r"\n{3,}", "\n\n", best)

    return out


def summary_len(summary: str) -> int:
    return len(summary) if summary else 0


def is_resource_exhausted(err: Exception) -> bool:
    s = repr(err).lower()
    return (
        ("resource_exhausted" in s)
        or ("429" in s)
        or ("quota" in s)
        or ("rate" in s and "limit" in s)
    )


def backoff_sleep(attempt: int) -> None:
    base = min(60, 2**attempt)
    jitter = random.uniform(0, 0.25 * base)
    time.sleep(base + jitter)


def _build_generate_config(max_output_tokens: int) -> Any:
    kwargs: dict[str, Any] = {
        "temperature": 0.2,
        "max_output_tokens": max_output_tokens,
    }

    # Disable thinking when supported to avoid hidden-token truncation and reduce cost.
    thinking_cls = getattr(types, "ThinkingConfig", None)
    if thinking_cls is not None:
        try:
            kwargs["thinking_config"] = thinking_cls(thinking_budget=0)
        except Exception:
            pass

    return types.GenerateContentConfig(**kwargs)


def call_gemini(client: Any, prompt: str, max_output_tokens: int) -> tuple[str, str, TokenStats]:
    resp = client.models.generate_content(
        model=MODEL_ID,
        contents=prompt,
        config=_build_generate_config(max_output_tokens),
    )

    finish_reason = ""
    try:
        if getattr(resp, "candidates", None):
            fr = getattr(resp.candidates[0], "finish_reason", None)
            if fr is not None:
                finish_reason = str(fr)
    except Exception:
        finish_reason = ""

    usage = getattr(resp, "usage_metadata", None)
    input_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
    output_tokens = int(
        (getattr(usage, "candidates_token_count", 0) or 0)
        or (getattr(usage, "output_token_count", 0) or 0)
    )
    total_tokens = int((getattr(usage, "total_token_count", 0) or 0) or (input_tokens + output_tokens))

    return (resp.text or "").strip(), finish_reason, {
        "input": input_tokens,
        "output": output_tokens,
        "total": total_tokens,
    }


def is_incomplete_text(text: str, finish_reason: str) -> bool:
    if not text:
        return True

    tail = text.rstrip()
    if finish_reason and "MAX_TOKENS" in finish_reason.upper():
        return True
    if tail.endswith("..."):
        return True
    if re.search(r"[,;:]$", tail):
        return True
    return False


def summarize_abstract_en(client: Any, pmid: str, abstract_en: str) -> tuple[str | None, TokenStats, str | None]:
    prompt = build_en_summary_prompt(abstract_en)
    last_text = ""
    last_finish_reason = ""
    tokens_used = zero_tokens()

    for attempt in range(MAX_MODEL_RETRIES):
        try:
            last_text, last_finish_reason, call_tokens = call_gemini(client, prompt, max_output_tokens=2600)
            add_tokens(tokens_used, call_tokens)

            parsed = parse_sections(last_text, ("EN_SUMMARY",))
            summary_en = parsed["EN_SUMMARY"]

            if not summary_en:
                prompt = "Format is invalid. Return exactly EN_SUMMARY field.\n\n" + prompt
                continue

            if is_incomplete_text(summary_en, last_finish_reason):
                prompt = (
                    "Summary is incomplete. Return a complete summary with a full ending and no ellipsis.\n\n"
                    + prompt
                )
                continue

            if summary_len(summary_en) < 200:
                prompt = "Summary is too short. Rewrite around 1000 characters.\n\n" + prompt
                continue

            return summary_en, tokens_used, None

        except Exception as e:
            if is_resource_exhausted(e):
                backoff_sleep(attempt)
                continue

            (RAW_DIR / f"{pmid}_summary_exception.txt").write_text(repr(e), encoding="utf-8")
            return None, tokens_used, f"en_exception:{type(e).__name__}"

        time.sleep(0.1)

    (RAW_DIR / f"{pmid}_summary_raw.txt").write_text(
        f"finish_reason={last_finish_reason}\n\n{last_text}",
        encoding="utf-8",
    )
    return None, tokens_used, f"en_incomplete:{last_finish_reason or 'unknown'}"


def translate_summary_ru(
    client: Any,
    pmid: str,
    title_en: str,
    summary_en: str,
) -> tuple[tuple[str, str] | None, TokenStats, str | None]:
    prompt = build_translate_prompt(title_en, summary_en)
    last_text = ""
    last_finish_reason = ""
    tokens_used = zero_tokens()

    for attempt in range(MAX_MODEL_RETRIES):
        try:
            last_text, last_finish_reason, call_tokens = call_gemini(client, prompt, max_output_tokens=3200)
            add_tokens(tokens_used, call_tokens)

            parsed = parse_sections(last_text, ("RU_TITLE", "RU_SUMMARY"))
            ru_title = parsed["RU_TITLE"]
            ru_summary = parsed["RU_SUMMARY"]

            if not ru_title or not ru_summary:
                prompt = "Format is invalid. Return only RU_TITLE and RU_SUMMARY.\n\n" + prompt
                continue

            if is_incomplete_text(ru_summary, last_finish_reason):
                prompt = (
                    "RU summary is incomplete. Translate the entire English summary and end with a full sentence.\n\n"
                    + prompt
                )
                continue

            return (ru_title, ru_summary), tokens_used, None

        except Exception as e:
            if is_resource_exhausted(e):
                backoff_sleep(attempt)
                continue

            (RAW_DIR / f"{pmid}_translate_exception.txt").write_text(repr(e), encoding="utf-8")
            return None, tokens_used, f"ru_exception:{type(e).__name__}"

        time.sleep(0.1)

    (RAW_DIR / f"{pmid}_translate_raw.txt").write_text(
        f"finish_reason={last_finish_reason}\n\n{last_text}",
        encoding="utf-8",
    )
    return None, tokens_used, f"ru_incomplete:{last_finish_reason or 'unknown'}"


def make_telegram_html(
    title_ru: str,
    journal: str,
    date: str,
    authors: list[str],
    summary_ru: str,
    link: str,
) -> str:
    authors_str = ", ".join(authors[:8])
    if len(authors) > 8:
        authors_str += f" (+{len(authors) - 8})"

    return (
        f"<b>{html_escape(title_ru)}</b>\n"
        f"<i>{html_escape(journal or '')}</i> - {html_escape(date or '')}\n"
        f"Авторы: {html_escape(authors_str)}\n"
        f"\n<b>Краткое резюме (по аннотации):</b>\n"
        f"{html_escape(summary_ru)}\n"
        f"\n<a href=\"{html_escape(link)}\">Оригинальная статья</a>"
    )


def summarize_one(
    client: Any,
    row: dict,
) -> tuple[tuple[str, str, str, str] | None, TokenStats, str | None]:
    pmid = row["pmid"]
    title_en = row.get("title_en") or ""
    abstract_en = row.get("abstract_en") or ""
    journal = row.get("journal") or ""
    date = row.get("publication_date") or ""
    authors = json.loads(row.get("authors_json") or "[]")
    link = row.get("link") or row.get("doi_url") or row.get("pubmed_url") or ""

    total_tokens = zero_tokens()

    if not abstract_en.strip():
        return None, total_tokens, "missing_abstract"

    summary_en, en_tokens, en_error = summarize_abstract_en(client, pmid, abstract_en)
    add_tokens(total_tokens, en_tokens)
    if not summary_en:
        return None, total_tokens, en_error or "en_failed"

    translated, ru_tokens, ru_error = translate_summary_ru(client, pmid, title_en, summary_en)
    add_tokens(total_tokens, ru_tokens)
    if not translated:
        return None, total_tokens, ru_error or "ru_failed"

    ru_title, ru_summary = translated
    tg_html = make_telegram_html(ru_title, journal, date, authors, ru_summary, link)
    return (ru_title, summary_en, ru_summary, tg_html), total_tokens, None


def run_pipeline(days_back: int | None = None, limit: int = 200) -> dict:
    load_dotenv()

    fetched_total = 0
    fetched_count = 0
    skipped_existing = 0
    new_pmids: list[str] = []

    try:
        fetched = fetch_top_journal_articles_last_days(days_back)
        fetched_total = len(fetched)

        existing = get_existing_pmids([a.get("pmid") for a in fetched if a.get("pmid")])
        fresh_articles = [a for a in fetched if a.get("pmid") and a.get("pmid") not in existing]
        skipped_existing = fetched_total - len(fresh_articles)

        fetched_count = upsert_raw_articles(fresh_articles)
        new_pmids = [a["pmid"] for a in fresh_articles if a.get("pmid")]
    except Exception as e:
        print(f"[WARN] Fetch failed: {e}", flush=True)

    pending = get_unsummarized_for_pmids(new_pmids, limit=limit)
    pending_count = len(pending)

    if not pending:
        return {
            "fetched": fetched_count,
            "fetched_total": fetched_total,
            "skipped_existing": skipped_existing,
            "pending": 0,
            "summarized": 0,
            "failed": 0,
            "tokens_input": 0,
            "tokens_output": 0,
            "tokens_total": 0,
            "fail_reasons": {},
            "elapsed_sec": 0,
        }

    if genai is None or types is None:
        print("[WARN] google-genai package is missing; cannot summarize pending articles.", flush=True)
        return {
            "fetched": fetched_count,
            "fetched_total": fetched_total,
            "skipped_existing": skipped_existing,
            "pending": pending_count,
            "summarized": 0,
            "failed": pending_count,
            "tokens_input": 0,
            "tokens_output": 0,
            "tokens_total": 0,
            "fail_reasons": {"missing_google_genai": pending_count},
            "elapsed_sec": 0,
        }

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("[WARN] GEMINI_API_KEY is missing; cannot summarize pending articles.", flush=True)
        return {
            "fetched": fetched_count,
            "fetched_total": fetched_total,
            "skipped_existing": skipped_existing,
            "pending": pending_count,
            "summarized": 0,
            "failed": pending_count,
            "tokens_input": 0,
            "tokens_output": 0,
            "tokens_total": 0,
            "fail_reasons": {"missing_gemini_api_key": pending_count},
            "elapsed_sec": 0,
        }

    client = genai.Client(api_key=api_key)

    ok = 0
    failed = 0
    fail_reasons: dict[str, int] = {}
    tokens_total = zero_tokens()

    started_at = time.time()

    for idx, row in enumerate(pending, start=1):
        pmid = row["pmid"]
        print(f"[{idx}/{pending_count}] PMID {pmid} processing...", flush=True)

        res, item_tokens, reason = summarize_one(client, row)
        add_tokens(tokens_total, item_tokens)

        if not res:
            failed += 1
            key = reason or "unknown"
            fail_reasons[key] = fail_reasons.get(key, 0) + 1
            print(
                f"[{idx}/{pending_count}] PMID {pmid} FAILED ({key}) | "
                f"ok={ok} failed={failed} | run_tokens={tokens_total['total']}",
                flush=True,
            )
            continue

        title_ru, summary_en, summary_ru, tg_html = res
        mark_summarized(
            row["pmid"],
            title_ru=title_ru,
            summary_en=summary_en,
            summary_ru=summary_ru,
            tg_message_html=tg_html,
            abstract_ru=None,
        )
        ok += 1

        print(
            f"[{idx}/{pending_count}] PMID {pmid} OK | "
            f"ok={ok} failed={failed} | "
            f"item_tokens={item_tokens['total']} | run_tokens={tokens_total['total']}",
            flush=True,
        )

        time.sleep(REQUEST_DELAY_SEC)

    elapsed_sec = int(time.time() - started_at)

    return {
        "fetched": fetched_count,
        "fetched_total": fetched_total,
        "skipped_existing": skipped_existing,
        "pending": pending_count,
        "summarized": ok,
        "failed": failed,
        "tokens_input": tokens_total["input"],
        "tokens_output": tokens_total["output"],
        "tokens_total": tokens_total["total"],
        "fail_reasons": fail_reasons,
        "elapsed_sec": elapsed_sec,
    }


def main() -> None:
    stats = run_pipeline(limit=200)
    tz = ZoneInfo("Europe/Paris")
    stamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M")

    reasons = stats.get("fail_reasons") or {}
    reasons_str = ", ".join(f"{k}:{v}" for k, v in sorted(reasons.items())) if reasons else "none"

    print(
        f"[{stamp}] Fetched(new/raw): {stats['fetched']}/{stats.get('fetched_total', 0)} | "
        f"SkippedExisting: {stats.get('skipped_existing', 0)} | Pending: {stats['pending']} | "
        f"Summarized: {stats['summarized']} | Failed: {stats['failed']} | "
        f"Tokens(in/out/total): {stats['tokens_input']}/{stats['tokens_output']}/{stats['tokens_total']} | "
        f"Elapsed(s): {stats['elapsed_sec']} | FailReasons: {reasons_str}"
    )


if __name__ == "__main__":
    main()
