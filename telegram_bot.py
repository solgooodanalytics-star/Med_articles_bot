import json
import os
import time
from datetime import datetime, timedelta
from html import escape as html_escape

import requests
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs):
        return False

from db import (
    get_active_subscribers,
    get_article_counts,
    get_state,
    get_summarized_between_dates,
    get_summarized_by_date,
    has_fetch_run,
    init_db,
    is_subscribed,
    mark_delivery,
    mark_fetch_run,
    set_state,
    set_subscription,
    upsert_subscriber,
    was_delivered,
)
from summarize_ru import run_pipeline


BOT_STATE_BACKFILL_DONE = "bootstrap_last7_done"
CALLBACK_SUBSCRIBE = "sub:on"
CALLBACK_UNSUBSCRIBE = "sub:off"
CALLBACK_WEEK = "list:week"


class TelegramAPI:
    def __init__(self, token: str):
        self.base_url = f"https://api.telegram.org/bot{token}"

    def _post(self, method: str, payload: dict) -> dict:
        r = requests.post(f"{self.base_url}/{method}", json=payload, timeout=(5, 60))
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error in {method}: {data}")
        return data

    def get_updates(self, offset: int | None, timeout: int = 25) -> list[dict]:
        payload = {"timeout": timeout, "allowed_updates": ["message", "callback_query"]}
        if offset is not None:
            payload["offset"] = offset
        data = self._post("getUpdates", payload)
        return data.get("result", [])

    def send_message(
        self,
        chat_id: int,
        text: str,
        reply_markup: dict | None = None,
        parse_mode: str = "HTML",
        disable_preview: bool = True,
    ) -> dict:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_preview,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._post("sendMessage", payload)

    def answer_callback(self, callback_query_id: str, text: str | None = None) -> None:
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        self._post("answerCallbackQuery", payload)


def _timezone() -> str:
    return os.getenv("BOT_TIMEZONE", "Europe/Paris")


def _now() -> datetime:
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo(_timezone()))


def _daily_hour_minute() -> tuple[int, int]:
    hour = int(os.getenv("BOT_DAILY_HOUR", "9"))
    minute = int(os.getenv("BOT_DAILY_MINUTE", "0"))
    hour = min(max(hour, 0), 23)
    minute = min(max(minute, 0), 59)
    return hour, minute


def _target_yesterday() -> str:
    d = _now().date() - timedelta(days=1)
    return d.isoformat()


def _keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Подписаться", "callback_data": CALLBACK_SUBSCRIBE},
                {"text": "Отписаться", "callback_data": CALLBACK_UNSUBSCRIBE},
            ],
            [{"text": "Статьи за неделю", "callback_data": CALLBACK_WEEK}],
        ]
    }


def _build_week_lines(articles: list[dict]) -> list[str]:
    lines: list[str] = []
    for a in articles:
        date = a.get("publication_date") or ""
        date = date[:10] if date else ""
        title = html_escape(a.get("title_ru") or a.get("title_en") or "(Без названия)")
        journal = html_escape(a.get("journal") or "")
        link = a.get("link") or a.get("doi_url") or a.get("pubmed_url") or ""
        if link:
            lines.append(f"{date} | <i>{journal}</i> | <a href=\"{html_escape(link)}\">{title}</a>")
        else:
            lines.append(f"{date} | <i>{journal}</i> | {title}")
    return lines


def _send_long_html(api: TelegramAPI, chat_id: int, lines: list[str], header: str) -> None:
    if not lines:
        api.send_message(chat_id, header + "\nСтатей нет.")
        return

    chunk = header + "\n"
    for line in lines:
        candidate = chunk + line + "\n"
        if len(candidate) > 3800:
            api.send_message(chat_id, chunk.strip())
            chunk = header + "\n" + line + "\n"
        else:
            chunk = candidate

    if chunk.strip():
        api.send_message(chat_id, chunk.strip())


def _send_last_week(api: TelegramAPI, chat_id: int) -> None:
    end_date = (_now().date() - timedelta(days=1)).isoformat()
    start_date = (_now().date() - timedelta(days=7)).isoformat()
    articles = get_summarized_between_dates(start_date, end_date)
    lines = _build_week_lines(articles)
    header = f"<b>Статьи за период {start_date} - {end_date}</b>"
    _send_long_html(api, chat_id, lines, header)


def _send_daily_digest(api: TelegramAPI, chat_id: int, target_date: str) -> int:
    articles = get_summarized_by_date(target_date)
    if not articles:
        api.send_message(chat_id, f"За {target_date} нет обработанных статей.")
        return 0

    api.send_message(chat_id, f"<b>Ежедневная подборка за {target_date}</b>\nКоличество: {len(articles)}")
    sent = 0
    for a in articles:
        msg = a.get("tg_message_html")
        if not msg:
            title = html_escape(a.get("title_ru") or a.get("title_en") or "Без названия")
            summary = html_escape(a.get("summary_ru") or a.get("summary_en") or "")
            link = html_escape(a.get("link") or a.get("doi_url") or a.get("pubmed_url") or "")
            msg = f"<b>{title}</b>\n\n{summary}"
            if link:
                msg += f"\n\n<a href=\"{link}\">Открыть статью</a>"

        api.send_message(chat_id, msg)
        sent += 1
        time.sleep(0.15)

    return sent


def _run_bootstrap_if_needed() -> None:
    done = get_state(BOT_STATE_BACKFILL_DONE)
    if done == "1":
        return

    print("[BOOTSTRAP] Fetch + summarize for last 7 days...", flush=True)
    stats = run_pipeline(days_back=7, limit=1000)
    print(
        "[BOOTSTRAP] "
        f"fetched={stats.get('fetched', 0)} summarized={stats.get('summarized', 0)} failed={stats.get('failed', 0)}",
        flush=True,
    )

    today = _now().date()
    for i in range(1, 8):
        mark_fetch_run((today - timedelta(days=i)).isoformat(), mode="bootstrap7", fetched_count=stats.get("fetched", 0))

    set_state(BOT_STATE_BACKFILL_DONE, "1")


def _run_daily_fetch_if_needed() -> None:
    target = _target_yesterday()
    if has_fetch_run(target):
        return

    print(f"[SCHED] Fetch + summarize for {target}...", flush=True)
    stats = run_pipeline(days_back=1, limit=500)
    mark_fetch_run(target, mode="daily1", fetched_count=stats.get("fetched", 0))

    print(
        f"[SCHED] {target} fetched={stats.get('fetched', 0)} summarized={stats.get('summarized', 0)} failed={stats.get('failed', 0)}",
        flush=True,
    )


def _run_daily_send_if_needed(api: TelegramAPI) -> None:
    target = _target_yesterday()
    for chat_id in get_active_subscribers():
        if was_delivered(chat_id, target):
            continue
        sent_count = _send_daily_digest(api, chat_id, target)
        mark_delivery(chat_id, target, sent_count)
        print(f"[SCHED] Delivered {sent_count} articles to chat {chat_id} for {target}", flush=True)


def _maybe_run_scheduled_jobs(api: TelegramAPI) -> None:
    hour, minute = _daily_hour_minute()
    now = _now()

    if (now.hour, now.minute) < (hour, minute):
        return

    _run_daily_fetch_if_needed()
    _run_daily_send_if_needed(api)


def _start_text(chat_id: int) -> str:
    state = "подписан" if is_subscribed(chat_id) else "не подписан"
    hour, minute = _daily_hour_minute()
    return (
        "Бот активен.\n"
        f"Статус: <b>{state}</b>\n"
        "Вы будете получать статьи за предыдущий день один раз в сутки.\n"
        f"Время отправки ({_timezone()}): {hour:02d}:{minute:02d}."
    )


def _status_text() -> str:
    counts = get_article_counts()
    active_subs = len(get_active_subscribers())
    return (
        f"Всего статей: {counts['total']}\n"
        f"Обработано: {counts['summarized']}\n"
        f"В очереди: {counts['pending']}\n"
        f"Активных подписчиков: {active_subs}"
    )


def _handle_start(api: TelegramAPI, chat_id: int, user: dict | None) -> None:
    username = (user or {}).get("username")
    first_name = (user or {}).get("first_name")
    upsert_subscriber(chat_id, is_active=True, username=username, first_name=first_name)
    api.send_message(chat_id, _start_text(chat_id), reply_markup=_keyboard())


def _handle_callback(api: TelegramAPI, callback: dict) -> None:
    qid = callback.get("id")
    data = callback.get("data") or ""
    msg = callback.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")

    if not qid or not chat_id:
        return

    if data == CALLBACK_SUBSCRIBE:
        set_subscription(int(chat_id), True)
        api.answer_callback(qid, "Подписка включена")
        api.send_message(int(chat_id), "Подписка включена.", reply_markup=_keyboard())
        return

    if data == CALLBACK_UNSUBSCRIBE:
        set_subscription(int(chat_id), False)
        api.answer_callback(qid, "Подписка отключена")
        api.send_message(int(chat_id), "Подписка отключена.", reply_markup=_keyboard())
        return

    if data == CALLBACK_WEEK:
        api.answer_callback(qid, "Готовлю список за неделю...")
        _send_last_week(api, int(chat_id))
        return

    api.answer_callback(qid, "Неизвестное действие")


def _handle_message(api: TelegramAPI, msg: dict) -> None:
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return

    text = (msg.get("text") or "").strip()
    user = msg.get("from") or {}

    if text == "/start":
        _handle_start(api, int(chat_id), user)
        return

    if text == "/subscribe":
        set_subscription(int(chat_id), True)
        api.send_message(int(chat_id), "Подписка включена.", reply_markup=_keyboard())
        return

    if text == "/unsubscribe":
        set_subscription(int(chat_id), False)
        api.send_message(int(chat_id), "Подписка отключена.", reply_markup=_keyboard())
        return

    if text in {"/week", "/lastweek", "/неделя"}:
        _send_last_week(api, int(chat_id))
        return

    if text == "/status":
        api.send_message(int(chat_id), _status_text(), reply_markup=_keyboard())
        return

    api.send_message(
        int(chat_id),
        "Используйте /start для открытия меню.\nКоманды: /subscribe, /unsubscribe, /week, /status",
        reply_markup=_keyboard(),
    )


def run_bot() -> None:
    load_dotenv()
    init_db()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing in environment.")

    api = TelegramAPI(token)

    _run_bootstrap_if_needed()

    offset: int | None = None
    print("[BOT] Polling started", flush=True)

    while True:
        try:
            _maybe_run_scheduled_jobs(api)
            updates = api.get_updates(offset=offset, timeout=25)

            for upd in updates:
                uid = upd.get("update_id")
                if uid is not None:
                    offset = int(uid) + 1

                if "callback_query" in upd:
                    _handle_callback(api, upd["callback_query"])
                elif "message" in upd:
                    _handle_message(api, upd["message"])

        except Exception as e:
            print(f"[BOT] Loop error: {e}", flush=True)
            time.sleep(3)


def main() -> None:
    run_bot()


if __name__ == "__main__":
    main()
