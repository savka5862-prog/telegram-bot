"""Complete Finance Bot runtime, menus, and chat-scoped accounting.

The workspace uses the existing bot_database.db when BOT_DB_BACKEND=sqlite.
Selecting a backend never deletes records or copies data between databases.
"""

import asyncio
import csv
import hmac
import io
import json
import logging
import math
import os
import re
import secrets
import signal
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from threading import Thread

from flask import Flask, abort, request
from werkzeug.serving import make_server
from importlib import import_module

# The module is supplied by python-telegram-bot, NOT the unrelated "telegram"
# distribution. Dynamic imports avoid Replit's incorrect dependency inference.
_telegram = import_module("telegram")
_telegram_ext = import_module("telegram.ext")
InlineKeyboardButton = _telegram.InlineKeyboardButton
InlineKeyboardMarkup = _telegram.InlineKeyboardMarkup
KeyboardButton = _telegram.KeyboardButton
ReplyKeyboardMarkup = _telegram.ReplyKeyboardMarkup
Update = _telegram.Update
Application = _telegram_ext.Application
CallbackQueryHandler = _telegram_ext.CallbackQueryHandler
CommandHandler = _telegram_ext.CommandHandler
ContextTypes = _telegram_ext.ContextTypes
ExtBot = _telegram_ext.ExtBot
MessageHandler = _telegram_ext.MessageHandler
filters = _telegram_ext.filters
from finance_storage import connection, polling_lock, uses_postgres

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
# Bot API URLs include the token, so never print HTTP request URLs in workflow logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
DB_FILE = Path(__file__).resolve().with_name("bot_database.db")
PORT = int(os.getenv("PORT", "5000"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_SECRET = secrets.token_urlsafe(32)

app = Flask(__name__)
telegram_application: Application | None = None
telegram_loop: asyncio.AbstractEventLoop | None = None


def bold_html(text, already_html=False):
    """Bold bot text without treating user-supplied text as Telegram markup."""
    body = str(text)
    if already_html:
        body = body.replace("<b>", "").replace("</b>", "")
    else:
        body = escape(body)
    # Telegram code/pre entities cannot overlap bold entities.
    parts = re.split(r"(<code>.*?</code>|<pre>.*?</pre>)", body, flags=re.DOTALL)
    return "".join(
        part if part.startswith(("<code>", "<pre>")) else f"<b>{part}</b>"
        for part in parts if part
    )


class BoldHTMLBot(ExtBot):
    """Apply consistent formatting through public Telegram methods, including replies."""

    @staticmethod
    def formatted(args, kwargs, field, position):
        args, kwargs = list(args), dict(kwargs)
        already_html = kwargs.get("parse_mode") == "HTML"
        if field in kwargs and kwargs[field] is not None:
            kwargs[field] = bold_html(kwargs[field], already_html)
        elif len(args) > position and args[position] is not None:
            args[position] = bold_html(args[position], already_html)
        kwargs["parse_mode"] = "HTML"
        return args, kwargs

    async def send_message(self, *args, **kwargs):
        args, kwargs = self.formatted(args, kwargs, "text", 1)
        return await super().send_message(*args, **kwargs)

    async def edit_message_text(self, *args, **kwargs):
        args, kwargs = self.formatted(args, kwargs, "text", 0)
        return await super().edit_message_text(*args, **kwargs)

    async def send_document(self, *args, **kwargs):
        args, kwargs = self.formatted(args, kwargs, "caption", 2)
        return await super().send_document(*args, **kwargs)


@contextmanager
def db_connection(db_file=None):
    """Commit/roll back transactions and always close their connections."""
    with connection(db_file or DB_FILE) as conn:
        yield conn


def init_db():
    if uses_postgres(DB_FILE):
        # Replit Publish manages production DDL. Never create/alter tables here.
        with db_connection() as conn:
            conn.execute("SELECT id, chat_id, details, deal_number, accounting_period FROM records LIMIT 0")
            conn.execute("SELECT id, chat_id FROM stock_topups LIMIT 0")
            synchronize_deal_numbers(conn)
        return
    with db_connection() as conn:
        lock_numbering(conn)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                user_id INTEGER,
                type TEXT,
                text_summary TEXT,
                amount REAL DEFAULT 0,
                currency TEXT DEFAULT '€',
                payment_type TEXT DEFAULT 'cash',
                item_name TEXT,
                item_qty REAL DEFAULT 0,
                product_qty REAL DEFAULT 0,
                product_unit TEXT DEFAULT '',
                details TEXT DEFAULT '',
                deal_number INTEGER,
                accounting_period TEXT,
                created_at TEXT
            )
            """
        )
        record_columns = {
            row[1] for row in cursor.execute("PRAGMA table_info(records)")
        }
        if "chat_id" not in record_columns:
            # Originating chats were never stored; do not invent them for old rows.
            cursor.execute("ALTER TABLE records ADD COLUMN chat_id INTEGER")
        if "product_qty" not in record_columns:
            cursor.execute("ALTER TABLE records ADD COLUMN product_qty REAL DEFAULT 0")
        if "product_unit" not in record_columns:
            cursor.execute("ALTER TABLE records ADD COLUMN product_unit TEXT DEFAULT ''")
        if "details" not in record_columns:
            cursor.execute("ALTER TABLE records ADD COLUMN details TEXT DEFAULT ''")
        if "deal_number" not in record_columns:
            cursor.execute("ALTER TABLE records ADD COLUMN deal_number INTEGER")
        if "accounting_period" not in record_columns:
            cursor.execute("ALTER TABLE records ADD COLUMN accounting_period TEXT")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deal_counters (
                chat_id INTEGER NOT NULL,
                accounting_period TEXT NOT NULL,
                last_number INTEGER NOT NULL,
                PRIMARY KEY (chat_id, accounting_period)
            )
        """)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_topups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                item_name TEXT,
                qty REAL,
                created_at TEXT
            )
            """
        )
        columns = {
            row[1] for row in cursor.execute("PRAGMA table_info(stock_topups)")
        }
        if "user_id" not in columns:
            cursor.execute("ALTER TABLE stock_topups ADD COLUMN user_id INTEGER")
        if "chat_id" not in columns:
            cursor.execute("ALTER TABLE stock_topups ADD COLUMN chat_id INTEGER")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS records_chat_id_idx "
            "ON records (chat_id, created_at, id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS records_user_id_idx "
            "ON records (user_id, id DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS stock_topups_user_id_idx "
            "ON stock_topups (user_id, id DESC)"
        )
        synchronize_deal_numbers(conn)
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS records_deal_number_idx
            ON records (COALESCE(chat_id, user_id), accounting_period, deal_number)
            WHERE type = 'deal' AND deal_number IS NOT NULL
        """)


def ledger_scope(chat_id):
    """A shared chat ledger, with private-only access to unassigned legacy rows."""
    if not isinstance(chat_id, int) or chat_id == 0:
        raise ValueError("A valid Telegram chat ID is required.")
    if chat_id > 0:
        return "(chat_id = ? OR (chat_id IS NULL AND user_id = ?))", (chat_id, chat_id)
    return "chat_id = ?", (chat_id,)


MONTHS_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}
PRODUCT_UNITS = (
    r"(?:грамм(?:а|ов)?|гр|г|g)\s+слабый|слабый|"
    r"грамм(?:а|ов)?|гр|г|g|штук(?:а|и)?|шт\.?|хтс|xtc"
)
PRODUCT_RE = re.compile(
    rf"^\s*(?:\+\s*|пополнить\s+)?(\d+(?:[.,]\d+)?)\s*"
    rf"({PRODUCT_UNITS})?(?=$|\s|[.,;:=+])", re.IGNORECASE,
)
DEAL_PREFIX_RE = re.compile(
    rf"^\s*\d+(?:[.,]\d+)?\s*(?:{PRODUCT_UNITS})?"
    rf"(?:\s*(?:\bза\b|=)\s*|\s+(?=\d))", re.IGNORECASE,
)


def format_ru_date(dt):
    return f"{dt.day} {MONTHS_RU[dt.month]} {dt.year} г."

def get_accounting_period_start(dt: datetime) -> datetime:
    """Most recent daily boundary: weekdays at 02:00, weekends at 03:00."""
    boundary = dt.replace(
        hour=3 if dt.weekday() >= 5 else 2, minute=0, second=0, microsecond=0,
    )
    if dt < boundary:
        previous = dt - timedelta(days=1)
        boundary = previous.replace(
            hour=3 if previous.weekday() >= 5 else 2,
            minute=0, second=0, microsecond=0,
        )
    return boundary


def accounting_period_key(created_at):
    try:
        return get_accounting_period_start(
            datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S")
        ).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        # Preserve legacy records without pretending an unknown timestamp is today.
        return "unknown"


def lock_numbering(conn):
    """Serialize numbering and the accompanying record write in one transaction."""
    if getattr(conn, "is_postgres", False):
        conn.execute("SELECT pg_advisory_xact_lock(718941204)")
    elif not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")


def next_deal_number(conn, chat_id, period):
    scope, params = ledger_scope(chat_id)
    highest = conn.execute(
        f"SELECT COALESCE(MAX(deal_number), 0) FROM records "
        f"WHERE {scope} AND type = 'deal' AND accounting_period = ?",
        (*params, period),
    ).fetchone()[0]
    counter = conn.execute(
        "SELECT last_number FROM deal_counters WHERE chat_id = ? AND accounting_period = ?",
        (chat_id, period),
    ).fetchone()
    return max(highest, counter[0] if counter else 0) + 1


def allocate_deal_number(conn, chat_id, period):
    number = next_deal_number(conn, chat_id, period)
    conn.execute(
        """INSERT INTO deal_counters (chat_id, accounting_period, last_number)
           VALUES (?, ?, ?) ON CONFLICT (chat_id, accounting_period)
           DO UPDATE SET last_number = excluded.last_number""",
        (chat_id, period, number),
    )
    return number


def synchronize_deal_numbers(conn):
    """Backfill old deals once, preserving saved numbers and all financial data."""
    lock_numbering(conn)
    rows = conn.execute(
        """SELECT id, chat_id, user_id, created_at, deal_number, accounting_period
           FROM records WHERE type = 'deal' ORDER BY created_at, id"""
    ).fetchall()
    for record_id, chat, user, created_at, number, period in rows:
        # Unassigned legacy records are private-only; never invent group ownership.
        owner = chat if chat is not None else user if user and user > 0 else None
        if not owner:
            continue
        period = period or accounting_period_key(created_at)
        if number is None:
            number = allocate_deal_number(conn, owner, period)
            conn.execute(
                "UPDATE records SET deal_number = ?, accounting_period = ? WHERE id = ?",
                (number, period, record_id),
            )
        else:
            conn.execute(
                """INSERT INTO deal_counters (chat_id, accounting_period, last_number)
                   VALUES (?, ?, ?) ON CONFLICT (chat_id, accounting_period)
                   DO UPDATE SET last_number = CASE
                       WHEN deal_counters.last_number < excluded.last_number
                       THEN excluded.last_number ELSE deal_counters.last_number END""",
                (owner, period, number),
            )


def get_deal_number(chat_id, created_at_str, record_id=None):
    """Read the stored entry number, never renumber saved deals after deletion."""
    scope, params = ledger_scope(chat_id)
    with db_connection() as conn:
        if record_id is not None:
            row = conn.execute(
                f"SELECT deal_number FROM records WHERE id = ? AND {scope} AND type = 'deal'",
                (record_id, *params),
            ).fetchone()
            return row[0] if row else 0
        return next_deal_number(conn, chat_id, accounting_period_key(created_at_str)) - 1


def format_quantity(value):
    return f"{value:g}"


def product_label(unit):
    if unit == "шт":
        return "ХТС"
    if unit == "слабый":
        return "Слабый"
    return "Товар"


def product_measure(unit):
    return "шт" if unit == "шт" else "г"


def record_edit_title(chat_id, record_id):
    """Display the saved deal number, never the internal edit identifier."""
    with db_connection() as conn:
        scope, params = ledger_scope(chat_id)
        row = conn.execute(
            f"SELECT type, deal_number FROM records WHERE id = ? AND {scope}",
            (record_id, *params),
        ).fetchone()
    suffix = f" #{row[1]}" if row and row[0] == "deal" and row[1] is not None else ""
    return f"Изменение записи{suffix}"


def get_preview_deal_number(chat_id, now, editing_record_id=None):
    """Preview the next ordinal, or the existing position for a record edit."""
    if editing_record_id is not None:
        scope, params = ledger_scope(chat_id)
        with db_connection() as conn:
            row = conn.execute(
                f"SELECT type, created_at FROM records WHERE id = ? AND {scope}",
                (editing_record_id, *params),
            ).fetchone()
        if row:
            return get_deal_number(
                chat_id, row[1], editing_record_id if row[0] == "deal" else None,
            ) + (row[0] != "deal")
    return get_deal_number(chat_id, now.strftime("%Y-%m-%d %H:%M:%S")) + 1


def parse_product(text, default_unit=""):
    match = PRODUCT_RE.match(text)
    if not match:
        return 0.0, ""
    raw_unit = (match.group(2) or "").lower()
    remainder = text[match.end():].strip().lower()
    if not raw_unit and re.match(
        r"^(€|\$|eur\b|евро\b|usd\b|грн\b|uah\b)", remainder, re.IGNORECASE,
    ):
        return 0.0, ""
    weak_marker = re.search(r"\bслабый\b", text, re.IGNORECASE)
    if raw_unit or weak_marker:
        if "слабый" in raw_unit or weak_marker:
            unit = "слабый"
        else:
            unit = "шт" if raw_unit.startswith(("шт", "хтс", "xtc")) else "г"
    else:
        unit = default_unit
    return (float(match.group(1).replace(",", ".")), unit) if unit else (0.0, "")


def decode_details(details):
    if not details:
        return {}
    result = json.loads(details) if isinstance(details, str) else details
    if not isinstance(result, dict):
        raise ValueError("Неверные данные записи.")
    return result


def record_products(qty, unit, details=""):
    products = decode_details(details).get("products")
    return products if products is not None else ({unit: qty or 0.0} if unit else {})


def record_payments(kind, payment, amount, details=""):
    payments = decode_details(details).get("payments")
    if payments is not None:
        return payments
    channel = (
        "card" if kind in {"take_card", "direct_expense_card"}
        else "cash" if kind in {"take_cash", "cash_take"}
        else payment or "cash"
    )
    return {channel: amount or 0.0}


def get_stock(db_file, chat_id):
    quantities = {"г": 0.0, "слабый": 0.0, "шт": 0.0}
    sold = {"г": 0.0, "слабый": 0.0, "шт": 0.0}
    with db_connection(db_file) as conn:
        scope, params = ledger_scope(chat_id)
        rows = conn.execute(
            f"""SELECT type, product_qty, product_unit, details FROM records
               WHERE {scope} AND type IN
                   ('topup_product','deal','self_g','self_sht','self_use','purchase',
                    'direct_expense_product')
               ORDER BY created_at, id""", params,
        ).fetchall()
    for kind, qty, unit, details in rows:
        for unit, qty in record_products(qty, unit, details).items():
            if unit not in quantities:
                continue
            if kind == "topup_product":
                quantities[unit] += qty
                sold[unit] = 0.0
            else:
                quantities[unit] -= qty
                if kind == "deal":
                    sold[unit] += qty
    return quantities, sold


def money_category(kind, summary=""):
    if kind in {"direct_expense_cash", "direct_expense_card"}:
        return "expense"
    if kind in {"cash_take", "take_cash", "take_card"}:
        return "taken"
    if kind == "expense" and (summary or "").startswith("Снятие"):
        return "taken"
    return kind


def get_money_totals(db_file, chat_id):
    totals = {"topup": {}, "expense": {}, "advance": {}, "purchase": {}, "taken": {}}
    with db_connection(db_file) as conn:
        scope, params = ledger_scope(chat_id)
        rows = conn.execute(
            f"SELECT type, text_summary, amount, currency FROM records WHERE {scope}",
            params,
        ).fetchall()
    for kind, summary, amount, currency in rows:
        category = money_category(kind, summary)
        if category in totals:
            currency = currency or "€"
            totals[category][currency] = totals[category].get(currency, 0.0) + (amount or 0.0)
    return totals


def get_main_reply_keyboard():
    keyboard = [
        [KeyboardButton("🛒 Сделка"), KeyboardButton("📦 Остаток и касса")],
        [KeyboardButton("➕ Пополнение товара"), KeyboardButton("📋 История сделок")],
        [KeyboardButton("💵 Забрать кассу"), KeyboardButton("📊 Итог за период")],
        [KeyboardButton("✏ Изменить записи"), KeyboardButton("📖 Как записывать")],
        [KeyboardButton("📁 Резервная копия"), KeyboardButton("🔄 Рестарт учёта")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


MENU_SECTIONS = {
    "menu_deal": "🛒 Сделка",
    "menu_product_topup": "➕ Пополнение товара",
    "menu_balance": "📦 Остаток и касса",
    "menu_history": "📋 История сделок",
    "menu_cash": "💵 Забрать кассу",
    "menu_stats": "📊 Итог за период",
    "menu_edit": "✏ Изменить записи",
    "menu_help": "📖 Как записывать",
    "menu_backup": "📁 Резервная копия",
    "menu_restart": "🔄 Рестарт учёта",
}
MENU_ALIASES = {
    "🤝 Сделка": "🛒 Сделка",
    "📋 История записей": "📋 История сделок",
    "✏️ Изменить записи": "✏ Изменить записи",
}


def get_main_inline_keyboard():
    sections = list(MENU_SECTIONS.items())
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(label, callback_data=key)
            for key, label in sections[index:index + 2]
        ]
        for index in range(0, len(sections), 2)
    ])


def back_to_main_keyboard():
    return None


def clear_input_state(context):
    clear_input_data(context.chat_data)


def clear_input_data(user_data):
    for key in ("pending_action", "editing_record_id", "pending_expense",
                "waiting_for_take_cash_amount", "cash_withdraw_chat_id", "input_chat_id"):
        user_data.pop(key, None)


def invalidate_chat_inputs(context, chat_id):
    """Invalidate all members' pending records in the reset chat, and no others."""
    application = getattr(context, "application", None)
    users = [context.chat_data, *getattr(application, "chat_data", {}).values(),
             context.user_data, *getattr(application, "user_data", {}).values()]
    for user_data in users:
        records = user_data.get("pending_records", {})
        for key, pending in list(records.items()):
            if pending["chat_id"] == chat_id:
                records.pop(key)
        if not records:
            user_data.pop("pending_records", None)
        if (user_data.get("input_chat_id") == chat_id
                or user_data.get("cash_withdraw_chat_id") == chat_id
                or user_data.get("pending_expense", {}).get("chat_id") == chat_id):
            clear_input_data(user_data)


def format_saved_record(pending, record_id):
    text = pending["text_summary"]
    amount = pending["amount"]
    currency = pending["currency"]
    pay_icon = payment_icon(pending["payment_type"])
    if pending["record_type"] == "topup_product":
        return f"✅ Пополнение товара успешно подтверждено:\n{text[:3200]}"
    if pending["record_type"] == "topup":
        return f"✅ Касса успешно пополнена:\n{text[:3000]} (+{format_quantity(amount)} {currency} {pay_icon})"
    if pending["record_type"] in {"expense", "advance"}:
        label = "Аванс" if pending["record_type"] == "advance" else "Расход"
        return f"✅ {label} успешно подтвержден:\n{text[:3000]} (–{format_quantity(amount)} {currency} {pay_icon})"
    if pending["record_type"] in {"self_use", "purchase", "direct_expense_product",
                                 "direct_expense_cash", "direct_expense_card"}:
        return f"✅ Списание успешно подтверждено:\n{text[:3200]}"
    displayed_amount = int(amount) if amount.is_integer() else amount
    with db_connection() as conn:
        row = conn.execute("SELECT created_at FROM records WHERE id = ?", (record_id,)).fetchone()
    deal_number = get_deal_number(pending["chat_id"], row[0], record_id)
    _, note = split_record_note(text, "deal")
    return (
        f"✅ Сделка №{deal_number}\n"
        f"Товар: {format_quantity(pending['product_qty'])} "
        f"{product_measure(pending['product_unit'])} · {product_label(pending['product_unit'])}\n"
        f"Получено: {displayed_amount} {currency} {pay_icon}\n"
        f"{format_payment_parts(pending)}"
        + (f"\n└, {note[:1200]}" if note else "")
    )


def payment_icon(payment):
    return "💵/💳" if payment == "mixed" else "💳" if payment == "card" else "💵"


def format_payment_parts(pending):
    payments = record_payments(
        pending["record_type"], pending["payment_type"], pending["amount"],
        pending.get("details", ""),
    )
    return " · ".join(
        f"{payment_icon(channel)} {format_quantity(amount)} {pending['currency']}"
        for channel, amount in payments.items()
    )


def format_writeoff(record, *, history=False):
    products = record_products(
        record["product_qty"], record["product_unit"], record.get("details", ""),
    )
    product_text = " · ".join(
        f"{format_quantity(qty)} {product_measure(unit)} · {product_label(unit)}"
        for unit, qty in products.items()
    ) or "количество не указано"
    short_products = " · ".join(
        f"{format_quantity(qty)} {product_measure(unit)}" for unit, qty in products.items()
    ) or "количество не указано"
    short_payments = " / ".join(
        f"{format_quantity(amount)} {record.get('currency') or '€'} {payment_icon(channel)}"
        for channel, amount in record_payments(
            record["record_type"], record["payment_type"], record["amount"],
            record.get("details", ""),
        ).items()
    )
    if record["record_type"] == "direct_expense_product":
        if history:
            return f"➖ Списание · {short_products}"
        return f"➖ Списание: {product_text}"
    if record["record_type"] in {"direct_expense_cash", "direct_expense_card"}:
        if history:
            return f"➖ Списание · {short_payments}"
        return f"➖ Списание с кассы: {format_payment_parts(record)}"
    if record["record_type"] == "self_use":
        return f"👤 Списано себе: {product_text}"
    details = decode_details(record.get("details", ""))
    item = details.get("item_description") or "\n".join(
        line.strip() for line in record["text_summary"].splitlines()[2:] if line.strip()
    ) or "описание не указано"
    if history:
        note = f"\n└, {item[:2300]}" if item != "описание не указано" else ""
        return f"🛒 Покупка · {short_products} · {short_payments}{note}"
    return (
        f"📄 Списание за покупку\nТовар: {product_text}\n"
        f"Из кассы: {format_payment_parts(record)} · {item[:2300]}"
    )


def split_record_note(text, kind):
    """Separate a note from accounting input without interpreting its contents."""
    if kind not in {"deal", "expense", "advance"}:
        return text, ""
    accounting, separator, note = text.partition("\n")
    accounting = accounting.strip()
    note = note.strip() if separator else ""
    if kind == "deal":
        prefix = DEAL_PREFIX_RE.match(accounting)
        if prefix:
            # Consume only the accounting prefix. Everything after the first
            # free-text word is a note, even if it contains more numbers.
            token = re.compile(
                rf"\s*(?:[+-]?\d+(?:[.,]\d+)?|[€$/]|\bслабый\b|"
                rf"(?:eur|евро|usd|грн|uah)(?!\w)|на\s+карту\b|"
                rf"{PAYMENT_MARKER})", re.IGNORECASE,
            )
            end = prefix.end()
            while match := token.match(accounting, end):
                end = match.end()
            if end > prefix.end():
                inline_note = accounting[end:].strip()
                accounting = accounting[:end].strip()
                note = "\n".join(part for part in (inline_note, note) if part)
    return accounting, note


async def create_record_preview(text, chat_id, context, editing_record_id=None):
    now = datetime.now()
    records = context.chat_data.setdefault("pending_records", {})
    for key, record in list(records.items()):
        if now - record["created_at"] >= timedelta(minutes=30):
            records.pop(key)
    # Keep multiple previews independent without retaining unbounded message text.
    while len(records) >= 20:
        records.pop(next(iter(records)))
    key = secrets.token_urlsafe(8)
    original_text = text
    first_type = get_record_type(text.partition("\n")[0].strip())
    record_type = first_type if first_type in {"deal", "expense", "advance"} else get_record_type(text)
    text, note = split_record_note(text, record_type)
    parsed = parse_amount(text) if record_type not in {
        "topup_product", "self_use", "purchase", "direct_expense_product",
    } else None
    if record_type == "expense" and parsed is None and is_expense_keyword(text):
        context.chat_data["pending_action"] = "expense_amount"
        context.chat_data["pending_expense"] = {
            "text": original_text, "chat_id": chat_id, "editing_record_id": editing_record_id,
        }
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"➖ Расход: {text[:2000]}\nВведите сумму, например 100 или 100 €.",
            reply_markup=back_to_main_keyboard(),
        )
        return False
    amount, currency = parsed if parsed else (0.0, "€")
    product_qty, product_unit = (
        parse_product(text, default_unit="г" if record_type == "deal" else "")
        if record_type in {"topup_product", "deal"}
        else (0.0, "")
    )
    payment_type = get_payment_type(text)
    details = {"note": note} if note else {}
    try:
        if record_type == "topup_product":
            products = parse_product_topups(text)
            product_unit, product_qty = next(iter(products.items()))
            if len(products) > 1:
                details["products"] = products
        elif record_type == "self_use":
            product_qty, product_unit = parse_self_use(text)
        elif record_type == "direct_expense_product":
            product = re.fullmatch(
                rf"-\s*(\d+(?:[.,]\d+)?)\s*({PRODUCT_UNITS})\s*", text, re.IGNORECASE,
            )
            if not product:
                raise ValueError("Списание товара: -25г или -10 хтс.")
            product_qty, product_unit = parse_product(f"{product.group(1)} {product.group(2)}")
            if product_qty <= 0:
                raise ValueError("Укажите положительное количество товара.")
        elif record_type == "purchase":
            product_qty, product_unit, amount, currency, item = parse_purchase(text)
            details["item_description"] = item
            if parse_payment_types(text) == "both":
                raise ValueError("Укажите один способ оплаты покупки: наличные или карта.")
        elif record_type in {"direct_expense_cash", "direct_expense_card"}:
            money = re.fullmatch(
                rf"-\s*(\d+(?:[.,]\d{{1,2}})?)\s*(€|eur|евро|\$|usd|грн|uah)?"
                rf"\s*({PAYMENT_MARKER})?\s*", text, re.IGNORECASE,
            )
            if not money:
                raise ValueError("Списание денег: -200€ или -200€ 💳.")
            amount = float(money.group(1).replace(",", "."))
            currency = CURRENCY_NAMES.get((money.group(2) or "€").lower(), "€")
        elif record_type == "deal":
            cash_amount, card_amount = parse_deal_amounts(text, amount)
            payments = {
                channel: value for channel, value in
                (("cash", cash_amount), ("card", card_amount)) if value > 0
            }
            amount = sum(payments.values())
            if payments:
                payment_type = "mixed" if len(payments) > 1 else next(iter(payments))
            if len(payments) > 1 or "/" in text:
                details["payments"] = payments
        elif parse_payment_types(text) == "both":
            raise ValueError("Укажите один способ оплаты для пополнения, расхода или аванса.")
        if record_type not in {"topup_product", "self_use", "direct_expense_product"} and (not math.isfinite(amount) or amount <= 0):
            raise ValueError("Укажите положительную сумму. Например: 2г за 50.")
        if not math.isfinite(product_qty) or product_qty < 0 or (product_unit and product_qty == 0):
            raise ValueError("Количество должно быть конечным положительным числом.")
    except ValueError as error:
        await context.bot.send_message(
            chat_id=chat_id, text=f"⚠️ {error}", reply_markup=back_to_main_keyboard(),
        )
        return False
    pending = {
        "record_type": record_type,
        "text_summary": original_text,
        "amount": amount,
        "currency": currency,
        "payment_type": payment_type,
        "chat_id": chat_id,
        "created_at": now,
        "editing_record_id": editing_record_id,
        "product_qty": product_qty,
        "product_unit": product_unit,
        "details": json.dumps(details, ensure_ascii=False) if details else "",
    }
    records[key] = pending
    type_name = {
        "topup_product": "➕ Пополнение товара",
        "topup": "➕ Пополнение кассы",
        "expense": "➖ Расход",
        "advance": "👤 Аванс",
        "deal": "🛒 Сделка",
        "self_use": "👤 Списание себе",
        "purchase": "📄 Списание",
        "direct_expense_product": "➖ Списание товара",
        "direct_expense_cash": "➖ Списание с кассы",
        "direct_expense_card": "➖ Списание с карты",
    }[pending["record_type"]]
    edit_notice = f"<b>✏ {record_edit_title(chat_id, editing_record_id)}</b>\n" if editing_record_id is not None else ""
    value_line = (
        "<b>Количество:</b> " + " · ".join(
            f"{format_quantity(qty)} {product_measure(unit)} · {product_label(unit)}"
            for unit, qty in record_products(product_qty, product_unit, details).items()
        )
        if record_type == "topup_product"
        else f"<b>Сумма:</b> {escape(format_payment_parts(pending))}"
    )
    preview_text = (
        f"{edit_notice}"
        f"<b>{type_name}:</b> {escape(text[:3000])}\n"
        f"{value_line}"
    )
    if record_type in {"self_use", "purchase", "direct_expense_product",
                       "direct_expense_cash", "direct_expense_card"}:
        preview_text = edit_notice + escape(format_writeoff(pending))
    elif record_type == "deal":
        deal_number = get_preview_deal_number(chat_id, now, editing_record_id)
        product_text = " · ".join(
            f"{format_quantity(qty)} {product_measure(unit)} · {product_label(unit)}"
            for unit, qty in record_products(product_qty, product_unit, details).items()
        )
        preview_text = (
            edit_notice + f"<b>🛒 Сделка №{deal_number}</b>\n"
            f"Товар: {escape(product_text)}\nПолучено: {escape(format_payment_parts(pending))}"
        )
    if note:
        preview_text += f"\n<b>└, {escape(note[:1200])}</b>"
    try:
        sent_preview = await context.bot.send_message(
            chat_id=chat_id,
            text=preview_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{key}")],
                [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{key}")],
            ]),
        )
        if isinstance(getattr(sent_preview, "message_id", None), int):
            pending["message_id"] = sent_preview.message_id
    except Exception:
        records.pop(key, None)
        raise
    return True


CARD_MARKER = r"(?:💳|🪪|карт\w*|card)"
CASH_MARKER = r"(?:💵|нал\w*|готівк\w*|гот|cash)"
PAYMENT_MARKER = rf"(?:{CARD_MARKER}|{CASH_MARKER})"


def parse_payment_types(text: str):
    lowered = text.lower()
    has_card = bool(re.search(CARD_MARKER, lowered))
    has_cash = bool(re.search(CASH_MARKER, lowered))
    return "both" if has_card and has_cash else "card" if has_card else "cash"


def get_payment_type(text: str) -> str:
    return "card" if parse_payment_types(text) == "card" else "cash"


def parse_deal_amounts(text: str, total_amount: float):
    """Validate marked payments, including a stated total with partial card payment."""
    if not math.isfinite(total_amount) or total_amount <= 0:
        raise ValueError("Укажите положительную сумму сделки.")
    parsed = parse_amount(text)
    currency = parsed[1] if parsed else "€"
    if "/" in text:
        payments, _ = parse_mixed_payment(text, currency)
        return payments.get("cash", 0.0), payments.get("card", 0.0)
    price_parts = re.split(r"\bза\b|=", text, maxsplit=1, flags=re.IGNORECASE)
    prefix_match = DEAL_PREFIX_RE.match(text)
    price_text = (
        text[prefix_match.end():] if prefix_match else price_parts[-1]
    ).strip().lower()
    if re.search(r"-\s*\d", price_text):
        raise ValueError("Суммы оплаты должны быть положительными.")
    matches = list(re.finditer(
        rf"(?<![\w.,+-])(\d+(?:[.,]\d{{1,2}})?)\s*"
        rf"(€|\$|eur|евро|usd|грн|uah)?\s*({PAYMENT_MARKER})(?!\w)",
        price_text,
    ))
    if not matches:
        payment = parse_payment_types(text)
        if payment == "both":
            raise ValueError("Укажите сумму наличными и сумму картой.")
        return (0.0, total_amount) if payment == "card" else (total_amount, 0.0)
    assigned_markers = {match.span(3) for match in matches}
    if any(
        marker.span() not in assigned_markers
        for marker in re.finditer(PAYMENT_MARKER, price_text)
    ):
        raise ValueError("Укажите отдельную сумму для каждого способа оплаты.")
    payments = {"cash": 0.0, "card": 0.0}
    for match in matches:
        amount = float(match.group(1).replace(",", "."))
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Каждая сумма оплаты должна быть больше нуля.")
        if match.group(2) and CURRENCY_NAMES[match.group(2)] != currency:
            raise ValueError("Обе части одной сделки должны быть в одной валюте.")
        channel = "card" if re.fullmatch(CARD_MARKER, match.group(3)) else "cash"
        payments[channel] += amount
    allocated = sum(payments.values())
    if not math.isfinite(allocated):
        raise ValueError("Сумма слишком большая.")
    prefix = price_text[:matches[0].start()].strip()
    explicit_total = bool(re.fullmatch(
        r"\d+(?:[.,]\d{1,2})?\s*(?:€|\$|eur|евро|usd|грн|uah)?", prefix,
    ))
    if explicit_total:
        if allocated > total_amount + 0.0001:
            raise ValueError("Суммы оплаты превышают общую сумму сделки.")
        if payments["cash"] and payments["card"]:
            if not math.isclose(allocated, total_amount, abs_tol=0.0001):
                raise ValueError("Суммы наличными и картой должны совпадать с общей суммой.")
        else:
            other_channel = "cash" if payments["card"] else "card"
            payments[other_channel] = max(0.0, total_amount - allocated)
    elif allocated > total_amount + 0.0001 and len(matches) == 1:
        raise ValueError("Сумма оплаты превышает общую сумму сделки.")
    return payments["cash"], payments["card"]


EXPENSE_KEYWORDS = ("топка", "бенз", "бензин", "расходник", "хавка", "вода", "тачка")


def is_expense_keyword(text: str) -> bool:
    return any(keyword in text.lower() for keyword in EXPENSE_KEYWORDS)


def get_record_type(text: str) -> str:
    lowered = text.strip().lower()
    if DEAL_PREFIX_RE.match(lowered):
        return "deal"
    lines = [line.strip() for line in lowered.splitlines() if line.strip()]
    negative_product = re.match(
        rf"^-\s*\d+(?:[.,]\d+)?\s*(?:{PRODUCT_UNITS})(?=$|\s|[.,])", lowered,
    )
    if negative_product:
        return "direct_expense_product" if not lowered[negative_product.end():].strip() else "purchase"
    if len(lines) >= 3 and any("-" in line for line in lines):
        return "purchase"
    if re.search(r"\b(?:себе|себес)\b", lowered):
        return "self_use"
    if lowered.startswith(("+", "пополнить")):
        if parse_product(text)[1]:
            return "topup_product"
        return "topup"
    if lowered.startswith("-") and "аванс" not in lowered:
        if is_expense_keyword(lowered):
            return "expense"
        return "direct_expense_card" if get_payment_type(text) == "card" else "direct_expense_cash"
    if lowered.startswith(("-", "расход", "аванс")) or is_expense_keyword(lowered):
        return "advance" if "аванс" in lowered else "expense"
    return "deal"


def parse_self_use(text):
    match = re.fullmatch(
        rf"\s*(\d+(?:[.,]\d+)?)\s*({PRODUCT_UNITS})?\s*(?:себе|себес)\s*",
        text, re.IGNORECASE,
    )
    if not match:
        raise ValueError("Для себя: 0,5 себе или 10 шт себе. Укажите положительное количество.")
    qty, unit = parse_product(f"{match.group(1)} {match.group(2) or 'г'}")
    if not math.isfinite(qty) or qty <= 0:
        raise ValueError("Количество для себя должно быть конечным положительным числом.")
    return qty, unit


def parse_purchase(text):
    match = re.fullmatch(
        rf"\s*-\s*(\d+(?:[.,]\d+)?)\s*({PRODUCT_UNITS})\s+"
        rf"-\s*(\d+(?:[.,]\d{{1,2}})?)\s*(€|eur|евро|\$|usd|грн|uah)?"
        rf"[ \t]*({PAYMENT_MARKER})?(?:\s+(.+))?\s*",
        text, re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError("Покупка: -10xtc -250€ Ray ban (в строку или в столбик).")
    qty, unit = parse_product(f"{match.group(1)} {match.group(2)}")
    amount = float(match.group(3).replace(",", "."))
    currency = CURRENCY_NAMES.get((match.group(4) or "€").lower(), "€")
    if not math.isfinite(qty) or qty <= 0 or not math.isfinite(amount) or amount <= 0:
        raise ValueError("Количество и сумма покупки должны быть конечными положительными числами.")
    item = (match.group(6) or "товар").strip()
    # A malformed money line must not become an item's description.
    lines = text.strip().splitlines()
    product_only = re.fullmatch(
        rf"-\s*\d+(?:[.,]\d+)?\s*({PRODUCT_UNITS})\s*", lines[0], re.IGNORECASE,
    )
    money_line = lines[1] if product_only and len(lines) > 1 else ""
    if money_line and not re.fullmatch(
        rf"-\s*\d+(?:[.,]\d{{1,2}})?\s*(?:€|eur|евро|\$|usd|грн|uah)?"
        rf"\s*(?:{PAYMENT_MARKER})?\s*", money_line.strip(), re.IGNORECASE,
    ):
        raise ValueError("Вторая строка покупки должна содержать только сумму и способ оплаты.")
    return qty, unit, amount, currency, item


MONEY_RE = re.compile(
    r"(?<![\w])(\d+(?:[.,]\d{1,2})?)\s*(€|eur|евро|\$|usd|грн|uah)",
    re.IGNORECASE,
)
CURRENCY_NAMES = {
    "€": "€",
    "eur": "€",
    "евро": "€",
    "$": "$",
    "usd": "$",
    "грн": "грн",
    "uah": "грн",
}


def parse_amount(text: str, allow_plain_number: bool = False):
    if not allow_plain_number and get_record_type(text) == "topup_product":
        return None
    # A sale's price follows "за" or "="; its first number may be quantity.
    if not allow_plain_number:
        prefix = DEAL_PREFIX_RE.match(text)
        if prefix:
            price = re.match(
                r"(\d+(?:[.,]\d+)?)\s*(€|eur|евро|\$|usd|грн|uah)?",
                text[prefix.end():], re.IGNORECASE,
            )
            if price:
                return (
                    float(price.group(1).replace(",", ".")),
                    CURRENCY_NAMES.get((price.group(2) or "€").lower(), "€"),
                )
        price = re.search(
            r"(?:\bза\b|=)\s*(\d+(?:[.,]\d+)?)\s*(€|eur|евро|\$|usd|грн|uah)?",
            text,
            re.IGNORECASE,
        )
        if get_record_type(text) == "deal" and price:
            amount = float(price.group(1).replace(",", "."))
            currency = CURRENCY_NAMES.get((price.group(2) or "€").lower(), "€")
            return amount, currency
    match = MONEY_RE.search(text)
    if match:
        amount = float(match.group(1).replace(",", "."))
        currency = CURRENCY_NAMES[match.group(2).lower()]
        return amount, currency

    if allow_plain_number:
        match = re.fullmatch(
            r"\s*(\d+(?:[.,]\d{1,2})?)\s*(€|eur|евро|\$|usd|грн|uah)?\s*",
            text,
            re.IGNORECASE,
        )
        if match:
            amount = float(match.group(1).replace(",", "."))
            currency = CURRENCY_NAMES.get((match.group(2) or "€").lower(), "€")
            return amount, currency
    else:
        match = re.search(r"\d+(?:[.,]\d+)?", text)
        if match:
            return float(match.group().replace(",", ".")), "€"
    return None


def parse_product_topups(text):
    """Parse the whole input so unsupported trailing stock isn't silently lost."""
    text = re.sub(r"^\s*пополнить\s+", "", text, flags=re.IGNORECASE)
    chunks = re.split(r"\s*(?=\+\s*\d)", text.strip())
    products = {}
    for chunk in filter(None, chunks):
        match = re.fullmatch(
            rf"\+?\s*(\d+(?:[.,]\d+)?)\s*({PRODUCT_UNITS})\s*",
            chunk, re.IGNORECASE,
        )
        if not match:
            raise ValueError("Пополнение товара: +100г +90 хтс. Укажите единицу каждого количества.")
        qty, unit = parse_product(chunk)
        if not math.isfinite(qty) or qty <= 0:
            raise ValueError("Количество пополнения должно быть больше нуля.")
        products[unit] = products.get(unit, 0.0) + qty
        if not math.isfinite(products[unit]):
            raise ValueError("Количество слишком большое.")
    if not products:
        raise ValueError("Укажите количество товара.")
    return products


def parse_mixed_payment(text, default_currency="€"):
    """One sale, two payment amounts, one inventory deduction."""
    price_parts = re.split(r"\bза\b|=", text, maxsplit=1, flags=re.IGNORECASE)
    if len(price_parts) != 2:
        raise ValueError("Смешанная оплата: 1 за 50€/40💳.")
    chunks = price_parts[1].strip().split("/")
    if len(chunks) != 2:
        raise ValueError("Укажите две суммы оплаты через /, например 50€/40💳.")
    payments = {}
    currency = default_currency
    for index, chunk in enumerate(chunks):
        match = re.fullmatch(r"\s*(\d+(?:[.,]\d{1,2})?)\s*(.*?)\s*", chunk)
        if not match:
            raise ValueError("Неверная сумма смешанной оплаты.")
        amount = float(match.group(1).replace(",", "."))
        suffix = match.group(2).lower()
        if not re.fullmatch(
            rf"(?:(?:€|\$|eur|евро|usd|грн|uah|{PAYMENT_MARKER})\s*)*",
            suffix,
        ):
            raise ValueError("Пометьте оплату символами 💵 или 💳.")
        currencies = re.findall(r"€|\$|eur|евро|usd|грн|uah", suffix)
        if any(CURRENCY_NAMES[item] != currency for item in currencies):
            raise ValueError("Обе части одной сделки должны быть в одной валюте.")
        cash_mark = bool(re.search(CASH_MARKER, suffix))
        card_mark = bool(re.search(CARD_MARKER, suffix))
        if cash_mark and card_mark:
            raise ValueError("Укажите только один способ оплаты для каждой суммы.")
        channel = (
            "card" if card_mark else "cash" if cash_mark or currencies
            else "cash" if index == 0 else "card"
        )
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("Каждая сумма оплаты должна быть больше нуля.")
        payments[channel] = payments.get(channel, 0.0) + amount
    if not math.isfinite(sum(payments.values())):
        raise ValueError("Сумма слишком большая.")
    return payments, currency


def get_balances(chat_id: int):
    balances = {"cash": {}, "card": {}}
    with db_connection() as conn:
        scope, params = ledger_scope(chat_id)
        rows = conn.execute(
            f"""
            SELECT type, payment_type, amount, currency, details
            FROM records
            WHERE {scope} AND type IN
                ('topup', 'deal', 'expense', 'advance', 'purchase',
                 'direct_expense_cash', 'direct_expense_card',
                 'cash_take', 'take_cash', 'take_card')
            """,
            params,
        ).fetchall()

    for record_type, payment_type, amount, currency, details in rows:
        currency = currency or "€"
        direction = 1 if record_type in {"topup", "deal"} else -1
        for channel, part_amount in record_payments(
            record_type, payment_type, amount, details
        ).items():
            if channel in balances:
                balances[channel][currency] = (
                    balances[channel].get(currency, 0.0) + direction * part_amount
                )
    return balances


def format_balance(values: dict[str, float], *, compact=False) -> str:
    nonzero = [(currency, amount) for currency, amount in values.items() if abs(amount) > 0.0001]
    if not nonzero:
        return "0 €"
    return ", ".join(
        f"{format_quantity(amount) if compact else f'{amount:.2f}'} {currency}"
        for currency, amount in sorted(nonzero)
    )


def automatic_period(period: str, now: datetime):
    end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    if period == "day":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        title = format_ru_date(now)
    elif period == "week":
        start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        title = f"{format_ru_date(start)} — {format_ru_date(now)}"
    elif period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        title = f"{format_ru_date(start)} — {format_ru_date(now)}"
    elif period == "all":
        return "0001-01-01 00:00:00", "9999-12-31 23:59:59", "За всё время"
    else:
        raise ValueError("Unknown statistics period")
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), title


def build_period_summary(db_file, chat_id, start_date, end_date, title, format_money):
    with db_connection(db_file) as conn:
        scope, params = ledger_scope(chat_id)
        rows = conn.execute(
            f"""SELECT type, text_summary, amount, currency, payment_type,
                      product_qty, product_unit, details
               FROM records
               WHERE {scope} AND created_at >= ? AND created_at <= ?""",
            (*params, start_date, end_date),
        ).fetchall()
    categories = ("deal", "topup", "expense", "advance", "purchase", "taken")
    totals = {kind: {"cash": {}, "card": {}} for kind in categories}
    movement = {
        kind: {"г": 0.0, "слабый": 0.0, "шт": 0.0}
        for kind in ("deal", "topup_product", "self", "purchase", "direct_expense_product")
    }
    net = {"cash": {}, "card": {}}
    for kind, summary, amount, currency, payment, qty, unit, details in rows:
        product_kind = "self" if kind in {"self_g", "self_sht", "self_use"} else kind
        if product_kind in movement:
            for product_unit, product_qty in record_products(qty, unit, details).items():
                if product_unit in movement[product_kind]:
                    movement[product_kind][product_unit] += product_qty
        category = money_category(kind, summary)
        if category not in totals:
            continue
        currency = currency or "€"
        for channel, part_amount in record_payments(kind, payment, amount, details).items():
            if channel not in net:
                continue
            group = totals[category][channel]
            group[currency] = group.get(currency, 0.0) + part_amount
            direction = 1 if category in {"deal", "topup"} else -1
            net[channel][currency] = net[channel].get(currency, 0.0) + direction * part_amount

    def combined(category):
        result = {}
        for amounts in totals[category].values():
            for currency, amount in amounts.items():
                result[currency] = result.get(currency, 0.0) + amount
        return format_money(result)

    def channels(values):
        return (
            f"💵 Наличные: {format_money(values['cash'])}\n"
            f"💳 Карта: {format_money(values['card'])}"
        )

    expenses = {"cash": {}, "card": {}}
    for channel in expenses:
        for category in ("expense", "purchase"):
            for currency, amount in totals[category][channel].items():
                expenses[channel][currency] = expenses[channel].get(currency, 0.0) + amount

    return (
        f"📊 Итог за период\n📅 {title}\nЗаписей: {len(rows)}\n\n"
        f"📈 Продажи\n{channels(totals['deal'])}\n\n"
        f"Пополнения кассы\n{channels(totals['topup'])}\n\n"
        f"📉 Расходы\n{channels(expenses)}\n"
        f"🛒 Покупки у клиентов (включены в расходы): {combined('purchase')}\n"
        f"👤 Выдано авансом: {combined('advance')}\n"
        f"💵 Забрано из кассы: {combined('taken')}\n\n"
        f"💶 Денег осталось за период\n{channels(net)}\n"
        "(Пополнения + продажи − расходы, включая покупки − авансы − снятия; "
        "без остатка на начало периода.)\n\n"
        "📦 Движение товара\n📦 Товар\n"
        f"• Продано: {format_quantity(movement['deal']['г'])} г\n"
        f"• Пополнено: {format_quantity(movement['topup_product']['г'])} г\n"
        f"• Себе: {format_quantity(movement['self']['г'])} г\n"
        f"• Закупки: {format_quantity(movement['purchase']['г'])} г\n"
        f"• Списано: {format_quantity(movement['direct_expense_product']['г'])} г\n\n"
        "📦 Слабый\n"
        f"• Продано: {format_quantity(movement['deal']['слабый'])} г\n"
        f"• Пополнено: {format_quantity(movement['topup_product']['слабый'])} г\n"
        f"• Себе: {format_quantity(movement['self']['слабый'])} г\n"
        f"• Закупки: {format_quantity(movement['purchase']['слабый'])} г\n"
        f"• Списано: {format_quantity(movement['direct_expense_product']['слабый'])} г\n\n"
        "📦 ХТС\n"
        f"• Продано: {format_quantity(movement['deal']['шт'])} шт.\n"
        f"• Пополнено: {format_quantity(movement['topup_product']['шт'])} шт.\n"
        f"• Себе: {format_quantity(movement['self']['шт'])} шт.\n"
        f"• Закупки: {format_quantity(movement['purchase']['шт'])} шт.\n"
        f"• Списано: {format_quantity(movement['direct_expense_product']['шт'])} шт."
    )


def period_summary(chat_id: int, start_date: str, end_date: str, title: str):
    return build_period_summary(
        DB_FILE, chat_id, start_date, end_date, title, format_balance
    )


async def show_period_stats(context, chat_id, start_dt, end_dt, period_title_str,
                            message_to_edit=None):
    text = period_summary(
        chat_id, start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"), period_title_str,
    )
    if message_to_edit is not None:
        await message_to_edit.edit_text(text, reply_markup=stats_back_keyboard())
    else:
        await context.bot.send_message(
            chat_id=chat_id, text=text, reply_markup=stats_back_keyboard(),
        )


def stats_back_keyboard():
    return None


def parse_period_range(text: str):
    parts = re.split(r"\s*(?:–|—|-|до)\s*", text.strip(), maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None

    current_year = datetime.now().year

    def parse_date(value: str, year: int):
        date_parts = value.strip().split(".")
        if len(date_parts) == 2:
            day, month = (int(part) for part in date_parts)
            return datetime(year, month, day)
        if len(date_parts) == 3:
            day, month, explicit_year = (int(part) for part in date_parts)
            if explicit_year < 100:
                explicit_year += 2000
            return datetime(explicit_year, month, day)
        raise ValueError("Invalid date")

    try:
        start = parse_date(parts[0], current_year)
        end_has_year = len(parts[1].strip().split(".")) == 3
        end = parse_date(parts[1], current_year if end_has_year else start.year)
        if not end_has_year and end < start:
            end = end.replace(year=end.year + 1)
    except (ValueError, OverflowError):
        return None
    if end < start:
        return None
    return start.strftime("%Y-%m-%d 00:00:00"), end.strftime("%Y-%m-%d 23:59:59")


def save_record(
    user_id: int,
    record_type: str,
    text: str,
    created_at: str,
    payment_type: str = "cash",
    amount: float = 0.0,
    currency: str = "€",
    product_qty: float = 0.0,
    product_unit: str = "",
    details: str = "",
    *,
    chat_id: int,
) -> int:
    ledger_scope(chat_id)
    with db_connection() as conn:
        lock_numbering(conn)
        period = accounting_period_key(created_at) if record_type == "deal" else None
        number = allocate_deal_number(conn, chat_id, period) if period else None
        postgres = getattr(conn, "is_postgres", False)
        cursor = conn.execute(
            """
            INSERT INTO records
                (chat_id, user_id, type, text_summary, amount, currency, payment_type,
                  product_qty, product_unit, created_at, details, deal_number, accounting_period)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """ + (" RETURNING id" if postgres else ""),
            (
                chat_id, user_id, record_type, text, amount, currency, payment_type,
                product_qty, product_unit, created_at, details, number, period,
            ),
        )
        return cursor.fetchone()[0] if postgres else cursor.lastrowid


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    clear_input_state(context)
    name = user.first_name if user and user.first_name else "друг"
    if update.message:
        try:
            await update.message.delete()
        except Exception:
            logger.debug("Could not delete the /start message.", exc_info=True)
        await context.bot.send_message(
            chat_id=update.message.chat_id,
            text=f"👋 Привет, {name}! Бот успешно активирован для этого чата. Выберите нужное действие ниже: 👇",
            reply_markup=get_main_reply_keyboard(),
        )


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_input_state(context)
    if update.message:
        await update.message.reply_text(
            "📍 Главное меню учета\nВыберите нужный раздел:",
            reply_markup=get_main_inline_keyboard(),
        )


def history_page(chat_id, page_index, *, include_edit_buttons=False):
    """Group all records by day and paginate within Telegram's message limit."""
    with db_connection() as conn:
        scope, params = ledger_scope(chat_id)
        rows = conn.execute(
            f"""SELECT id, type, text_summary, payment_type, created_at,
                      amount, currency, details, product_qty, product_unit, deal_number FROM records
               WHERE {scope} ORDER BY created_at, id""", params,
        ).fetchall()
    header = "📄 История записей\n💵 наличные · 💳 карта\n\n"
    pages = []
    page_buttons = []
    current_buttons = []
    current = header
    last_date = None
    labels = {
        "topup_product": "📦 Приход", "topup": "➕ Касса",
        "expense": "🛑 Расход", "advance": "👤 Аванс",
        "take_cash": "💵 Забрано из кассы", "cash_take": "💵 Забрано из кассы",
        "take_card": "💳 Забрано с карты", "deal": "✅ Сделка",
        "self_g": "📦 Взято себе", "self_sht": "📦 Взято себе",
    }
    for record_id, kind, summary, payment, created_at, amount, currency, details, qty, unit, deal_number in rows:
        try:
            date = format_ru_date(datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S"))
            time_text = created_at[11:16]
        except (TypeError, ValueError):
            date, time_text = "Дата не указана", "--:--"
        category = money_category(kind, summary)
        label = labels.get("take_cash" if category == "taken" and kind == "expense" else kind, kind)
        values = []
        products = record_products(qty, unit, details)
        if kind in {"deal", "topup_product", "self_g", "self_sht"}:
            values.append(" · ".join(
                f"{product_label(product_unit)} "
                f"{'+' if kind == 'topup_product' else ''}{format_quantity(product_qty)} "
                f"{'шт.' if product_unit == 'шт' else product_measure(product_unit)}"
                for product_unit, product_qty in products.items()
            ) or "Товар: количество не указано")
        if category in {"deal", "topup", "expense", "advance", "taken"}:
            sign = "+" if category == "topup" else "–" if category in {"expense", "advance"} else ""
            values.append(" / ".join(
                f"{sign}{format_quantity(part)} {currency or '€'} {payment_icon(channel)}"
                for channel, part in record_payments(kind, payment, amount, details).items()
            ))
        # Keep the accounting line and show multiline notes below it.
        accounting, note = split_record_note(summary or "", kind)
        if summary:
            values.append(accounting[:1200] if note else summary[:2300])
        deal_suffix = ""
        if kind == "deal":
            deal_suffix = f" №{deal_number}" if deal_number is not None else ""
        line = (
            f"{time_text} {label}{deal_suffix}"
            f" · {' · '.join(values)}"
            f"{f' · #{deal_number}' if kind == 'deal' and deal_number is not None else ''}\n"
        )
        if note:
            line += f"└, {note[:1100]}\n"
        if kind in {"self_use", "purchase", "direct_expense_product",
                    "direct_expense_cash", "direct_expense_card"}:
            record = {
                "record_type": kind, "text_summary": summary or "",
                "amount": amount or 0.0, "currency": currency or "€",
                "payment_type": payment, "product_qty": qty, "product_unit": unit,
                "details": details,
            }
            line = f"{time_text} {format_writeoff(record, history=True)}\n"
        heading = f"📅 {date}\n" if date != last_date else ""
        if len(current) + len(heading) + len(line) > 3600:
            pages.append(current)
            page_buttons.append(current_buttons)
            current_buttons = []
            current = header
            heading = f"📅 {date}\n"
        current += heading + line
        entry_label = f"Сделка #{deal_number}" if kind == "deal" and deal_number is not None else label
        if include_edit_buttons:
            current_buttons.append([InlineKeyboardButton(
                f"✏ {entry_label} · {date} · {time_text}",
                callback_data=f"edit_{record_id}",
            )])
        last_date = date
    pages.append(current if rows else header + "📌 История записей пуста.")
    page_buttons.append(current_buttons)
    page_index = max(0, min(page_index, len(pages) - 1))
    navigation = []
    page_callback = "edit_history" if include_edit_buttons else "history"
    if page_index:
        navigation.append(InlineKeyboardButton("« Ранее", callback_data=f"{page_callback}_{page_index - 1}"))
    if page_index + 1 < len(pages):
        navigation.append(InlineKeyboardButton("Далее »", callback_data=f"{page_callback}_{page_index + 1}"))
    keyboard = page_buttons[page_index] + ([navigation] if navigation else [])
    text = pages[page_index]
    if len(pages) > 1:
        text += f"\nСтраница {page_index + 1} из {len(pages)}"
    return text, InlineKeyboardMarkup(keyboard) if keyboard else None


async def show_menu_section(text, user_id, chat_id, context, message=None):
    """Use the same sections for both the reply keyboard and inline navigation."""
    async def respond(text, reply_markup=None, parse_mode=None):
        keyboard = reply_markup if reply_markup is not None else back_to_main_keyboard()
        if message is not None:
            await message.edit_text(text=text, reply_markup=keyboard, parse_mode=parse_mode)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode=parse_mode)

    if text == "🛒 Сделка":
        await respond(
            text="🛒 Новая сделка\n💬 Отправьте текст сделки в чат "
            "(например: 3 за 150€ байба, 2г за 50, 1 за 90 слабый, "
            "5 хтс за 50 или 1 за 50/40💳)."
        )
        return True

    if text == "➕ Пополнение товара":
        await respond(
            text="➕ Пополнение товара\n📦 Отправьте количество для пополнения "
            "(например: +100г, +50г слабый, +90 шт, +90 хтс, +90 xtc "
            "или вместе: +100г +90 хтс)."
        )
        return True

    if text == "📦 Остаток и касса":
        balances = get_balances(chat_id)
        stock, sold = get_stock(DB_FILE, chat_id)
        totals = get_money_totals(DB_FILE, chat_id)
        with db_connection() as conn:
            scope, params = ledger_scope(chat_id)
            self_products = conn.execute(
                f"""SELECT product_qty, product_unit, details FROM records
                   WHERE {scope} AND type IN ('self_use','self_g','self_sht')""",
                params,
            ).fetchall()
        personally_used = {"г": 0.0, "слабый": 0.0, "шт": 0.0}
        for qty, unit, details in self_products:
            for product_unit, quantity in record_products(qty, unit, details).items():
                if product_unit in personally_used:
                    personally_used[product_unit] += quantity
        balance_text = (
            "📦 Остаток и касса\n\n"
            "📦 Товар\n"
            f"• Товар: {format_quantity(stock['г'])} г\n"
            f"• Слабый: {format_quantity(stock['слабый'])} г\n"
            f"• ХТС: {format_quantity(stock['шт'])} шт.\n\n"
            "👤 Себе\n"
            f"• Товар: {format_quantity(personally_used['г'])} г\n"
            f"• Слабый: {format_quantity(personally_used['слабый'])} г\n"
            f"• ХТС: {format_quantity(personally_used['шт'])} шт.\n\n"
            "📈 Продано с последнего пополнения\n"
            f"• Товар: {format_quantity(sold['г'])} г\n"
            f"• Слабый: {format_quantity(sold['слабый'])} г\n"
            f"• ХТС: {format_quantity(sold['шт'])} шт.\n\n"
            "💶 Деньги\n"
            f"💵 Наличные: {format_balance(balances['cash'])}\n"
            f"💳 Карта: {format_balance(balances['card'])}\n"
            f"➕ Пополнено кассы: {format_balance(totals['topup'])}\n"
            f"➖ Расходы: {format_balance(totals['expense'])}\n"
            f"👤 Выдано авансом: {format_balance(totals['advance'])}\n"
            f"🛒 Покупки у клиентов: {format_balance(totals['purchase'])}\n"
            f"💵 Забрано из кассы: {format_balance(totals['taken'])}"
        )
        await respond(text=balance_text)
        return True

    if text == "📋 История сделок":
        page = 0
        while True:
            history_text, keyboard = history_page(chat_id, page)
            if page == 0:
                await respond(text=history_text, reply_markup=InlineKeyboardMarkup([]))
            else:
                await context.bot.send_message(
                    chat_id=chat_id, text=history_text, reply_markup=None,
                )
            if not keyboard or not any(
                button.callback_data == f"history_{page + 1}"
                for row in keyboard.inline_keyboard for button in row
            ):
                break
            page += 1
        return True

    if text == "💵 Забрать кассу":
        balances = get_balances(chat_id)
        await respond(
            text="💵 Касса\n"
            f"Сейчас в кассе: {format_balance({key: max(0.0, value) for key, value in balances['cash'].items()}, compact=True)}\n\n"
            "Желаете забрать деньги?",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("💵 Забрать всю кассу", callback_data="take_all_cash")],
                    [InlineKeyboardButton("✏ Ввести сумму", callback_data="take_custom_cash")],
                    [InlineKeyboardButton("Отменить", callback_data="cancel_kassa")],
                ]
            ),
        )
        return True

    if text == "📊 Итог за период":
        await respond(
            text="📊 Итог за период\n🗓 Выберите период для автоматического расчета:",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("📅 За сегодня", callback_data="stats_day"),
                        InlineKeyboardButton("📆 За неделю", callback_data="stats_week"),
                    ],
                    [
                        InlineKeyboardButton("🗓 За месяц", callback_data="stats_month"),
                        InlineKeyboardButton("♾ Итог за всё время", callback_data="stats_all"),
                    ],
                    [InlineKeyboardButton("🔍 Выбрать период (даты)", callback_data="stats_custom")],
                ]
            ),
        )
        return True

    if text in {"✏ Изменить записи", "✏️ Изменить записи"}:
        context.chat_data["pending_action"] = "edit_id"
        context.chat_data["input_chat_id"] = chat_id
        history_text, keyboard = history_page(chat_id, 0, include_edit_buttons=True)
        await respond(
            text="✏ Изменение записи\n"
            "🔢 Напишите номер сделки (например: 40), которую хотите изменить,\n"
            "или выберите нужную запись ниже.\n\n"
            + history_text,
            reply_markup=keyboard or back_to_main_keyboard(),
        )
        return True

    if text == "📖 Как записывать":
        await respond(
            text="📖 Как записывать:\n\n"
            "• Сделки: <code>3 за 150€ байба</code>, <code>2г за 50</code>, "
            "<code>1 за 90 слабый</code> или <code>5 хтс за 50</code>\n"
            "• Смешанная оплата: 1 за 50/40💳 (50 наличными, 40 картой)\n"
            "• Общая сумма с оплатой картой: 1 за 90 40💳 (50 наличными, 40 картой)\n"
            "• Для себя: <code>0,5 себе</code>, <code>5г слабый себе</code>, <code>10 хтс себес</code> или <code>10 шт себе</code> (только списание товара).\n"
            "• Покупки у клиентов — в столбик или строку:\n<code>-10xtc\n-250€\nRay ban</code>\n"
            "  Или: <code>-10xtc -250€ Ray ban</code>. Можно указать только товар и сумму.\n"
            "  Списываются и товар, и деньги. Для карты: -100€ 💳 во второй строке.\n"
            "• Прямое списание товара: <code>-25г</code>, <code>-10г слабый</code>, <code>-10 хтс</code> или <code>-10 шт</code> (без денег).\n"
            "• Прямое списание денег: <code>-200€</code> или <code>-200€ 💳</code> (без товара).\n"
            "• Товар: +100г, +50г слабый, +90 шт / хтс / xtc или вместе: +100г +90 хтс\n"
            "• Касса: +2500 евро, +1000 💳 или пополнить 2500\n"
            "• Расходы: - 150 карта, - 40 бензин или расход 40\n"
            "• Расходники без минуса: топка 100€, бенз 100€, бензин 100€, "
            "расходник 100€, хавка 20€, вода 5€ или тачка 100€\n"
            "• Если отправить только название расхода (например, хавка), бот спросит сумму.\n"
            "• Слабый товар: <code>1 за 90 слабый</code> или <code>2г за 50 слабый</code>.\n"
            "• Аванс: аванс 150 или - 150 аванс карта\n"
            "• Комментарий к сделке можно написать после суммы: <code>3 за 150€ байба</code>.\n"
            "• Комментарий к сделке, расходу или авансу — также на следующих строках; "
            "сумму и способ оплаты укажите в первой строке.\n"
            "• Оплата: 💵 наличные / cash | 💳 карта / card\n"
            "• Без единицы количество сделки считается в граммах; «слабый» — отдельный товар в граммах; хтс/xtc = шт.\n"
            "• Запись сохраняется только после нажатия «✅ Подтвердить».\n"
            "• Старые записи без количества не входят в остаток; проверьте их через изменение записей.",
            parse_mode="HTML",
        )
        return True

    if text == "📁 Резервная копия":
        with db_connection() as conn:
            scope, params = ledger_scope(chat_id)
            rows = conn.execute(
                f"""
                SELECT id, type, text_summary, amount, currency, payment_type,
                       product_qty, product_unit, created_at, details, chat_id, user_id,
                       deal_number, accounting_period
                FROM records
                WHERE {scope}
                ORDER BY id
                """,
                params,
            ).fetchall()
        if not rows:
            await respond(text="📁 Пока нечего сохранять — история этого чата пуста.")
            return True
        output = io.StringIO(newline="")
        writer = csv.writer(output)
        writer.writerow(
            [
                "ID", "Type", "Summary", "Amount", "Currency", "PaymentType",
                "ProductQty", "ProductUnit", "CreatedAt", "Details", "ChatID", "AuthorID",
                "DealNumber", "AccountingPeriod",
            ]
        )
        writer.writerows(rows)
        document = io.BytesIO(output.getvalue().encode("utf-8-sig"))
        document.name = "backup_accounting.csv"
        await context.bot.send_document(
            chat_id=chat_id,
            document=document,
            caption="📁 Резервная копия успешно сформирована. В файле записи этого чата.",
        )
        await respond(text="📁 Резервная копия отправлена.")
        return True

    if text == "🔄 Рестарт учёта":
        await respond(
            text="🔄 Рестарт учёта\n"
            "⚠️ Удалить все сделки и историю для этого чата? Это нельзя отменить.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("⚠️ Да, начать с нуля", callback_data="menu_restart_do")],
                ]
            ),
        )
        return True
    return False


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or not update.effective_user:
        return
    text = update.message.text.strip()
    text = MENU_ALIASES.get(text, text)
    user_id = update.effective_user.id
    chat_id = update.message.chat_id
    try:
        await update.message.delete()
    except Exception:
        logger.debug("Could not delete an incoming user message.", exc_info=True)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if text in set(MENU_SECTIONS.values()) | {"✏️ Изменить записи"}:
        clear_input_state(context)
        await show_menu_section(text, user_id, chat_id, context)
        return
    if text.lower() in {"меню", "главное меню"}:
        clear_input_state(context)
        await context.bot.send_message(
            chat_id=chat_id,
            text="📍 Главное меню учета\nВыберите нужный раздел:",
            reply_markup=get_main_inline_keyboard(),
        )
        return

    pending_action = context.chat_data.get("pending_action")
    if pending_action and context.chat_data.get("input_chat_id", chat_id) != chat_id:
        await context.bot.send_message(
            chat_id=chat_id, text="Продолжите ввод в чате, где начали это действие.",
        )
        return
    if pending_action == "expense_amount":
        expense = context.chat_data.get("pending_expense")
        if not expense or expense["chat_id"] != chat_id:
            await context.bot.send_message(
                chat_id=chat_id, text="Продолжите ввод суммы в чате, где начали запись расхода.",
            )
            return
        parsed = parse_amount(text, allow_plain_number=True)
        if not parsed or not math.isfinite(parsed[0]) or parsed[0] <= 0:
            await context.bot.send_message(
                chat_id=chat_id, text="Введите положительную сумму, например 100 или 100 €.",
                reply_markup=back_to_main_keyboard(),
            )
            return
        amount, currency = parsed
        accounting, note = split_record_note(expense["text"], "expense")
        expense_text = f"{accounting} {amount:.2f} {currency}"
        if note:
            expense_text += f"\n{note}"
        if await create_record_preview(
            expense_text, chat_id, context,
            editing_record_id=expense["editing_record_id"],
        ):
            clear_input_state(context)
        return
    if pending_action == "cash_take_custom" or context.chat_data.get("waiting_for_take_cash_amount"):
        if context.chat_data.get("cash_withdraw_chat_id", chat_id) != chat_id:
            await context.bot.send_message(
                chat_id=chat_id, text="Продолжите ввод суммы в чате, где начали снятие наличных.",
            )
            return
        parsed = parse_amount(text, allow_plain_number=True)
        if not parsed or not math.isfinite(parsed[0]) or parsed[0] <= 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Введите положительную сумму, например 25 или 25 €.",
                reply_markup=back_to_main_keyboard(),
            )
            return
        amount, currency = parsed
        available = get_balances(chat_id)["cash"].get(currency, 0.0)
        if amount > available + 0.0001:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Недостаточно наличных: доступно {available:.2f} {currency}.",
                reply_markup=back_to_main_keyboard(),
            )
            clear_input_state(context)
            return
        save_record(
            user_id,
            "take_cash",
            f"Снятие кассы: {amount:.2f} {currency}",
            now_str,
            amount=amount,
            currency=currency,
            chat_id=chat_id,
        )
        remaining_cash = get_balances(chat_id)["cash"]
        remaining_text = " / ".join(
            f"{format_quantity(balance)} {balance_currency}"
            for balance_currency, balance in sorted(remaining_cash.items())
        ) or f"0 {currency}"
        clear_input_state(context)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"💵 Касса забрана\n"
                 f"💸 Забрано: {format_quantity(amount)} {currency}\n"
                 f"💶 Осталось в кассе: {remaining_text}\n\n"
                 "✅ Сумма успешно учтена в этом чате.",
            reply_markup=back_to_main_keyboard(),
        )
        return

    if pending_action == "period_summary":
        date_range = parse_period_range(text)
        if not date_range:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Не удалось распознать даты. Используйте формат 25.02.2026 – 01.03.2026.",
            )
            return
        title = " — ".join(
            format_ru_date(datetime.strptime(value, "%Y-%m-%d %H:%M:%S"))
            for value in date_range
        )
        summary = period_summary(chat_id, *date_range, title)
        context.chat_data.pop("pending_action", None)
        await context.bot.send_message(
            chat_id=chat_id,
            text=summary,
            reply_markup=stats_back_keyboard(),
        )
        return

    if pending_action == "edit_id":
        id_match = re.search(r"\d+", text)
        if not id_match:
            await context.bot.send_message(chat_id=chat_id, text="Отправьте номер сделки, например #40, или выберите запись кнопкой.")
            return
        number = int(id_match.group())
        with db_connection() as conn:
            scope, params = ledger_scope(chat_id)
            matches = conn.execute(
                f"SELECT id, created_at FROM records WHERE type = 'deal' AND deal_number = ? AND {scope} "
                "ORDER BY created_at DESC, id DESC",
                (number, *params),
            ).fetchall()
        if not matches:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Запись не найдена в вашей истории.",
                reply_markup=back_to_main_keyboard(),
            )
            context.chat_data.pop("pending_action", None)
            return
        if len(matches) > 1:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Сделка #{number} встречается в разные дни. Выберите нужную запись:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        f"Сделка #{number} · {created_at or 'Дата не указана'}",
                        callback_data=f"edit_{record_id}",
                    )]
                    for record_id, created_at in matches
                ]),
            )
            return
        record_id = matches[0][0]
        context.chat_data["pending_action"] = "edit_text"
        context.chat_data["editing_record_id"] = record_id
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"{record_edit_title(chat_id, record_id)}.\nОтправьте новый текст.\n"
            "Чтобы удалить запись, напишите «удалить».",
            reply_markup=back_to_main_keyboard(),
        )
        return

    if pending_action == "edit_text":
        record_id = context.chat_data.get("editing_record_id")
        if text.lower() in {"удалить", "delete"}:
            with db_connection() as conn:
                scope, params = ledger_scope(chat_id)
                cursor = conn.execute(
                    f"DELETE FROM records WHERE id = ? AND {scope}",
                    (record_id, *params),
                )
            context.chat_data.pop("pending_action", None)
            context.chat_data.pop("editing_record_id", None)
            await context.bot.send_message(
                chat_id=chat_id,
                text="Запись удалена." if cursor.rowcount else "Запись не найдена.",
                reply_markup=back_to_main_keyboard(),
            )
            return
        if await create_record_preview(text, chat_id, context, editing_record_id=record_id):
            clear_input_state(context)
        return

    await create_record_preview(text, chat_id, context)


async def delete_record_preview(message, fallback_text):
    try:
        await message.delete()
    except Exception:
        # A deletion can be refused by Telegram; remove the buttons either way.
        await message.edit_text(fallback_text, reply_markup=back_to_main_keyboard())

def legacy_receipt_record_id(chat_id, message):
    """Resolve old, unbound deal buttons only when their receipt identifies one record."""
    if getattr(message, "forward_origin", None):
        return None
    match = re.match(r"^✅ Сделка [№#](\d+)(?:\n|$)", getattr(message, "text", "") or "")
    if not match:
        return None
    with db_connection() as conn:
        scope, params = ledger_scope(chat_id)
        rows = conn.execute(
            f"SELECT id, accounting_period FROM records "
            f"WHERE type = 'deal' AND deal_number = ? AND {scope}",
            (int(match.group(1)), *params),
        ).fetchall()
    confirmed_at = getattr(message, "edit_date", None)
    if confirmed_at:
        # Stored timestamps use the server's local datetime, as does record creation.
        local_time = confirmed_at.astimezone().replace(tzinfo=None)
        period = accounting_period_key(local_time.strftime("%Y-%m-%d %H:%M:%S"))
        rows = [row for row in rows if row[1] == period]
    return rows[0][0] if len(rows) == 1 else None


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    user_id = query.from_user.id
    if not query.message:
        return
    chat_id = query.message.chat_id

    if data == "trigger_edit":
        record_id = legacy_receipt_record_id(chat_id, query.message)
        if record_id is None:
            clear_input_state(context)
            await query.message.reply_text(
                "Не удалось однозначно определить запись по старой кнопке. "
                "Выберите нужную запись в меню «✏ Изменить записи».",
            )
            return
        data = f"receipt_edit_{record_id}"

    if data == "cancel_kassa":
        clear_input_state(context)
        await delete_record_preview(query.message, "❌ Действие отменено.")

    elif data in {"back_to_main", "action_cancel", "cancel_take"}:
        clear_input_state(context)
        await query.message.edit_text(
            (
                "❌ Действие отменено.\n\n" if data == "cancel_take" else ""
            ) + "📍 Главное меню учета\nВыберите нужный раздел:",
            reply_markup=get_main_inline_keyboard(),
        )

    elif data == "stats_menu_back":
        clear_input_state(context)
        await show_menu_section(
            "📊 Итог за период", user_id, query.message.chat_id, context, query.message
        )

    elif data in MENU_SECTIONS:
        clear_input_state(context)
        await show_menu_section(
            MENU_SECTIONS[data], user_id, query.message.chat_id, context, query.message
        )

    elif data.startswith(("history_", "edit_history_")):
        clear_input_state(context)
        include_edits = data.startswith("edit_history_")
        if include_edits:
            context.chat_data["pending_action"] = "edit_id"
            context.chat_data["input_chat_id"] = chat_id
        try:
            page = int(data.rpartition("_")[2])
        except ValueError:
            await query.message.reply_text("Некорректный номер страницы.")
            return
        text, keyboard = history_page(chat_id, page, include_edit_buttons=include_edits)
        await query.message.edit_text(text, reply_markup=keyboard if include_edits else None)

    elif data.startswith(("confirm_", "cancel_")):
        key = data.partition("_")[2]
        records = context.chat_data.get("pending_records", {})
        pending = records.get(key)
        if not pending:
            # Without chat-scoped state we cannot verify this message.
            await query.message.reply_text(
                "⚠️ Время подтверждения истекло или запись уже обработана. Отправьте запись снова.",
            )
            return
        if pending and pending["chat_id"] != query.message.chat_id:
            await query.message.reply_text(
                "⚠️ Это подтверждение принадлежит другому чату."
            )
            return
        if pending.get("message_id") is not None and pending["message_id"] != query.message.message_id:
            await query.message.reply_text("⚠️ Это подтверждение принадлежит другому сообщению.")
            return
        if datetime.now() - pending["created_at"] >= timedelta(minutes=30):
            records.pop(key, None)
            await delete_record_preview(
                query.message,
                "⚠️ Время подтверждения истекло или запись уже обработана. Отправьте запись снова.",
            )
            return
        if data.startswith("cancel_"):
            records.pop(key)
            await delete_record_preview(query.message, "❌ Запись отменена.")
            return
        record_id = pending.get("editing_record_id")
        if record_id is not None:
            with db_connection() as conn:
                lock_numbering(conn)
                scope, params = ledger_scope(chat_id)
                original = conn.execute(
                    f"SELECT type, created_at, deal_number, accounting_period "
                    f"FROM records WHERE id = ? AND {scope}", (record_id, *params),
                ).fetchone()
                number, period = None, None
                if original and pending["record_type"] == "deal":
                    period = original[3] or accounting_period_key(original[1])
                    number = (
                        original[2] if original[0] == "deal" and original[2] is not None
                        else allocate_deal_number(conn, chat_id, period)
                    )
                cursor = conn.execute(
                    f"""
                    UPDATE records
                    SET type = ?, text_summary = ?, amount = ?, currency = ?, payment_type = ?,
                        product_qty = ?, product_unit = ?, details = ?,
                        deal_number = ?, accounting_period = ?
                    WHERE id = ? AND {scope}
                    """,
                    (
                        pending["record_type"], pending["text_summary"], pending["amount"],
                        pending["currency"], pending["payment_type"],
                        pending["product_qty"], pending["product_unit"],
                        pending["details"], number, period, record_id, *params,
                    ),
                )
            if not cursor.rowcount:
                records.pop(key)
                await query.message.edit_text(
                    "Запись не найдена в вашей истории.",
                    reply_markup=back_to_main_keyboard(),
                )
                return
        else:
            record_id = save_record(
                user_id,
                pending["record_type"],
                pending["text_summary"],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                pending["payment_type"],
                pending["amount"],
                pending["currency"],
                pending["product_qty"],
                pending["product_unit"],
                pending["details"],
                chat_id=chat_id,
            )
        # No await between saving and consuming the preview: repeated clicks cannot save twice.
        records.pop(key)
        await query.message.edit_text(
            format_saved_record(pending, record_id),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Изменить", callback_data=f"edit_rec_{record_id}")],
            ]),
        )

    elif data == "take_card":
        clear_input_state(context)
        await query.message.edit_text(
            "Забрать кассу можно только наличными. Баланс карты не изменён.",
            reply_markup=back_to_main_keyboard(),
        )

    elif data in {"take_cash", "cash_take_all", "take_all_cash"}:
        clear_input_state(context)
        payment_type = "cash"
        pay_icon = "💵"
        pay_name = "наличные"
        available = {
            currency: amount
            for currency, amount in get_balances(chat_id)[payment_type].items()
            if amount > 0.0001
        }
        if not available:
            result = f"{pay_icon} Нет доступных средств для снятия ({pay_name})."
        else:
            for currency, amount in available.items():
                save_record(
                    user_id,
                    "take_cash",
                    f"Снятие кассы ({pay_name}): {amount:.2f} {currency}",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    payment_type=payment_type,
                    amount=amount,
                    currency=currency,
                    chat_id=chat_id,
                )
            result = "💵 Касса забрана\n💸 Забрано: " + " / ".join(
                f"{format_quantity(amount)} {currency}" for currency, amount in available.items()
            )
            result += "\n\nКасса обнулена. Новые наличные будут считаться заново."
        await query.message.edit_text(result, reply_markup=back_to_main_keyboard())

    elif data in {"cash_take_custom", "take_custom_cash"}:
        clear_input_state(context)
        context.chat_data["pending_action"] = "cash_take_custom"
        context.chat_data["waiting_for_take_cash_amount"] = True
        context.chat_data["cash_withdraw_chat_id"] = query.message.chat_id
        await query.message.edit_text(
            "✏ Ввести сумму\n💬 Отправьте в чат сумму наличных, которую хотите забрать "
            "(например: 150 или 150 €):",
            reply_markup=back_to_main_keyboard(),
        )

    elif data == "stats_custom":
        clear_input_state(context)
        context.chat_data["pending_action"] = "period_summary"
        context.chat_data["input_chat_id"] = chat_id
        await query.message.edit_text(
            "📊 Итог за период\n"
            "Отправьте две даты: 25.02.2026 – 01.03.2026.\n"
            "Год можно не писать: 25.02 – 01.03.",
            reply_markup=stats_back_keyboard(),
        )

    elif data in {"stats_day", "stats_week", "stats_month", "stats_all"}:
        clear_input_state(context)
        start_date, end_date, title = automatic_period(
            data.partition("_")[2], datetime.now()
        )
        await query.message.edit_text(
            period_summary(chat_id, start_date, end_date, title),
            reply_markup=stats_back_keyboard(),
        )

    elif data == "menu_restart_do":
        with db_connection() as conn:
            lock_numbering(conn)
            scope, params = ledger_scope(chat_id)
            conn.execute(f"DELETE FROM records WHERE {scope}", params)
            conn.execute(f"DELETE FROM stock_topups WHERE {scope}", params)
            conn.execute("DELETE FROM deal_counters WHERE chat_id = ?", (chat_id,))
        invalidate_chat_inputs(context, chat_id)
        await query.message.edit_text(
            "🔄 Учет для этого чата успешно обнулен. Нумерация сделок начнется с №1.",
            reply_markup=back_to_main_keyboard(),
        )

    elif data.startswith(("edit_", "receipt_edit_")):
        clear_input_state(context)
        match = re.fullmatch(r"(?:edit|edit_rec|receipt_edit)_(\d+)", data)
        if not match:
            await query.message.reply_text("Некорректный номер записи.")
            return
        record_id = int(match.group(1))
        receipt_button = data.startswith(("edit_rec_", "receipt_edit_"))
        respond = query.message.reply_text if receipt_button else query.message.edit_text
        with db_connection() as conn:
            scope, params = ledger_scope(chat_id)
            row = conn.execute(
                f"SELECT id FROM records WHERE id = ? AND {scope}",
                (record_id, *params),
            ).fetchone()
        if row:
            context.chat_data["pending_action"] = "edit_text"
            context.chat_data["input_chat_id"] = chat_id
            context.chat_data["editing_record_id"] = record_id
            await respond(
                f"{record_edit_title(chat_id, record_id)}.\nОтправьте новый текст.\n"
                "Чтобы удалить запись, напишите «удалить».",
                reply_markup=back_to_main_keyboard(),
            )
        else:
            await respond(
                "Запись не найдена в вашей истории.",
                reply_markup=back_to_main_keyboard(),
            )


def build_application() -> Application:
    application = Application.builder().bot(BoldHTMLBot(token=TOKEN)).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    application.add_handler(CallbackQueryHandler(button_callback))
    return application


@app.get("/")
def home():
    return "Finance Bot service. Check /api/healthz for readiness.", 200


@app.get("/api/healthz")
def health():
    try:
        with db_connection() as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as error:
        logger.warning("Database readiness failed (%s).", type(error).__name__)
        return {"status": "unavailable"}, 503
    enabled = os.getenv("BOT_POLLING_ENABLED", "false").lower() == "true"
    if enabled and (telegram_application is None or not telegram_application.running):
        return {"status": "starting"}, 503
    return {"status": "ok", "telegram": "enabled" if enabled else "workspace-paused"}, 200


@app.post(WEBHOOK_PATH)
def webhook():
    if not WEBHOOK_URL:
        abort(404)
    if telegram_application is None or telegram_loop is None:
        abort(503)

    received_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(received_secret, WEBHOOK_SECRET):
        abort(403)

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        abort(400)

    update = Update.de_json(payload, telegram_application.bot)
    future = asyncio.run_coroutine_threadsafe(
        telegram_application.update_queue.put(update),
        telegram_loop,
    )
    try:
        future.result(timeout=10)
    except Exception:
        future.cancel()
        logger.warning("Could not enqueue a Telegram webhook update.")
        abort(503)
    return "ok", 200


def make_http_server():
    return make_server("0.0.0.0", PORT, app, threaded=True)


def run_polling_mode(application: Application):
    global telegram_application
    telegram_application = application
    server = make_http_server()
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        logger.info("Starting Telegram polling and health server on port %s.", PORT)
        application.run_polling()
    finally:
        server.shutdown()
        server_thread.join(timeout=5)


async def run_webhook_mode(application: Application):
    global telegram_application, telegram_loop

    if not WEBHOOK_URL.startswith("https://"):
        raise RuntimeError("WEBHOOK_URL must be a public HTTPS base URL.")

    telegram_application = application
    telegram_loop = asyncio.get_running_loop()
    server = make_http_server()
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    initialized = False
    started = False
    try:
        await application.initialize()
        initialized = True
        await application.start()
        started = True
        await application.bot.set_webhook(
            url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            secret_token=WEBHOOK_SECRET,
            allowed_updates=Update.ALL_TYPES,
        )
        logger.info("Starting Telegram webhook server on port %s.", PORT)

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for handled_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(handled_signal, stop_event.set)
            except NotImplementedError:
                pass
        await stop_event.wait()
    finally:
        server.shutdown()
        server_thread.join(timeout=5)
        if started:
            await application.stop()
        if initialized:
            await application.shutdown()


def main():
    if os.getenv("BOT_POLLING_ENABLED", "false").lower() != "true":
        logger.info("Workspace health server only; Telegram polling is reserved for publishing.")
        make_http_server().serve_forever()
        return
    if not TOKEN:
        raise RuntimeError("Set TELEGRAM_TOKEN or TELEGRAM_BOT_TOKEN in Replit Secrets.")

    application = build_application()
    logger.info(
        "Starting Finance Bot with %s storage.",
        "PostgreSQL" if uses_postgres(DB_FILE) else DB_FILE.name,
    )
    with polling_lock(DB_FILE):
        if WEBHOOK_URL:
            asyncio.run(run_webhook_mode(application))
        else:
            run_polling_mode(application)


init_db()

if __name__ == "__main__":
    main()
