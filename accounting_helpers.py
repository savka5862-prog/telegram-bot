"""Inventory parsing and ledger aggregates, independent of Telegram handlers."""

import re
import sqlite3


PRODUCT_RE = re.compile(
    r"^\s*(?:\+\s*|пополнить\s+)?(\d+(?:[.,]\d+)?)\s*"
    r"((?:грамм(?:а|ов)?|гр|г|g)\s+слабый|слабый|"
    r"грамм(?:а|ов)?|гр|г|g|штук(?:а|и)?|шт\.?|хтс)?"
    r"(?=$|\s|[.,;:=])",
    re.IGNORECASE,
)


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
            unit = "шт" if raw_unit.startswith(("шт", "хтс")) else "г"
    else:
        # A currency amount alone is not a product quantity.
        unit = default_unit
    return (float(match.group(1).replace(",", ".")), unit) if unit else (0.0, "")


def get_stock(db_file, user_id):
    quantities = {"г": 0.0, "слабый": 0.0, "шт": 0.0}
    sold_since_topup = {"г": 0.0, "слабый": 0.0, "шт": 0.0}
    with sqlite3.connect(db_file) as conn:
        rows = conn.execute(
            """
            SELECT type, product_qty, product_unit
            FROM records WHERE user_id = ? AND type IN ('topup_product', 'deal')
            ORDER BY created_at, id
            """,
            (user_id,),
        ).fetchall()
    for record_type, qty, unit in rows:
        if unit not in quantities:
            continue
        qty = qty or 0.0
        if record_type == "topup_product":
            quantities[unit] += qty
            sold_since_topup[unit] = 0.0
        else:
            quantities[unit] -= qty
            sold_since_topup[unit] += qty
    return quantities, sold_since_topup


def money_category(record_type, summary=""):
    if record_type in {"cash_take", "take_cash", "take_card"}:
        return "taken"
    if record_type == "expense" and (summary or "").startswith("Снятие"):
        # Earlier working bot versions recorded withdrawals as named expenses.
        return "taken"
    return record_type


def get_money_totals(db_file, user_id):
    totals = {"topup": {}, "expense": {}, "advance": {}, "taken": {}}
    with sqlite3.connect(db_file) as conn:
        rows = conn.execute(
            "SELECT type, text_summary, amount, currency FROM records WHERE user_id = ?",
            (user_id,),
        ).fetchall()
    for record_type, summary, amount, currency in rows:
        category = money_category(record_type, summary)
        if category not in totals:
            continue
        currency = currency or "€"
        totals[category][currency] = totals[category].get(currency, 0.0) + (amount or 0.0)
    return totals


def format_quantity(value):
    return f"{value:g}"
