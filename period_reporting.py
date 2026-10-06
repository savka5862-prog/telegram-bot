"""Date labels and user-scoped financial and inventory period reports."""

import sqlite3
from contextlib import closing

from accounting_helpers import format_quantity, money_category


MONTHS_RU = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}


def format_ru_date(dt):
    return f"{dt.day} {MONTHS_RU[dt.month]} {dt.year} г."


def build_period_summary(db_file, user_id, start_date, end_date, title, format_money):
    with closing(sqlite3.connect(db_file)) as conn:
        rows = conn.execute(
            """SELECT type, text_summary, amount, currency, payment_type,
                      product_qty, product_unit
               FROM records
               WHERE user_id = ? AND created_at >= ? AND created_at <= ?""",
            (user_id, start_date, end_date),
        ).fetchall()
    categories = ("deal", "topup", "expense", "advance", "taken")
    totals = {kind: {"cash": {}, "card": {}} for kind in categories}
    movement = {kind: {"г": 0.0, "шт": 0.0} for kind in ("deal", "topup_product")}
    net = {"cash": {}, "card": {}}
    for kind, summary, amount, currency, payment, qty, unit in rows:
        if kind in movement and unit in movement[kind]:
            movement[kind][unit] += qty or 0.0
        category = money_category(kind, summary)
        if category not in totals:
            continue
        payment = (
            "card" if kind == "take_card"
            else "cash" if kind in {"take_cash", "cash_take"}
            else payment or "cash"
        )
        if payment not in net:
            continue
        currency = currency or "€"
        amount = amount or 0.0
        group = totals[category][payment]
        group[currency] = group.get(currency, 0.0) + amount
        direction = 1 if category in {"deal", "topup"} else -1
        net[payment][currency] = net[payment].get(currency, 0.0) + direction * amount

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

    return (
        f"📊 Итог за период\n📅 {title}\nЗаписей: {len(rows)}\n\n"
        f"Продажи\n{channels(totals['deal'])}\n\n"
        f"Пополнения кассы\n{channels(totals['topup'])}\n\n"
        f"Расходы\n{channels(totals['expense'])}\n"
        f"👤 Выдано авансом: {combined('advance')}\n"
        f"💵 Забрано из кассы: {combined('taken')}\n\n"
        f"Денег осталось за период\n{channels(net)}\n"
        "(Пополнения + продажи − расходы − авансы − снятия; "
        "без остатка на начало периода.)\n\n"
        "Движение товара\n"
        "📦 Товар\n"
        f"• Продано: {format_quantity(movement['deal']['г'])} г\n"
        f"• Пополнено: {format_quantity(movement['topup_product']['г'])} г\n"
        "• Взято себе: не учитывается\n\n"
        "📦 ХТС\n"
        f"• Продано: {format_quantity(movement['deal']['шт'])} шт.\n"
        f"• Пополнено: {format_quantity(movement['topup_product']['шт'])} шт.\n"
        "• Взято себе: не учитывается"
    )
