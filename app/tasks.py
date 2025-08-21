#!/usr/bin/env python3
# bot.py — однократная проверка задач и отправка отчётов в Telegram по местному времени IRKUTSK

import os
import asyncio
from dotenv import load_dotenv
from datetime import datetime, date, time, timezone
from zoneinfo import ZoneInfo
from telegram import Bot
from app.db import SessionLocal
from app.models import Task

load_dotenv()  # читает TELEGRAM_TOKEN и CHAT_ID из .env

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID"))

# Таймзона Иркутска (UTC+8)
IRKUTSK = ZoneInfo("Asia/Irkutsk")


def format_tasks(tasks: list[Task]) -> str:
    """Формирует нумерованный список задач, поля внутри каждой задачи разделены переносами строк.
    Если запланированное начало или ЖВВ просрочены, перед временем ставится 🔴."""
    blocks = []
    now_local = datetime.now(timezone.utc).astimezone(IRKUTSK)

    for idx, t in enumerate(tasks, start=1):
        # Конвертация времени в локальное IRKUTSK
        planned_utc = (t.planned_start.replace(tzinfo=timezone.utc)
                       if t.planned_start.tzinfo is None else
                       t.planned_start.astimezone(timezone.utc))
        planned_local = planned_utc.astimezone(IRKUTSK)

        due_utc = (t.due_datetime.replace(tzinfo=timezone.utc)
                   if t.due_datetime.tzinfo is None else
                   t.due_datetime.astimezone(timezone.utc))
        due_local = due_utc.astimezone(IRKUTSK)

        address = t.address_raw
        # Пометка просрочки перед каждым временем
        start_prefix = "🔴 " if planned_local < now_local else ""
        due_prefix = "🔴 " if due_local < now_local else ""

        # Формат времени hh.mm DD.MM.YYYY
        start_str = planned_local.strftime("%H:%M  %d.%m.%Y")
        due_str = due_local.strftime("%H:%M  %d.%m.%Y")

        # Формируем текст блока задачи
        block = [
            f"{idx}.",
            f"Адрес: {address}",
            f"Начало: {start_prefix}{start_str}",
            f"ЖВВ: {due_prefix}{due_str}",
        ]
        blocks.append("\n".join(block))
    # Разделяем блоки пустой строкой
    return "\n\n".join(blocks)


async def _send_messages(bot: Bot, messages: list[str]) -> None:
    """Асинхронно отправляет список сообщений через Bot.send_message."""
    for msg in messages:
        await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")


def main() -> None:
    # Инициализация Bot и БД
    bot = Bot(token=TELEGRAM_TOKEN)
    db = SessionLocal()

    # Текущее время для фильтрации просрочки
    now_utc = datetime.now(timezone.utc)

    # 1) Просроченные задачи
    overdue = (
        db.query(Task)
          .filter(Task.status != 'done', Task.status != 'cancelled', Task.due_datetime < now_utc)
          .all()
    )
    if overdue:
        text1 = "📌 *Просроченные задачи:*\n" + format_tasks(overdue)
    else:
        text1 = "✅ Нет просроченных задач."

    # 2) Задачи на сегодня (по IRKUTSK)
    today_local = datetime.now(IRKUTSK).date()
    start_local = datetime.combine(today_local, time.min, tzinfo=IRKUTSK)
    end_local = datetime.combine(today_local, time.max, tzinfo=IRKUTSK)
    # Переводим границы в UTC для запроса к БД
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    todays = (
        db.query(Task)
          .filter(Task.status != 'done',
                  Task.planned_start >= start_utc,
                  Task.planned_start <= end_utc)
          .all()
    )
    if todays:
        text2 = "🗓 *Задачи на сегодня:*\n" + format_tasks(todays)
    else:
        text2 = "🎉 Нет задач на сегодня."

    db.close()

    # Отправляем оба сообщения и завершаем
    asyncio.run(_send_messages(bot, [text1, text2]))


if __name__ == "__main__":
    main()

