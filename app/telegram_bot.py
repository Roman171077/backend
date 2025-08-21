import os
from dotenv import load_dotenv
import requests
import logging
from typing import Optional
from datetime import datetime
from app import models  # Импортируем модели для работы с таблицей TelegramMessage
from app.db import SessionLocal  # Импортируем сессию для взаимодействия с базой данных
# Загружаем переменные окружения из файла .env
load_dotenv()

# Конфигурация
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')

# Проверка, что переменные загружены
if not TELEGRAM_TOKEN or not CHAT_ID or not YANDEX_API_KEY:
    raise ValueError("Не все ключи были загружены из .env")

# Функция для отправки сообщения
def send_to_telegram(message: str) -> Optional[str]:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,  # Используем глобальный CHAT_ID
        "text": message,
        "parse_mode": "Markdown"
    }

    # Создаем сессию для работы с базой данных
    db = SessionLocal()

    try:
        # Отправляем сообщение в Telegram
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Проверка на ошибки
        response_data = response.json()
        telegram_message_id = response_data['result']['message_id']  # Получаем ID сообщения

        # Сохраняем сообщение в базе данных
        db_message = models.TelegramMessage(
            chat_id=CHAT_ID,  # Сохраняем идентификатор чата
            message_text=message,
            telegram_message_id=str(telegram_message_id)
        )
        db.add(db_message)
        db.commit()
        db.refresh(db_message)

        return str(telegram_message_id)  # Возвращаем ID сообщения для дальнейших операций

    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending message to Telegram: {e}")
        return None

    finally:
        db.close()  # Закрываем сессию после выполнения операций с базой данных


# Функция для отправки информации о сессии
def send_session_info(session_id: int, entry_time: datetime, exit_time: datetime, zone_name: str, time_spent: float):
    message = (
        f"🚗 **Сессия {session_id}**\n"
        f"Зона: {zone_name}\n"
        f"Время входа: {entry_time}\n"
        f"Время выхода: {exit_time}\n"
        f"Время в зоне: {time_spent:.2f} минут."
    )
    send_to_telegram(message)

# Функция для отправки данных о заявке
def send_task_info(task_id: int, address: str, arrival_time: datetime, departure_time: datetime, time_spent: float, confidence: float):
    message = (
        f"📝 **Задача {task_id}**\n"
        f"Адрес: {address}\n"
        f"Время прибытия: {arrival_time}\n"
        f"Время убытия: {departure_time}\n"
        f"Время на задаче: {time_spent:.2f} минут\n"
        f"Достоверность: {confidence}%"
    )
    send_to_telegram(message)

# Функция для отправки данных о остановке
def send_stop_info(stop_id: int, start_time: datetime, end_time: datetime, duration: float, coordinates: tuple):
    message = (
        f"⏱ **Остановка {stop_id}**\n"
        f"Время начала: {start_time}\n"
        f"Время окончания: {end_time}\n"
        f"Продолжительность: {duration:.2f} минут\n"
        f"Координаты: {coordinates}"
    )
    send_to_telegram(message)

def delete_telegram_message(telegram_message_id: str) -> bool:
    """Удаляет сообщение из чата Telegram по message_id"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessage"
    payload = {
        "chat_id": CHAT_ID,  # Используем глобальный CHAT_ID
        "message_id": telegram_message_id
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Error deleting message from Telegram: {e}")
        return False
