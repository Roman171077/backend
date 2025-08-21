# app/beacon_updater.py — опрос маяка, запись в UTC, лог в IRKUTSK + старт/финиш дня с проверкой задач

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone, time
import zoneinfo

import requests
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from get_app_code import get_app_code
from get_app_token import get_app_token
from get_slid_user_token import get_slid_user_token
from get_slnet_token import get_slnet_token
from get_user_id import get_user_id

from app.db import SessionLocal
from app.crud import create_beacon_coordinate
from app.schemas import BeaconCoordinateCreate
from app.analytics_stream import rt_processor
from app.telegram_bot import send_to_telegram
# Новая импорт для отправки отчёта по задачам
from app.tasks import main as send_task_report

# ——————————————————————————————————————————————————————————————
# Логирование
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Таймзона для логов
IRKUTSK = zoneinfo.ZoneInfo("Asia/Irkutsk")

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

SL_APP_ID     = os.getenv("SL_APP_ID")
SL_APP_SECRET = os.getenv("SL_APP_SECRET")
SL_LOGIN      = os.getenv("SL_LOGIN")
SL_PASSWORD   = os.getenv("SL_PASSWORD")

if not all([SL_APP_ID, SL_APP_SECRET, SL_LOGIN, SL_PASSWORD]):
    raise RuntimeError(f"В {BASE_DIR/'.env'} должны быть SL_APP_ID, SL_APP_SECRET, SL_LOGIN и SL_PASSWORD")

# Кэш токена
_token_cache = {
    "slnet_token": None,
    "user_id":     None,
    "expires_at":  datetime.now(IRKUTSK)
}

# Для отслеживания начала/конца дня и времени последнего запуска
_last_work_date = None
_last_run_time  = None


def authorise_full() -> tuple[str, str]:
    code        = get_app_code(SL_APP_ID, SL_APP_SECRET)
    app_token   = get_app_token(SL_APP_ID, SL_APP_SECRET, code)
    slid_token  = get_slid_user_token(app_token, SL_LOGIN, SL_PASSWORD)
    slnet_token = get_slnet_token(slid_token)
    user_id     = get_user_id(slid_token)
    return slnet_token, user_id


def authorise_cached() -> tuple[str, str]:
    now = datetime.now(IRKUTSK)
    if _token_cache["slnet_token"] and now < _token_cache["expires_at"]:
        return _token_cache["slnet_token"], _token_cache["user_id"]
    slnet_token, user_id = authorise_full()
    _token_cache.update({
        "slnet_token": slnet_token,
        "user_id":     user_id,
        "expires_at":  now + timedelta(hours=24)
    })
    logger.info("🔑 Новый slnet_token до %s", _token_cache["expires_at"].isoformat())
    return slnet_token, user_id


def fetch_coordinates() -> tuple[float, float, int]:
    slnet_token, user_id = authorise_cached()
    url = f"https://developer.starline.ru/json/v2/user/{user_id}/user_info"
    resp = requests.get(url, headers={"Cookie": f"slnet={slnet_token}"}, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    devices = payload.get("devices") or []
    if not devices:
        raise RuntimeError("У пользователя нет устройств.")
    pos = devices[0].get("position", {})
    lon, lat, ts = pos.get("y"), pos.get("x"), pos.get("ts")
    if None in (lat, lon, ts):
        raise RuntimeError(f"Некорректные данные позиции: {pos}")
    return lat, lon, ts


def record_beacon_coordinate() -> None:
    global _last_work_date, _last_run_time

    now_local = datetime.now(IRKUTSK)

    try:
        # 1) Получаем свежие координаты
        lat, lon, ts_dev = fetch_coordinates()
        dt_utc   = datetime.fromtimestamp(ts_dev, tz=timezone.utc)
        dt_local = dt_utc.astimezone(IRKUTSK)

        today = dt_local.date()
        # Границы рабочего дня
        start_thresh = datetime.combine(today, time(8, 0), tzinfo=IRKUTSK)
        end_thresh   = datetime.combine(today, time(21, 59), tzinfo=IRKUTSK)

        # 2) Старт дня: пересечение порога 08:00
        if (_last_run_time is None or _last_run_time < start_thresh) \
           and dt_local >= start_thresh \
           and _last_work_date != today:

            _last_work_date = today
            found = rt_processor._find_zone(BeaconCoordinateCreate(
                latitude=lat, longitude=lon, recorded_at=dt_utc
            ))
            if found:
                send_to_telegram(f"🔔 Начало работы: автомобиль в зоне «{found[1]}»")
            else:
                send_to_telegram("🔔 Начало работы: автомобиль в пути")

            # После уведомления о старте рабочего дня отправляем отчёт по задачам
            try:
                send_task_report()
            except Exception as err:
                logger.error("❌ Ошибка при отправке отчёта по задачам: %s", err, exc_info=True)

        # 3) Сохраняем в БД
        coord_in = BeaconCoordinateCreate(
            latitude=lat, longitude=lon, recorded_at=dt_utc
        )
        db: Session = SessionLocal()
        try:
            db_coord = create_beacon_coordinate(db, coord_in)
        finally:
            db.close()
        logger.info("✅ [%s] Сохранено: device_time=%s, lat=%.6f, lon=%.6f",
                    now_local.isoformat(), dt_local.isoformat(), lat, lon)

        # 4) Real-time аналитика (въезд/выезд/стопы)
        try:
            rt_processor.process(db_coord)
        except Exception as err:
            logger.error("❌ [RT] Ошибка обработки: %s", err, exc_info=True)

        # 5) Финиш дня: пересечение порога 21:59
        if (_last_run_time is None or _last_run_time < end_thresh) \
           and dt_local >= end_thresh:

            found = rt_processor._find_zone(db_coord)
            if found:
                send_to_telegram(f"🔔 Конец работы: автомобиль завершил день в зоне «{found[1]}»")
            else:
                send_to_telegram("🔔 Конец работы: автомобиль завершил день в пути")

        # 6) Обновляем отметку последнего запуска
        _last_run_time = dt_local

    except Exception as e:
        logger.error("❌ [%s] Ошибка записи: %s", now_local.isoformat(), e, exc_info=True)


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=IRKUTSK)
    # Запуск каждую минуту с 00:00 до 21:59 локального времени
    trigger   = CronTrigger(minute="*", hour="8-21", timezone=IRKUTSK)
    scheduler.add_job(record_beacon_coordinate, trigger, id="beacon_log")
    logger.info("🕑 Сервис запущен: запись координат каждую минуту (08–22 Irkutsk)")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Останавливаем сервис…")

