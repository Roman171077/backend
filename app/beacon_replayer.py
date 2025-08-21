# app/beacon_replayer.py — проигрывание координат из БД для отладки
import logging
from pathlib import Path
from datetime import datetime, time, date
import time as time_module
import zoneinfo
from typing import Iterable

from dotenv import load_dotenv
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.crud import get_beacon_coords_by_day
from app.analytics_stream import rt_processor
from app.telegram_bot import send_to_telegram
from app.tasks import main as send_task_report
from app.models import BeaconCoordinate

# ——————————————————————————————————————————————————————————————
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

IRKUTSK = zoneinfo.ZoneInfo("Asia/Irkutsk")

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

_last_work_date: date | None = None
_last_run_time: datetime | None = None

# Дата, за которую будет проигрываться трек
REPLAY_DATE = date(2025, 8, 18)
# Необязательная задержка между координатами (в секундах)
REPLAY_DELAY = 0.0


def _process_coordinate(coord: BeaconCoordinate) -> None:
    global _last_work_date, _last_run_time

    dt_utc = coord.recorded_at
    dt_local = dt_utc.astimezone(IRKUTSK)

    today = dt_local.date()
    start_thresh = datetime.combine(today, time(8, 0), tzinfo=IRKUTSK)
    end_thresh = datetime.combine(today, time(21, 59), tzinfo=IRKUTSK)

    if (
        (_last_run_time is None or _last_run_time < start_thresh)
        and dt_local >= start_thresh
        and _last_work_date != today
    ):
        _last_work_date = today
        found = rt_processor._find_zone(coord)
        if found:
            send_to_telegram(
                f"🔔 Начало работы: автомобиль в зоне «{found[1]}»"
            )
        else:
            send_to_telegram("🔔 Начало работы: автомобиль в пути")
        try:
            send_task_report()
        except Exception as err:  # pragma: no cover - отладочный модуль
            logger.error("❌ Ошибка при отправке отчёта по задачам: %s", err, exc_info=True)

    try:
        rt_processor.process(coord)
    except Exception as err:  # pragma: no cover - отладочный модуль
        logger.error("❌ [RT] Ошибка обработки: %s", err, exc_info=True)

    if (
        (_last_run_time is None or _last_run_time < end_thresh)
        and dt_local >= end_thresh
    ):
        found = rt_processor._find_zone(coord)
        if found:
            send_to_telegram(
                f"🔔 Конец работы: автомобиль завершил день в зоне «{found[1]}»"
            )
        else:
            send_to_telegram(
                "🔔 Конец работы: автомобиль завершил день в пути"
            )

    _last_run_time = dt_local


def replay(coords: Iterable[BeaconCoordinate], delay: float = 0.0) -> None:
    for coord in coords:
        _process_coordinate(coord)
        if delay:
            time_module.sleep(delay)


def main(day: date, delay: float = 0.0) -> None:
    db: Session = SessionLocal()
    try:
        coords = get_beacon_coords_by_day(db, day)
    finally:
        db.close()

    logger.info("Загружено %d координат за %s", len(coords), day.isoformat())
    replay(coords, delay)


if __name__ == "__main__":
    main(REPLAY_DATE, REPLAY_DELAY)
