import logging
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from app.db import SessionLocal
from app.models import DailyZoneStatistics

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ——————————————————————————————————————
DATE_FROM = date(2025, 5, 13)
DATE_TO   = date(2025, 5, 16)    # включительно
# ——————————————————————————————————————

def minutes_to_hours(value) -> float:
    """Перевод минут (int | Decimal) в часы с округлением до 0.1."""
    if value is None:
        return 0.0
    return round(float(value) / 60.0, 1)

def main() -> None:
    db: Session = SessionLocal()
    try:
        cur = DATE_FROM
        one_day = timedelta(days=1)

        while cur <= DATE_TO:
            next_day = cur + one_day

            day_start, day_end, work_m, stop_m, travel_m = (
                db.query(
                    func.min(DailyZoneStatistics.start_time),
                    func.max(DailyZoneStatistics.end_time),
                    func.coalesce(func.sum(DailyZoneStatistics.work_minutes),   0),
                    func.coalesce(func.sum(DailyZoneStatistics.stop_minutes),   0),
                    func.coalesce(func.sum(DailyZoneStatistics.travel_minutes), 0),
                )
                .filter(
                    and_(
                        DailyZoneStatistics.stats_datetime >= cur,
                        DailyZoneStatistics.stats_datetime <  next_day,
                    )
                )
                .one()
            )

            if day_start is None:
                log.info(f"{cur} — данных нет")
            else:
                shift_h  = round((day_end - day_start).total_seconds() / 3600, 1)
                work_h   = minutes_to_hours(work_m)
                stop_h   = minutes_to_hours(stop_m)
                travel_h = minutes_to_hours(travel_m)

                log.info(
                    f"{cur} | старт: {day_start.time()} | финиш: {day_end.time()} | "
                    f"смена: {shift_h:4.1f} ч | "
                    f"работа: {work_h:4.1f} ч | простой: {stop_h:4.1f} ч | путь: {travel_h:4.1f} ч"
                )

            cur = next_day

    finally:
        db.close()

if __name__ == "__main__":
    main()
