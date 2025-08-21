# analytics_simple.py

import math
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models import BeaconCoordinate, GeoZone, Task, DailyZoneStatistics

from analytics.task_filter import filter_tasks_for_zone
from analytics.session_analysis import compute_task_and_idle_times_with_rules
from analytics.path_analysis import load_service_tasks_for_date, detect_travel_stops

# ——————————————————————————————————————————————————————————————
# Logging configuration
def configure_logging():
    logging.basicConfig(format='%(message)s', level=logging.INFO)


# ——————————————————————————————————————————————————————————————
# Timezones
UTC = timezone.utc
IRKUTSK = ZoneInfo("Asia/Irkutsk")


# ——————————————————————————————————————————————————————————————
# Thresholds
MOVEMENT_THRESHOLD_M = 20.0
STOP_RADIUS_M = 20.0  # same as in path_analysis for consistency


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance between two points in meters by the Haversine formula."""
    R = 6371000.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_zone(pt: BeaconCoordinate, zone_defs):
    """Return (zone_id, name, type) if pt inside any zone, else None."""
    for zid, zname, cz_lat, cz_lon, cz_r, ztype in zone_defs:
        if haversine(pt.latitude, pt.longitude, cz_lat, cz_lon) <= cz_r:
            return zid, zname, ztype
    return None


def detect_first_movement_index(coords):
    """Find index of first movement > MOVEMENT_THRESHOLD_M."""
    for i in range(1, len(coords)):
        d = haversine(
            coords[i - 1].latitude,
            coords[i - 1].longitude,
            coords[i].latitude,
            coords[i].longitude,
        )
        if d > MOVEMENT_THRESHOLD_M:
            return i - 1
    return 0


def main():
    configure_logging()
    log = logging.getLogger(__name__)

    # 1) Analysis parameters
    target_date = date(2025, 5, 20)
    start_day = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=UTC)
    end_day = start_day + timedelta(hours=14)
    log.info(
        f"Анализ координат с {start_day.astimezone(IRKUTSK)} "
        f"до {end_day.astimezone(IRKUTSK)}"
    )

    db: Session = SessionLocal()
    try:
        # 2) Load coordinates
        coords_all = (
            db.query(BeaconCoordinate)
            .filter(
                BeaconCoordinate.recorded_at >= start_day,
                BeaconCoordinate.recorded_at < end_day,
            )
            .order_by(BeaconCoordinate.recorded_at)
            .all()
        )
        if not coords_all:
            log.info("Нет координат за указанный период.")
            return

        # 2.1) Detect real movement start
        move_start_idx = detect_first_movement_index(coords_all)
        coords = coords_all[move_start_idx:]
        if not coords:
            log.info("После детекции движения не осталось координат.")
            return

        if move_start_idx > 0:
            log.info(
                f"Начальные статичные точки отброшены: {move_start_idx}. "
                f"Первое движение в {coords[0].recorded_at.astimezone(IRKUTSK)}"
            )

        zones = db.query(GeoZone).all()
        tasks_all = db.query(Task).all()
        service_tasks = load_service_tasks_for_date(db, target_date)

        zone_defs = [
            (z.zone_id, z.name, z.center_lat, z.center_lon, z.radius_m, z.type)
            for z in zones
        ]

        # 3) Segment into travel / zone sessions
        sessions = []
        first_work_time = coords[0].recorded_at
        state = None
        current_session = None

        def close_session(end_idx: int):
            if current_session is None:
                return
            current_session['end_idx'] = end_idx
            current_session['end'] = coords[end_idx].recorded_at
            sessions.append(current_session.copy())

        # 3.1) Initialize first session
        first_pt = coords[0]
        zinfo = find_zone(first_pt, zone_defs)
        if zinfo:
            zid, zname, ztype = zinfo
            state = 'zone'
            current_session = {
                'type': 'zone',
                'start': first_pt.recorded_at,
                'start_idx': 0,
                'zone_id': zid,
                'zone_name': zname,
                'zone_type': ztype,
            }
        else:
            state = 'travel'
            current_session = {
                'type': 'travel',
                'start': first_pt.recorded_at,
                'start_idx': 0,
            }

        # 3.2) Process remaining points
        for idx, pt in enumerate(coords[1:], start=1):
            zinfo = find_zone(pt, zone_defs)
            if state == 'travel':
                if zinfo:
                    close_session(idx - 1)
                    zid, zname, ztype = zinfo
                    current_session = {
                        'type': 'zone',
                        'start': pt.recorded_at,
                        'start_idx': idx,
                        'zone_id': zid,
                        'zone_name': zname,
                        'zone_type': ztype,
                    }
                    state = 'zone'
            else:  # state == 'zone'
                if not zinfo:
                    close_session(idx - 1)
                    current_session = {
                        'type': 'travel',
                        'start': pt.recorded_at,
                        'start_idx': idx,
                    }
                    state = 'travel'
                else:
                    zid_new, _, _ = zinfo
                    if zid_new != current_session['zone_id']:
                        close_session(idx - 1)
                        zid, zname, ztype = zinfo
                        current_session = {
                            'type': 'zone',
                            'start': pt.recorded_at,
                            'start_idx': idx,
                            'zone_id': zid,
                            'zone_name': zname,
                            'zone_type': ztype,
                        }
        close_session(len(coords) - 1)

        # 4) End of work time = start of last session
        end_work = sessions[-1]['start'] if sessions else first_work_time
        log.info(f"Начало работы {first_work_time.astimezone(IRKUTSK)}")
        log.info(f"Конец работы  {end_work.astimezone(IRKUTSK)}")

        # 5) Compute and bulk-insert statistics
        stats_rows = []
        last_idx = len(sessions) - 1

        for idx, s in enumerate(sessions):
            sess_start = s['start'] if idx > 0 else first_work_time
            sess_end = s['end'] if idx < last_idx else end_work

            dur_min = int((sess_end - sess_start).total_seconds() / 60)
            if dur_min <= 0:
                zone_val = s.get('zone_id', 0) if s['type'] == 'zone' else 0
                stats_rows.append(
                    DailyZoneStatistics(
                        zone_id=zone_val,
                        stats_datetime=start_day,
                        start_time=sess_start,
                        end_time=sess_end,
                        work_minutes=0,
                        stop_minutes=0,
                        travel_minutes=0,
                    )
                )
                continue

            pts = coords[s['start_idx'] : s['end_idx'] + 1]
            if s['type'] == 'travel':
                zone_val = 0
                serv_stops, idle_stops = detect_travel_stops(pts, service_tasks)
                work_min = int(sum(st['duration'] for st in serv_stops))
                stop_min = int(sum(st['duration'] for st in idle_stops))
                travel_min = max(dur_min - work_min - stop_min, 0)
            else:
                # Zone session
                zone_val = s['zone_id']
                zone_obj = next((z for z in zones if z.zone_id == zone_val), None)
                tasks_in_zone = (
                    filter_tasks_for_zone(tasks_all, zone_obj, target_date)
                    if zone_obj
                    else []
                )

                # Filter incident tasks planned for target_date
                incident_tasks_today = [
                    t for t in tasks_in_zone
                    if t.type == 'incident'
                    and t.planned_start.date() == target_date
                ]
                # If there is an incident and duration > 60 min, count all as work
                if incident_tasks_today and dur_min > 60:
                    work_min = dur_min
                    stop_min = 0
                    travel_min = 0
                else:
                    work_min, stop_min = compute_task_and_idle_times_with_rules(
                        db, pts, tasks_in_zone
                    )
                    travel_min = max(dur_min - work_min - stop_min, 0)

            log.info(
                f"✏️ zone={zone_val} | {sess_start.astimezone(IRKUTSK)} → "
                f"{sess_end.astimezone(IRKUTSK)} | work={work_min}m "
                f"stop={stop_min}m travel={travel_min}m"
            )

            stats_rows.append(
                DailyZoneStatistics(
                    zone_id=zone_val,
                    stats_datetime=start_day,
                    start_time=sess_start,
                    end_time=sess_end,
                    work_minutes=work_min,
                    stop_minutes=stop_min,
                    travel_minutes=travel_min,
                )
            )

        log.info("Начало записи статистики в БД")
        db.add_all(stats_rows)
        db.commit()
        log.info("✅ Запись статистики завершена")
        log.info("Анализ завершён")

    finally:
        db.close()


if __name__ == "__main__":
    main()
