# app/analytics.py
import math
import logging
import json
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models import BeaconCoordinate, GeoZone
import app.crud as crud
import app.schemas as schemas
from app.visit_analysis import analyze_session
from app.detect_stops import detect_stops
from app.telegram_bot import send_to_telegram

# ——————————————————————————————————————————————————————————————
# Логирование
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ——————————————————————————————————————————————————————————————
# Часовые пояса
UTC = timezone.utc
IRKUTSK = ZoneInfo('Asia/Irkutsk')

def format_dt_to_irkutsk(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IRKUTSK).strftime('%Y-%m-%d %H:%M:%S')

# ——————————————————————————————————————————————————————————————
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ/2)**2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ——————————————————————————————————————————————————————————————
def find_zone(pt, zone_defs):
    for zid, zname, cz_lat, cz_lon, cz_r, ztype in zone_defs:
        if haversine(pt.latitude, pt.longitude, cz_lat, cz_lon) <= cz_r:
            return zid, zname, cz_lat, cz_lon, cz_r, ztype
    return None

# ——————————————————————————————————————————————————————————————
def main():
    db: Session = SessionLocal()
    try:
        target_date = date(2025, 5, 13)
        start = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=UTC)
        end = start + timedelta(days=1)
        logger.info(f"Loading coords from {start} to {end} UTC")

        coords = (
            db.query(BeaconCoordinate)
              .filter(
                  BeaconCoordinate.recorded_at >= start,
                  BeaconCoordinate.recorded_at < end
              )
              .order_by(BeaconCoordinate.recorded_at)
              .all()
        )
        logger.info(f"Total points loaded: {len(coords)}")
        if not coords:
            logger.info("No coordinates for the target date, exiting.")
            return

        zones = db.query(GeoZone).all()
        zone_defs = [(z.zone_id, z.name, z.center_lat, z.center_lon, z.radius_m, z.type) for z in zones]
        logger.info(f"Loaded {len(zone_defs)} geozones")

        # Начальное состояние
        first_pt = coords[0]
        first_time = first_pt.recorded_at
        current_zone = find_zone(first_pt, zone_defs)
        state = 'zone' if current_zone else 'travel'
        travel_start_idx = travel_start_time = None
        zone_session_id = None
        zone_type = None

        if state == 'zone':
            zid, zname, *_ , ztype = current_zone
            zone_type = ztype
            if ztype == 'territory':
                try:
                    sess = crud.create_geozone_session(
                        db, schemas.GeozoneSessionCreate(
                            zone_id=zid,
                            entry_time=first_time,
                            exit_time=None,
                            entry_lat=first_pt.latitude,
                            entry_lon=first_pt.longitude,
                            exit_lat=None,
                            exit_lon=None,
                            status="open"
                        )
                    )
                    zone_session_id = sess.session_id
                    logger.info(f"Opened territory session {zone_session_id} at {first_time}")
                except Exception as err:
                    db.rollback()
                    logger.error("Ошибка при открытии первой сессии %s: %s", zid, err, exc_info=True)
            send_to_telegram(f"🚗 Въезд в зону «{zname}» в {format_dt_to_irkutsk(first_time)}")
        else:
            travel_start_idx = 0
            travel_start_time = first_time
            send_to_telegram(f"🚗 Начало движения в {format_dt_to_irkutsk(first_time)}")

        # Основной цикл
        n = len(coords)
        for i in range(1, n):
            pt = coords[i]
            t_utc = pt.recorded_at
            current = find_zone(pt, zone_defs)

            if state == 'zone' and current is None:
                exit_time = t_utc
                if zone_type == 'territory':
                    try:
                        crud.close_geozone_session(
                            db, session_id=zone_session_id,
                            exit_time=exit_time,
                            exit_lat=pt.latitude,
                            exit_lon=pt.longitude
                        )
                        analyze_session(db, zone_session_id)
                    except Exception as err:
                        db.rollback()
                        logger.error("Ошибка при закрытии сессии %s: %s", zone_session_id, err, exc_info=True)
                state = 'travel'
                travel_start_idx = i
                travel_start_time = exit_time
                send_to_telegram(f"🚗 Автомобиль выехал из зоны в {format_dt_to_irkutsk(exit_time)}")

            elif state == 'travel' and current:
                segment = coords[travel_start_idx:i+1]
                logger.info(f"Closing travel [{travel_start_time} → {t_utc}], points {len(segment)}")
                detect_stops(segment)
                zid, zname, *_ , ztype = current
                zone_type = ztype
                if ztype == 'territory':
                    try:
                        sess = crud.create_geozone_session(
                            db, schemas.GeozoneSessionCreate(
                                zone_id=zid,
                                entry_time=t_utc,
                                exit_time=None,
                                entry_lat=pt.latitude,
                                entry_lon=pt.longitude,
                                exit_lat=None,
                                exit_lon=None,
                                status="open"
                            )
                        )
                        zone_session_id = sess.session_id
                        logger.info(f"Opened territory session {zone_session_id} at {t_utc}")
                    except Exception as err:
                        db.rollback()
                        logger.error("Ошибка при открытии сессии в зоне %s: %s", zname, err, exc_info=True)
                send_to_telegram(f"🚗 Въезд в зону «{zname}» в {format_dt_to_irkutsk(t_utc)}")
                state = 'zone'

        # Закрытие последней сессии
        last_pt = coords[-1]
        last_time = last_pt.recorded_at
        if state == 'zone' and zone_type == 'territory':
            try:
                crud.close_geozone_session(
                    db, session_id=zone_session_id,
                    exit_time=last_time,
                    exit_lat=last_pt.latitude,
                    exit_lon=last_pt.longitude
                )
                analyze_session(db, zone_session_id)
                send_to_telegram(f"🚗 Выезд из зоны в {format_dt_to_irkutsk(last_time)}")
            except Exception as err:
                db.rollback()
                logger.error("Ошибка при финальном закрытии сессии %s: %s", zone_session_id, err, exc_info=True)

        elif state == 'travel':
            segment = coords[travel_start_idx:]
            logger.info(f"Closing final travel [{travel_start_time} → {last_time}], points {len(segment)}")
            detect_stops(segment)

        logger.info("=== ANALYTICS COMPLETED ===")

    finally:
        db.close()

if __name__ == "__main__":
    main()
