# territory_analysis.py

import logging
from math import radians, sin, cos, sqrt, atan2
from sqlalchemy.orm import Session
from datetime import datetime
from app.db import SessionLocal
from app.models import GeoZone, Task

# ——— Logging —————————————————————————————————————————————————
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ——— Haversine для метрик расстояний —————————————————————————
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    φ1, φ2 = radians(lat1), radians(lat2)
    dφ = radians(lat2 - lat1)
    dλ = radians(lon2 - lon1)
    a = sin(dφ/2)**2 + cos(φ1)*cos(φ2)*sin(dλ/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

# ——— Основная точечная функция —————————————————————————————
def analyze_tasks_in_zone(zone_id: int, start_dt: datetime, end_dt: datetime):
    """
    Для одной сессии territory:
    - zone_id:   PK зоны
    - start_dt:  начало сессии в зоне
    - end_dt:    конец сессии в зоне
    """
    db: Session = SessionLocal()
    try:
        zone = db.query(GeoZone).filter(GeoZone.zone_id==zone_id).first()
        if not zone:
            logger.warning(f"Zone {zone_id} не найдена.")
            return

        logger.info(f"--- Задачи внутри '{zone.name}' (territory) с {start_dt} по {end_dt}:")

        # Берем все задачи (или можно отфильтровать по дате создания, если нужно)
        tasks = db.query(Task).all()
        found = 0
        for t in tasks:
            # считаем расстояние до центра зоны
            dist = haversine(t.lat, t.lng, zone.center_lat, zone.center_lon)
            if dist <= zone.radius_m:
                found += 1
                logger.info(
                    f"Task {t.task_id}: '{t.address_raw}', "
                    f"коорд=({t.lat:.5f},{t.lng:.5f}), dist={int(dist)}м"
                )
        if found == 0:
            logger.info("Ни одной задачи не обнаружено в пределах зоны.")
    finally:
        db.close()
