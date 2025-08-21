#!/usr/bin/env python3
# test_zone_detection.py

import logging
from math import radians, sin, cos, asin, sqrt

from app.db import SessionLocal
from app.crud import get_latest_beacon_coordinate, get_all_geozones

# ——— Конфигурация логирования —————————————————————————
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG  # DEBUG-уровень, чтобы видеть все подробности
)
logger = logging.getLogger(__name__)


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Расстояние между двумя точками на земном шаре по формуле Хаверсин.
    Возвращает метры.
    """
    R = 6371000  # радиус Земли в метрах
    φ1, λ1, φ2, λ2 = map(radians, (lat1, lon1, lat2, lon2))
    dφ = φ2 - φ1
    dλ = λ2 - λ1
    a = sin(dφ / 2)**2 + cos(φ1) * cos(φ2) * sin(dλ / 2)**2
    c = 2 * asin(sqrt(a))
    return R * c


def detect_current_zone():
    db = SessionLocal()
    try:
        coord = get_latest_beacon_coordinate(db)
        if not coord:
            logger.warning("Нет записей в beacon_coordinates")
            return

        # Логируем последнюю координату
        logger.info(
            "Получена последняя координата: "
            f"lat={coord.latitude:.6f}, lon={coord.longitude:.6f}, "
            f"recorded_at={coord.recorded_at}"
        )

        zones = get_all_geozones(db)
        if not zones:
            logger.warning("Таблица geo_zones пуста")
            return

        for zone in zones:
            # Логируем данные зоны
            logger.debug(
                "Зона '%s' (type=%s): центр=(%.6f, %.6f), радиус=%d м",
                zone.name, zone.type,
                zone.center_lat, zone.center_lon,
                zone.radius_m
            )

            # Считаем дистанцию
            dist = haversine(
                coord.latitude, coord.longitude,
                zone.center_lat, zone.center_lon
            )
            # Логируем расстояние
            logger.debug("Расстояние до зоны '%s': %.1f м", zone.name, dist)

            if dist <= zone.radius_m:
                logger.info(
                    "✅ Транспорт находится в зоне «%s» "
                    "(type=%s), расстояние до центра: %.1f м",
                    zone.name, zone.type, dist
                )
                break
        else:
            logger.info("ℹ️ Транспорт ни в одну геозону не попал")

    finally:
        db.close()


if __name__ == "__main__":
    detect_current_zone()
