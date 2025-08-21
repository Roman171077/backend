# app/components/task_filter.py
from datetime import date
from typing import List, Optional

from app.models import Task, GeoZone
import math

# Constants for zone and task proximity (метры)
# Можем вынести в константы, если потребуется


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Вычисляет расстояние между двумя точками по Haversine (в метрах).
    """
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def filter_tasks_for_zone(
    tasks: List[Task],
    zone: GeoZone,
    target_date: date
) -> List[Task]:
    """
    Фильтрует задачи по трём условиям относительно переданной зоны и даты анализа:
      1) t.created_at.date() <= target_date
      2) t.actual_end is None or t.actual_end.date() >= target_date
      3) задача находится внутри зоны (расстояние до центра <= zone.radius_m)

    :param tasks: список всех задач из БД
    :param zone: объект GeoZone с атрибутами center_lat, center_lon, radius_m
    :param target_date: дата анализа (date)
    :return: список задач, соответствующих условиям
    """
    filtered: List[Task] = []

    for t in tasks:
        # 1) создана до или в день target_date
        if t.created_at.date() > target_date:
            continue

        # 2) не завершена раньше target_date
        if t.actual_end and t.actual_end.date() < target_date:
            continue

        # 3) попадает в зону по географии
        distance = haversine(
            t.lat, t.lng,
            zone.center_lat, zone.center_lon
        )
        if distance <= zone.radius_m:
            filtered.append(t)

    return filtered
