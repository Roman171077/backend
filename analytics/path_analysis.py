# analytics.path_analysis

import math
import logging
from datetime import datetime, timedelta, date, time
from typing import List, Tuple, Optional

from sqlalchemy.orm import Session
from app.models import Task, BeaconCoordinate

# ——————————————————————————————————————————————————————————————
logger = logging.getLogger(__name__)

# ——————————————————————————————————————————————————————————————
# Константы (можно потом вынести в конфиг)
SERVICE_TASK_RADIUS       = 100.0   # м – радиус привязки «сервисной» остановки к задаче
STOP_RADIUS               = 20.0    # м – радиус кластеризации для любой остановки
MIN_STOP_DURATION_MINUTES = 10      # мин – минимальная длительность остановки
MIN_STOP_POINTS           = MIN_STOP_DURATION_MINUTES  # по одной точке в минуту
SPIKE_THRESHOLD           = 1100.0  # м – порог «скачка» GPS, выше которого не считаем простоями

# ——————————————————————————————————————————————————————————————

def load_service_tasks_for_date(
    db: Session,
    target_date: date
) -> List[Task]:
    """
    Загружает из БД задачи типа 'service', у которых planned_start
    попадает в указанный target_date (00:00–24:00 UTC).
    """
    start_dt = datetime.combine(target_date, time.min).replace(tzinfo=datetime.utcnow().tzinfo)
    end_dt   = start_dt + timedelta(days=1)

    tasks = (
        db.query(Task)
          .filter(
              Task.type == 'service',
              Task.planned_start >= start_dt,
              Task.planned_start <  end_dt
          )
          .all()
    )
    return tasks

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Расстояние по поверхности Земли (формула Haversine, в метрах).
    """
    R = 6371000.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ     = math.radians(lat2 - lat1)
    dλ     = math.radians(lon2 - lon1)
    a = math.sin(dφ/2)**2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def detect_travel_stops(
    coords: List[BeaconCoordinate],
    service_tasks: List[Task],
    service_radius: float = SERVICE_TASK_RADIUS,
    stop_radius:    float = STOP_RADIUS,
    min_points:     int   = MIN_STOP_POINTS,
    spike_threshold: float = SPIKE_THRESHOLD
) -> Tuple[List[dict], List[dict]]:
    """
    Находит и классифицирует остановки в пути:
      - 'service' (центр остановки попадает в радиус service_radius вокруг задачи)
      - 'idle'    (остановка без связи с задачами)
    При этом простои (idle), где между концом кластеризованной остановки
    и следующей точкой координат происходит «скачок» > spike_threshold,
    отбрасываются как ложные (т.е. считаются продолжением пути).
    """
    service_stops: List[dict] = []
    idle_stops:    List[dict] = []

    def build_stop(cluster: dict) -> dict:
        pts = cluster['coords']
        start = cluster['start']
        end   = pts[-1].recorded_at
        duration = (end - start).total_seconds() / 60
        lat_c = sum(p.latitude  for p in pts) / len(pts)
        lon_c = sum(p.longitude for p in pts) / len(pts)
        return {
            'start': start,
            'end': end,
            'duration': duration,
            'center': (lat_c, lon_c)
        }

    def classify(stop: dict) -> str:
        lat_c, lon_c = stop['center']
        nearby = [
            t for t in service_tasks
            if haversine(lat_c, lon_c, t.latitude, t.longitude) <= service_radius
        ]
        return 'service' if nearby else 'idle'

    if not coords:
        return service_stops, idle_stops

    cluster = {'coords': [], 'start': None}

    for idx, pt in enumerate(coords):
        t = pt.recorded_at

        # Начинаем кластер, если пуст
        if not cluster['coords']:
            cluster = {'coords': [pt], 'start': t}
            continue

        # Расстояние до первой точки кластера
        first = cluster['coords'][0]
        d_to_first = haversine(pt.latitude, pt.longitude, first.latitude, first.longitude)

        if d_to_first <= stop_radius:
            # Всё ещё в пределах остановки
            cluster['coords'].append(pt)
        else:
            # Выходим из радиуса — потенциально завершается остановка
            if len(cluster['coords']) >= min_points:
                # Проверяем «скачок» GPS между концом остановки и этой точкой
                last_pt = cluster['coords'][-1]
                d_spike = haversine(
                    last_pt.latitude, last_pt.longitude,
                    pt.latitude,     pt.longitude
                )
                if d_spike <= spike_threshold:
                    # Это настоящая остановка
                    stop = build_stop(cluster)
                    typ = classify(stop)
                    stop['type'] = typ
                    if typ == 'service':
                        service_stops.append(stop)
                    else:
                        idle_stops.append(stop)
                else:
                    logger.info(
                        f"[DEBUG] Пропущён idle: {last_pt.recorded_at} → {pt.recorded_at}, "
                        f"дистанция {d_spike:.0f}м > порог {spike_threshold:.0f}м"
                    )
            # Начинаем новый кластер с текущей точки
            cluster = {'coords': [pt], 'start': t}

    # Обработка последнего кластера
    if len(cluster['coords']) >= min_points:
        stop = build_stop(cluster)
        # Для последнего кластера нет «следующей» точки, считаем его валидным
        typ = classify(stop)
        stop['type'] = typ
        if typ == 'service':
            service_stops.append(stop)
        else:
            idle_stops.append(stop)

    return service_stops, idle_stops
