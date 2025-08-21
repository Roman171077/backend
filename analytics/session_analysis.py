#analytics.session_analysis
from typing import List, Tuple
from datetime import timedelta
import logging

from sqlalchemy.orm import Session
from analytics.task_filter import haversine
from app.models import BeaconCoordinate, Task, GeofenceRule

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)  # включаем DEBUG для подробного вывода


def detect_stops_inline(
        coords_segment: List[BeaconCoordinate],
        min_points: int = 3,
        time_window: timedelta = timedelta(minutes=5)  # Увеличиваем время в пределах одного окна для фиксированных точек
) -> List[dict]:
    """
    Детектирует остановки в сегменте координат.
    Возвращает список словарей {start, end, duration, center}.
    Параметры cluster_radius и min_points можно настраивать.
    """
    stops = []
    current = {'coords': [], 'start': None}

    # Проходим по координатам
    for coord in coords_segment:
        t = coord.recorded_at
        if not current['coords']:
            current = {'coords': [coord], 'start': t}
        else:
            # Логирование времени между точками
           # log.debug(f"Checking coords {current['coords'][-1].recorded_at} → {t}")

            # Если координаты такие же, что и в предыдущий раз
            if current['coords'][-1].latitude == coord.latitude and current['coords'][-1].longitude == coord.longitude:
                # Проверяем, попадают ли записи в окно времени
                if (t - current['coords'][-1].recorded_at) <= time_window:
                    current['coords'].append(coord)
                else:
                    # Останавливаем кластер, если интервалы слишком большие
                    if len(current['coords']) >= min_points:
                        end = current['coords'][-1].recorded_at
                        duration = (end - current['start']).total_seconds() / 60
                        lat_c = sum(c.latitude for c in current['coords']) / len(current['coords'])
                        lon_c = sum(c.longitude for c in current['coords']) / len(current['coords'])
                        stops.append({
                            'start': current['start'],
                            'end': end,
                            'duration': duration,
                            'center': (lat_c, lon_c)
                        })
                    current = {'coords': [coord], 'start': t}
            else:
                # Когда координаты меняются, проверяем предыдущий кластер
                if len(current['coords']) >= min_points:
                    end = current['coords'][-1].recorded_at
                    duration = (end - current['start']).total_seconds() / 60
                    lat_c = sum(c.latitude for c in current['coords']) / len(current['coords'])
                    lon_c = sum(c.longitude for c in current['coords']) / len(current['coords'])
                    stops.append({
                        'start': current['start'],
                        'end': end,
                        'duration': duration,
                        'center': (lat_c, lon_c)
                    })
                current = {'coords': [coord], 'start': t}

    # Завершаем последний кластер
    if current['coords'] and len(current['coords']) >= min_points:
        end = current['coords'][-1].recorded_at
        duration = (end - current['start']).total_seconds() / 60
        lat_c = sum(c.latitude for c in current['coords']) / len(current['coords'])
        lon_c = sum(c.longitude for c in current['coords']) / len(current['coords'])
        stops.append({
            'start': current['start'],
            'end': end,
            'duration': duration,
            'center': (lat_c, lon_c)
        })

    return stops



def compute_task_and_idle_times_with_rules(
    db: Session,
    coords: List[BeaconCoordinate],
    tasks: List[Task]
) -> Tuple[int, int]:
    """
    Сначала находим все остановки по координатам, затем
    классифицируем каждую остановку как 'работа' или 'простой'.
    Возвращает (total_task_minutes, total_idle_minutes).
    """
    # 1. Загружаем правила геозон
    rules = db.query(GeofenceRule).order_by(GeofenceRule.radius_m).all()
    if not rules:
        log.warning("Нет geofence-правил в базе")
        return 0, 0

    max_conf = max(r.confidence for r in rules)

    # 2. Находим все остановки на входных координатах
    stops = detect_stops_inline(coords, min_points=3)
    if not stops:
        log.info("Остановок не найдено")
        return 0, 0

    log.info(f"Найдено {len(stops)} остановок")

    total_task = 0   # минуты «работы»
    total_idle = 0   # минуты «простоя»

    # 3. Для каждой остановки ищем лучшую пару (task, rule)
    for s in stops:
        center_lat, center_lng = s['center']
        duration_min = s['duration']

        best_score = 0.0     # максимальный raw-score
        best_percent = 0.0   # нормированный % (0–100)

        for task in tasks:
            for rule in rules:
                dist_m = haversine(center_lat, center_lng, task.lat, task.lng)
                if dist_m > rule.radius_m:
                    continue

                dwell_ratio = min(duration_min / rule.dwell_minutes, 1.0)
                score = dwell_ratio * rule.confidence
                percent = score / max_conf * 100.0

                if score > best_score:
                    best_score = score
                    best_percent = percent

        # 4. Классифицируем остановку
        if best_percent > 30.0:
            total_task += int(duration_min)
            log.debug(f"Остановка {s['start']}–{s['end']} → WORK ({best_percent:.1f} %)")
        else:
            total_idle += int(duration_min)
            log.debug(f"Остановка {s['start']}–{s['end']} → IDLE ({best_percent:.1f} %)")

    log.info(f"Total task time: {total_task} min")
    log.info(f"Total idle time: {total_idle} min")

    return total_task, total_idle
