
# app/detect_stops.py
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Any

from app.models import BeaconCoordinate
from app.telegram_bot import send_to_telegram  # функция отправки сообщений
from app.visit_analysis import get_address_from_coordinates  # функция геокодирования

# Настройки детекции стоянок
CLUSTER_RADIUS = 5         # метров
MIN_POINTS = 10            # минимум точек для кластера

# Часовые пояса для форматирования времени
UTC = timezone.utc
IRKUTSK = ZoneInfo('Asia/Irkutsk')

logger = logging.getLogger(__name__)


def format_dt_to_irkutsk(dt: datetime) -> str:
    """
    Форматирует UTC-время в локальное по Иркутску.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IRKUTSK).strftime('%Y-%m-%d %H:%M:%S')


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Вычисляет расстояние между двумя точками в метрах по формуле Хаверсина.
    """
    import math
    R = 6371000.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ/2)**2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_stops(
    coords_segment: List[BeaconCoordinate]
) -> None:
    """
    Детектирует стоянки в отрезке координат и отправляет их в Telegram.

    :param coords_segment: список объектов BeaconCoordinate, упорядоченных по времени
    """
    stops: List[Dict[str, Any]] = []
    current = {'coords': [], 'start': None}

    for coord in coords_segment:
        t = coord.recorded_at
        if not current['coords']:
            # Начинаем новый кластер
            current['coords'] = [coord]
            current['start'] = t
        else:
            first = current['coords'][0]
            dist = haversine(
                coord.latitude, coord.longitude,
                first.latitude, first.longitude
            )
            if dist <= CLUSTER_RADIUS:
                # Добавляем в текущий кластер
                current['coords'].append(coord)
            else:
                # Завершаем предыдущий кластер
                if len(current['coords']) >= MIN_POINTS:
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
                # Начинаем новый кластер
                current['coords'] = [coord]
                current['start'] = t

    # Обработка последнего кластера
    if current['coords'] and len(current['coords']) >= MIN_POINTS:
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

    # Отправка найденных стоянок в Telegram
    if stops:
        for idx, stop in enumerate(stops, start=1):
            start_str = format_dt_to_irkutsk(stop['start'])
            end_str = format_dt_to_irkutsk(stop['end'])
            duration_min = int(stop['duration'])
            lat_c, lon_c = stop['center']
            try:
                address = get_address_from_coordinates(lat_c, lon_c)
            except Exception as e:
                logger.error(f"Ошибка геокодирования стоянки {idx}: {e}")
                address = "Неизвестный адрес"

            message = (
                f"*Стоянка:*\n"
                f"• Начало: `{start_str}`\n"
                f"• Конец: `{end_str}`\n"
                f"• Длительность: `{duration_min} мин`\n"
                f"• Координаты: ({lat_c:.6f}, {lon_c:.6f})\n"
                f"• Адрес: `{address}`"
            )
            send_to_telegram(message)
    else:
        logger.info("Стоянки не обнаружены.")

