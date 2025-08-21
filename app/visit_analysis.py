#app.visit_analysis.py
import logging
import sys
from math import radians, sin, cos, sqrt, atan2
from datetime import timedelta, datetime, timezone
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session
from app.db import engine, Base, SessionLocal
import app.models as models
import requests
from dotenv import load_dotenv
import os
from app.telegram_bot import send_to_telegram

# ——— Конфигурация и инициализация ——————————————————————————————————————————————————

load_dotenv()

YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')
GEOCODE_URL = "https://geocode-maps.yandex.ru/1.x/"
# Настраиваем логирование
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Создаём таблицы (если ещё не созданы)
Base.metadata.create_all(bind=engine)

# Часовые пояса
UTC = timezone.utc
IRKUTSK = ZoneInfo('Asia/Irkutsk')

def format_dt_to_irkutsk(dt: datetime) -> str:
    """
    Получает UTC-дату (с tzinfo=UTC или naive как UTC) и возвращает строку
    в формате YYYY-MM-DD HH:MM:SS по Иркутскому времени.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt_irk = dt.astimezone(IRKUTSK)
    return dt_irk.strftime('%Y-%m-%d %H:%M:%S')

# ——— Утилитарные функции ——————————————————————————————————————————————————————

def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние между двумя точками (метры), формула Haversine."""
    R = 6371000
    φ1, φ2 = radians(lat1), radians(lat2)
    Δφ = radians(lat2 - lat1)
    Δλ = radians(lon2 - lon1)
    a = sin(Δφ/2)**2 + cos(φ1) * cos(φ2) * sin(Δλ/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def get_address_from_coordinates(lat: float, lon: float, lang: str = "ru_RU") -> str:
    """
    Обратное геокодирование с учётом ближайшего дома.
    Возвращает полную строку адреса (из поля text).
    """
    params = {
        'apikey': YANDEX_API_KEY,
        'geocode': f"{lon},{lat}",
        'format': 'json',
        'kind': 'house',    # ближайший дом
        'results': 1,       # только первый результат
        'lang': lang
    }

    try:
        resp = requests.get(GEOCODE_URL, params=params, timeout=5)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Ошибка запроса к Geocoder: {e}")
        return "Не удалось получить адрес"

    try:
        geo_obj = resp.json()['response']['GeoObjectCollection']\
                          ['featureMember'][0]['GeoObject']
        # Полный адрес из метаданных
        return geo_obj['metaDataProperty']['GeocoderMetaData']['text']
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Ошибка разбора ответа Geocoder: {e}")
        return "Адрес не найден"

# ——— Основная логика анализа сессии —————————————————————————————————————————————

def analyze_session(db: Session, session_id: int, threshold: float = 0.95):
    logger.info(f"=== Начало анализа session_id={session_id} ===")

    sess = db.get(models.GeozoneSession, session_id)
    if not sess or sess.status != 'closed':
        logger.error(f"Session {session_id} недоступна или статус != closed")
        return

    zone = db.get(models.GeoZone, sess.zone_id)
    if not zone:
        logger.error(f"GeoZone {sess.zone_id} не найдена")
        return
    logger.info(f"Session {session_id}: геозона '{zone.name}', центр=({zone.center_lat:.6f},{zone.center_lon:.6f}), радиус={zone.radius_m}м")

    # Фильтрация задач по зоне
    all_tasks = (
        db.query(models.Task)
        .filter(models.Task.status != 'done')
        .all()
    )
    tasks = [
        t for t in all_tasks
        if distance(t.lat, t.lng, zone.center_lat, zone.center_lon) <= zone.radius_m
    ]
    logger.info(f"  Задач в геозоне: {len(tasks)}/{len(all_tasks)}")

    # Загружаем координаты сессии (UTC!)
    coords = db.query(models.BeaconCoordinate)\
               .filter(
                   models.BeaconCoordinate.recorded_at >= sess.entry_time,
                   models.BeaconCoordinate.recorded_at <= sess.exit_time
               )\
               .order_by(models.BeaconCoordinate.recorded_at)\
               .all()
    logger.info(f"  Координат за сессию: {len(coords)}")

    # Детекция стоянок вне задач
    OUTSIDE_TASK_RADIUS = 200
    CLUSTER_RADIUS = 5
    MIN_POINTS = 10

    idle_coords = [
        c for c in coords
        if not tasks or all(
            distance(c.latitude, c.longitude, t.lat, t.lng) > OUTSIDE_TASK_RADIUS
            for t in tasks
        )
    ]

    stops = []
    current = {'coords': [], 'start': None}

    for coord in idle_coords:
        t = coord.recorded_at
        if not current['coords']:
            current['coords'], current['start'] = [coord], t
        else:
            first = current['coords'][0]
            if distance(coord.latitude, coord.longitude, first.latitude, first.longitude) <= CLUSTER_RADIUS:
                current['coords'].append(coord)
            else:
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
                current = {'coords': [coord], 'start': t}

    # Последняя серия
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

    # Отправка стоянок
    if stops:
        for idx, stop in enumerate(stops, start=1):
            start_str = format_dt_to_irkutsk(stop['start'])
            end_str   = format_dt_to_irkutsk(stop['end'])
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
        logger.info("Стоянки в геозоне не обнаружены.")

    # Если есть задачи — анализ визитов
    if tasks:
        rules = db.query(models.GeofenceRule).order_by(models.GeofenceRule.radius_m).all()
        if not rules:
            logger.error("Нет правил geofence_rule")
            return
        max_conf = max(r.confidence for r in rules)
        logger.info(f"  Правил загружено: {len(rules)}")

        # Инициализация state...
        state = {
            t.task_id: {
                r.rule_id: {'current_run': 0, 'run_start': None, 'best_run': 0, 'best_start': None}
                for r in rules
            }
            for t in tasks
        }

        # Проходим по всем координатам и обновляем state...
        for coord in coords:
            t = coord.recorded_at
            for task in tasks:
                d_task = distance(coord.latitude, coord.longitude, task.lat, task.lng)
                for rule in rules:
                    st = state[task.task_id][rule.rule_id]
                    if d_task <= rule.radius_m:
                        if st['current_run'] == 0:
                            st['run_start'] = t
                        st['current_run'] += 1
                    else:
                        if st['current_run'] > st['best_run']:
                            st['best_run'] = st['current_run']
                            st['best_start'] = st['run_start']
                        st['current_run'] = 0
                        st['run_start'] = None
                    if d_task <= rule.radius_m:
                        break

        # Финализация последних серий
        for task in tasks:
            for rule in rules:
                st = state[task.task_id][rule.rule_id]
                if st['current_run'] > st['best_run']:
                    st['best_run'] = st['current_run']
                    st['best_start'] = st['run_start']

        # Оценка и отправка результатов
        for task in tasks:
            best_score, best_rule, best_start, best_run = 0, None, None, 0
            for rule in rules:
                st = state[task.task_id][rule.rule_id]
                if not st['best_start']:
                    continue
                dwell_ratio = min(st['best_run'] / rule.dwell_minutes, 1.0)
                base_score = dwell_ratio * rule.confidence
                time_factor = 1.2 if task.planned_start <= sess.exit_time else 0.8
                score = base_score * time_factor
                if score > best_score:
                    best_score, best_rule, best_start, best_run = score, rule, st['best_start'], st['best_run']

            if not best_rule:
                continue

            final_percent = min(best_score / max_conf * 100, 100.0)
            detected_start = best_start
            detected_end   = best_start + timedelta(minutes=best_run - 1)
            task.detected_start = detected_start
            task.detected_end   = detected_end
            task.detect_confidence = best_rule.confidence

            duration_min = int((detected_end - detected_start).total_seconds() / 60)
            msg = (
                f"*Обнаружено посещение задачи:*\n"
                f"• Адрес: `{task.address_raw}`\n"
                f"• Начало: `{format_dt_to_irkutsk(detected_start)}`\n"
                f"• Конец: `{format_dt_to_irkutsk(detected_end)}`\n"
                f"• Длительность: `{duration_min} мин`\n"
                f"• Вероятность: `{final_percent:.1f}%`"
            )
            send_to_telegram(msg)

            # Сохраняем историю визита
            hist = models.TaskVisitHistory(
                session_id=sess.session_id,
                task_id=task.task_id,
                rule_id=best_rule.rule_id,
                attempt_start=detected_start,
                attempt_end=detected_end,
                duration_sec=best_run * 60,
                result='confirmed' if final_percent >= threshold*100 else 'false',
                notes=f"final_percent={final_percent:.1f}%"
            )
            db.add(hist)

    # Закрываем сессию
    sess.status = 'processed'
    db.commit()
    logger.info(f"=== Конец анализа session_id={session_id} ===")


# ——— Точка входа ———————————————————————————————————————————————————————

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python -m app.visit_analysis <session_id>")
        sys.exit(1)
    db = SessionLocal()
    try:
        analyze_session(db, int(sys.argv[1]))
    finally:
        db.close()
