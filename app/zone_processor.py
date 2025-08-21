# app/zone_processor.py

import math
import logging
from datetime import timezone
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session

from app.models import BeaconCoordinate, GeoZone
import app.crud as crud
from app.schemas import GeozoneSessionCreate
from app.visit_analysis import analyze_session
from app.detect_stops import detect_stops
from app.telegram_bot import send_to_telegram

logger = logging.getLogger(__name__)
IRKUTSK = ZoneInfo('Asia/Irkutsk')


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(dλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class ZoneStateMachine:
    def __init__(self, db: Session, initial_point: BeaconCoordinate = None):
        self.db = db
        # загружаем все зоны
        zones = db.query(GeoZone).all()
        self.zone_defs = [
            (z.zone_id, z.name, z.center_lat, z.center_lon, z.radius_m, z.type)
            for z in zones
        ]

        # состояние
        self.state = 'travel'
        self.zone_session_id = None
        self.zone_type = None

        # данные для расчёта длительности
        self.zone_name = None
        self.zone_entry_time = None

        # буфер точек при движении
        self.buffer: list[BeaconCoordinate] = []

        # если есть стартовая точка — восстанавливаем состояние
        if initial_point:
            found = self._find_zone(initial_point)
            if found:
                self._enter_zone(found, initial_point, init=True)
            else:
                self.buffer = [initial_point]

    def _find_zone(self, pt: BeaconCoordinate):
        for zid, name, cz_lat, cz_lon, cz_r, ztype in self.zone_defs:
            if haversine(pt.latitude, pt.longitude, cz_lat, cz_lon) <= cz_r:
                return zid, name, cz_lat, cz_lon, cz_r, ztype
        return None

    def _enter_zone(self, found, pt: BeaconCoordinate, init=False):
        zid, zname, *_ , ztype = found
        t = pt.recorded_at

        # создаём сессию в БД
        sess = crud.create_geozone_session(
            self.db,
            GeozoneSessionCreate(
                zone_id=zid,
                entry_time=t,
                exit_time=None,
                entry_lat=pt.latitude,
                entry_lon=pt.longitude,
                exit_lat=None,
                exit_lon=None,
                status='open'
            )
        )

        # сохраняем имя зоны и время входа для расчёта длительности
        self.zone_name = zname
        self.zone_entry_time = t
        self.zone_session_id = sess.session_id
        self.zone_type = ztype
        self.state = 'zone'
        self.buffer.clear()

        # уведомление
        send_to_telegram(
            f"🚗 Въезд в зону «{zname}» в {t.astimezone(IRKUTSK).strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def _exit_zone(self, pt: BeaconCoordinate):
        t = pt.recorded_at

        # закрываем сессию и делаем анализ
        if self.zone_type == 'territory' and self.zone_session_id:
            crud.close_geozone_session(
                self.db,
                session_id=self.zone_session_id,
                exit_time=t,
                exit_lat=pt.latitude,
                exit_lon=pt.longitude
            )
            analyze_session(self.db, self.zone_session_id)

        # считаем длительность пребывания
        duration = 0
        if self.zone_entry_time:
            duration = int((t - self.zone_entry_time).total_seconds() / 60)

        # уведомление с названием зоны и временем
        send_to_telegram(
            f"🚗 Автомобиль выехал из зоны «{self.zone_name}» в "
            f"{t.astimezone(IRKUTSK).strftime('%Y-%m-%d %H:%M:%S')}, "
            f"длительность: {duration} мин."
        )

        # сбрасываем состояние в «движение»
        self.state = 'travel'
        self.buffer = [pt]
        self.zone_session_id = None
        self.zone_type = None
        self.zone_name = None
        self.zone_entry_time = None

    def process_point(self, pt: BeaconCoordinate):
        current = self._find_zone(pt)
        try:
            # выход из зоны
            if self.state == 'zone' and not current:
                self._exit_zone(pt)
                return

            # въезд в зону
            if self.state == 'travel' and current:
                # сначала обрабатываем накопленные точки пути
                detect_stops(self.buffer + [pt])
                self._enter_zone(current, pt)
                return

            # продолжаем движение — буферизуем
            if self.state == 'travel':
                self.buffer.append(pt)
                return

            # внутри зоны без выхода — ничего не делаем

        except Exception as err:
            logger.error("❌ Error in state transition: %s", err, exc_info=True)
            self.db.rollback()

    def finalize(self):
        # при завершении батча нужно тоже закрыть открытую зону или обработать оставшийся путь
        if self.state == 'zone' and self.zone_type == 'territory' and self.zone_session_id:
            last = self.db.query(BeaconCoordinate) \
                          .order_by(BeaconCoordinate.recorded_at.desc()) \
                          .first()
            if last:
                self._exit_zone(last)

        elif self.state == 'travel' and self.buffer:
            detect_stops(self.buffer)
