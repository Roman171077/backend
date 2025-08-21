# app/analytics_stream.py

import logging
from datetime import timezone
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import BeaconCoordinate, GeoZone, GeozoneSession
import app.crud as crud
import app.schemas as schemas
from app.visit_analysis import analyze_session
from app.detect_stops import detect_stops
from app.telegram_bot import send_to_telegram
from app.analytics import haversine, format_dt_to_irkutsk

# ——————————————————————————————————————————————————————————————
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class RealTimeProcessor:
    """Класс для обработки координат маяка в режиме реального времени"""
    def __init__(self):
        # Инициализируем БД и загружаем все зоны
        self.db: Session = SessionLocal()
        zones = self.db.query(GeoZone).all()
        self.zone_defs = [
            (z.zone_id, z.name, z.center_lat, z.center_lon, z.radius_m, z.type)
            for z in zones
        ]

        # Восстанавливаем состояние (без уведомлений!)
        last = (
            self.db.query(BeaconCoordinate)
                   .order_by(BeaconCoordinate.recorded_at.desc())
                   .first()
        )
        if last:
            found = self._find_zone(last)
            if found:
                # вошли в зону
                self.state = 'zone'
                self.zone_id, _, _, _, _, self.zone_type = found
                sess = (
                    self.db.query(GeozoneSession)
                           .filter_by(zone_id=self.zone_id, status='open')
                           .order_by(GeozoneSession.entry_time.desc())
                           .first()
                )
                self.zone_session_id = sess.session_id if sess else None
                self.buffer = []
            else:
                # в движении
                self.state = 'travel'
                self.zone_id = None
                self.zone_session_id = None
                self.buffer = [last]
        else:
            # нет данных, считаем в движении
            self.state = 'travel'
            self.zone_id = None
            self.zone_session_id = None
            self.buffer = []

        logger.info(f"[RT] состояние восстановлено: {self.state}")

    def _find_zone(self, pt: BeaconCoordinate):
        for zid, zname, cz_lat, cz_lon, cz_r, ztype in self.zone_defs:
            if haversine(pt.latitude, pt.longitude, cz_lat, cz_lon) <= cz_r:
                return zid, zname, cz_lat, cz_lon, cz_r, ztype
        return None

    def process(self, pt: BeaconCoordinate):
        """Основной метод обработки точек координат"""
        t = pt.recorded_at
        current = self._find_zone(pt)

        try:
            # 1) zone → другая zone (сразу перескок между геозонами)
            if self.state == 'zone' and current and current[0] != self.zone_id:
                new_zid, new_zname, *_ , new_ztype = current

                # Закрываем предыдущую сессию
                if self.zone_type == 'territory':
                    crud.close_geozone_session(
                        self.db,
                        session_id=self.zone_session_id,
                        exit_time=t,
                        exit_lat=pt.latitude,
                        exit_lon=pt.longitude
                    )
                    analyze_session(self.db, self.zone_session_id)
                send_to_telegram(
                    f"🚗 Автомобиль выехал из зоны «{self.zone_id}» в {format_dt_to_irkutsk(t)}"
                )

                # Открываем новую сессию
                self.zone_type = new_ztype
                self.zone_id   = new_zid
                if new_ztype == 'territory':
                    sess = crud.create_geozone_session(
                        self.db,
                        schemas.GeozoneSessionCreate(
                            zone_id=new_zid,
                            entry_time=t,
                            exit_time=None,
                            entry_lat=pt.latitude,
                            entry_lon=pt.longitude,
                            exit_lat=None,
                            exit_lon=None,
                            status="open"
                        )
                    )
                    self.zone_session_id = sess.session_id
                send_to_telegram(
                    f"🚗 Въезд в зону «{new_zname}» в {format_dt_to_irkutsk(t)}"
                )

                # Сохраняем состояние
                self.state  = 'zone'
                self.buffer = []
                return

            # 2) zone → travel (выезд из зоны в движение)
            if self.state == 'zone' and current is None:
                if self.zone_type == 'territory':
                    crud.close_geozone_session(
                        self.db,
                        session_id=self.zone_session_id,
                        exit_time=t,
                        exit_lat=pt.latitude,
                        exit_lon=pt.longitude
                    )
                    analyze_session(self.db, self.zone_session_id)
                send_to_telegram(f"🚗 Автомобиль выехал из зоны в {format_dt_to_irkutsk(t)}")
                self.state = 'travel'
                self.zone_id = None
                self.buffer = [pt]
                return

            # 3) travel → zone (въезд в зону из движения)
            if self.state == 'travel' and current:
                if self.buffer:
                    detect_stops(self.buffer + [pt])
                zid, zname, *_ , ztype = current
                self.zone_type = ztype
                self.zone_id   = zid
                if ztype == 'territory':
                    sess = crud.create_geozone_session(
                        self.db,
                        schemas.GeozoneSessionCreate(
                            zone_id=zid,
                            entry_time=t,
                            exit_time=None,
                            entry_lat=pt.latitude,
                            entry_lon=pt.longitude,
                            exit_lat=None,
                            exit_lon=None,
                            status="open"
                        )
                    )
                    self.zone_session_id = sess.session_id
                send_to_telegram(f"🚗 Въезд в зону «{zname}» в {format_dt_to_irkutsk(t)}")
                self.state = 'zone'
                self.buffer = []
                return

            # 4) travel → travel (продолжаем движение)
            if self.state == 'travel' and current is None:
                self.buffer.append(pt)
                return

            # 5) zone → zone (остаемся в той же зоне) — ничего не делаем
            # если self.state=='zone' and current and current[0]==self.zone_id

        except Exception as err:
            # Откатываем «битую» сессию и пересоздаем соединение
            try:
                self.db.rollback()
                self.db.close()
            except Exception:
                pass
            self.db = SessionLocal()
            logger.error("❌ [RT] Ошибка обработки, сессия пересоздана: %s", err, exc_info=True)

# единый процессор
rt_processor = RealTimeProcessor()
