# detect_from_excel.py

from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2
import pandas as pd

from sqlalchemy.orm import Session
from app.db import SessionLocal
from app import crud, schemas, config, models

# 1) Haversine — расстояние в метрах
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# 2) Центроид
def centroid(points):
    lat = sum(p['latitude'] for p in points) / len(points)
    lon = sum(p['longitude'] for p in points) / len(points)
    return lat, lon

# 3) Основная логика поиска стоянок
def detect_parking_from_list(coords: list[dict], db_sess: Session):
    coords.sort(key=lambda x: x['recorded_at'])
    min_pts = config.PARKING_DURATION_SEC // 60
    events = []
    i = 0
    n = len(coords)
    while i <= n - min_pts:
        window = coords[i:i + min_pts]
        # макс. попарное расстояние
        max_d = 0
        for a in window:
            for b in window:
                d = haversine(a['latitude'], a['longitude'], b['latitude'], b['longitude'])
                if d > max_d:
                    max_d = d

        if max_d <= config.PARKING_RADIUS_M:
            start = window[0]['recorded_at']
            lat_c, lon_c = centroid(window)
            j = i + min_pts
            # расширяем, пока в радиусе
            while j < n and haversine(lat_c, lon_c,
                                      coords[j]['latitude'], coords[j]['longitude']) <= config.PARKING_RADIUS_M:
                cnt = j - i + 1
                lat_c = (lat_c * (cnt - 1) + coords[j]['latitude']) / cnt
                lon_c = (lon_c * (cnt - 1) + coords[j]['longitude']) / cnt
                j += 1
            end = coords[j - 1]['recorded_at']
            # записываем в БД
            evt_in = schemas.ParkingEventCreate(
                start_time=start,
                end_time=end,
                center_lat=lat_c,
                center_lon=lon_c
            )
            db_evt = crud.create_parking_event(db_sess, evt_in)
            events.append({
                'id': db_evt.id,
                'start': start,
                'end': end,
                'center': (lat_c, lon_c)
            })
            i = j
        else:
            i += 1
    return events

def main():
    # читаем симуляцию
    file = "coords.xlsx"
    df = pd.read_excel(file)  # убедитесь, что openpyxl установлен
    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
    coords = df.to_dict(orient="records")

    db = SessionLocal()
    # Очищаем старые тестовые events
    db.query(models.ParkingEvent).delete()
    db.commit()

    # Запускаем детектор
    events = detect_parking_from_list(coords, db)

    # Выводим результаты
    print(f"Найдено стоянок: {len(events)}")
    for e in events:
        print(f"  id={e['id']}, start={e['start']}, end={e['end']}, center={e['center']}")

    db.close()

if __name__ == "__main__":
    main()
