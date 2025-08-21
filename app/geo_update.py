# app/geo_update.py

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import Optional, Tuple, List
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Node, TravelTime

# ─── Настройка ──────────────────────────────────────────────────────────────
load_dotenv()  # подхватит YANDEX_API_KEY и TOMTOM_API_KEY из .env

YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

if not YANDEX_API_KEY or not TOMTOM_API_KEY:
    raise RuntimeError("Не заданы YANDEX_API_KEY или TOMTOM_API_KEY в окружении")

# ─── 1) Геокодер Яндекса ─────────────────────────────────────────────────────

def geocode_yandex(address: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Возвращает (lat, lon) или (None, None) если не найдено.
    """
    url = "https://geocode-maps.yandex.ru/1.x/"
    params = {
        "apikey": YANDEX_API_KEY,
        "format": "json",
        "geocode": address
    }
    resp = requests.get(url, params=params, timeout=(5, 15))
    resp.raise_for_status()
    members = resp.json()["response"]["GeoObjectCollection"]["featureMember"]
    if not members:
        return None, None
    lon, lat = map(float, members[0]["GeoObject"]["Point"]["pos"].split())
    return lat, lon

# ─── 2) ТомТом матрица времени ────────────────────────────────────────────────

def tomtom_time_matrix(coords: List[Tuple[Optional[float], Optional[float]]]) -> List[List[int]]:
    """
    Принимает список (lat, lon), возвращает матрицу travelTimeInSeconds.
    None → 0
    """
    pts = []
    for lat, lon in coords:
        if lat is None or lon is None:
            # если нет координат, вставляем дублера
            pts.append({"point": {"latitude": 0.0, "longitude": 0.0}})
        else:
            pts.append({"point": {"latitude": lat, "longitude": lon}})

    url = f"https://api.tomtom.com/routing/matrix/2?key={TOMTOM_API_KEY}"
    body = {
        "origins":      pts,
        "destinations": pts,
        "options": {
            "departAt":   "now",
            "routeType":  "fastest",
            "traffic":    "live",
            "travelMode": "car"
        }
    }
    resp = requests.post(
        url,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=(10, 20)
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    n = len(pts)
    matrix = [[0]*n for _ in range(n)]
    for item in data:
        i = item["originIndex"]
        j = item["destinationIndex"]
        matrix[i][j] = item["routeSummary"].get("travelTimeInSeconds", 0)
    return matrix

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    db: Session = SessionLocal()

    # 1) Считываем адреса из БД
    nodes = db.query(Node).order_by(Node.node_id).all()
    if not nodes:
        print("❌ В таблице nodes нет ни одной записи.")
        return

    # 2) Геокодим каждый адрес и обновляем lat/lon
    print("🔍 Геокодирование адресов…")
    coords: List[Tuple[Optional[float], Optional[float]]] = []
    for node in nodes:
        lat, lon = geocode_yandex(node.address)
        if lat is None or lon is None:
            print(f"⚠️ Не найден: {node.address}")
        else:
            node.lat, node.lon = lat, lon
        coords.append((node.lat, node.lon))
        time.sleep(0.2)
    db.commit()
    print("✅ Геокодирование завершено.\n")

    # 3) Получаем новую матрицу времени
    print("⏱ Получаем матрицу TomTom…")
    matrix = tomtom_time_matrix(coords)
    print("✅ Матрица получена.\n")

    # 4) Обновляем travel_times
    print("🗑 Очищаем старые travel_times…")
    db.query(TravelTime).delete()
    db.commit()

    print("💾 Сохраняем новую матрицу…")
    n = len(nodes)
    for i in range(n):
        for j in range(n):
            tt = matrix[i][j]
            db.add(TravelTime(
                from_id    = nodes[i].node_id,
                to_id      = nodes[j].node_id,
                travel_sec = tt
            ))
    db.commit()
    print("✅ Обновлено в travel_times.\n")

    # 5) (Опционально) Печать матрицы
    df = pd.DataFrame(
        matrix,
        index=[f"{node.node_id}" for node in nodes],
        columns=[f"{node.node_id}" for node in nodes]
    )
    print("Матрица travel_time (сек):")
    print(df)

    db.close()

if __name__ == "__main__":
    main()
