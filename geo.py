# geo.py
import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# 1) Подгружаем .env (только в локальной dev-среде)
load_dotenv()

# 2) Читаем ключи из окружения
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

# Проверим, что ключи есть
if not YANDEX_API_KEY or not TOMTOM_API_KEY:
    raise RuntimeError("Не заданы API-ключи YANDEX_API_KEY или TOMTOM_API_KEY")

ADDRESSES = [
    "г. Ангарск, Кулибина, 22",
    "Иркутский район, Садко тер. СНТ, Зимний проезд, 2",
    "г. Ангарск, нефтяник, 120",
    "г. Иркутск, березовая роща, 60",
    "г. Иркутск, дружбы, 92"
]

# ====== 1. ГЕОКОДЕР ЯНДЕКСА ======
def geocode_yandex(addresses):
    url = "https://geocode-maps.yandex.ru/1.x/"
    coords = []
    for addr in addresses:
        params = {
            "apikey": YANDEX_API_KEY,
            "format": "json",
            "geocode": addr
        }
        try:
            r = requests.get(url, params=params, timeout=(5,15))
            r.raise_for_status()
            m = r.json()["response"]["GeoObjectCollection"]["featureMember"]
            if m:
                lon, lat = map(float, m[0]["GeoObject"]["Point"]["pos"].split())
                coords.append((lat, lon))
            else:
                print(f"⚠️ Не найден адрес: {addr}")
                coords.append((None, None))
        except Exception as e:
            print(f"❌ Ошибка геокодирования «{addr}»: {e}")
            coords.append((None, None))
        time.sleep(0.2)
    return coords

# ====== 2. СИНХРОННЫЙ MATRIX v2 TOMTOM ======
def tomtom_time_matrix_v2(coords):
    """
    Использует POST https://api.tomtom.com/routing/matrix/2?key=...
    тело JSON:
      {
        "origins": [{"point":{...}}, ...],
        "destinations": [{"point":{...}}, ...],
        "options": {
          "departAt":"now",
          "routeType":"fastest",
          "traffic":"live",
          "travelMode":"car"
        }
      }
    Возвращает матрицу travelTimeInSeconds.
    """
    # Фильтруем не-None
    pts = [{"point": {"latitude": lat, "longitude": lon}}
           for lat, lon in coords if lat is not None]
    url = f"https://api.tomtom.com/routing/matrix/2?key={TOMTOM_API_KEY}"
    headers = {"Content-Type": "application/json"}
    body = {
        "origins": pts,
        "destinations": pts,
        "options": {
            "departAt": "now",
            "routeType": "fastest",
            "traffic": "live",
            "travelMode": "car"
        }
    }

    resp = requests.post(url, json=body, headers=headers, timeout=(10,20))
    resp.raise_for_status()
    data = resp.json()

    # Собираем в NxN
    n = len(pts)
    matrix = [[None]*n for _ in range(n)]
    for item in data["data"]:
        i = item["originIndex"]
        j = item["destinationIndex"]
        tt = item["routeSummary"]["travelTimeInSeconds"]
        matrix[i][j] = tt
    return matrix

# ====== 3. MAIN ======
def main():
    # 1) Геокодим
    coords = geocode_yandex(ADDRESSES)
    print("Координаты:")
    for addr, (lat, lon) in zip(ADDRESSES, coords):
        print(f"  • {addr:50s} → {lat}, {lon}")
    print()

    # 2) Матрица TomTom v2
    print("Получаем матрицу времени (TomTom v2)...")
    matrix = tomtom_time_matrix_v2(coords)
    if not matrix:
        print("Не удалось получить матрицу.")
        return

    # 3) Вывод в DataFrame и перевод в минуты
    df = pd.DataFrame(matrix,
                      index=[f"from_{i}" for i in range(len(matrix))],
                      columns=[f"to_{j}"   for j in range(len(matrix))])
    df = (df / 60).round(1)
    print("\nМатрица времени в пути (мин):")
    print(df)

if __name__ == "__main__":
    main()
