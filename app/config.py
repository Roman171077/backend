# app/config.py
import os

PARKING_RADIUS_M     = int(os.getenv("PARKING_RADIUS_M", "150"))
PARKING_DURATION_SEC = 8 * 60  # 8 минут
