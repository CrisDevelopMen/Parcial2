"""Ejemplo de consumidor Kinesis para alimentar GET /analytics/live.
En producción: Lambda (Kinesis trigger) o proceso long-running que actualiza
un store compartido (Redis, DynamoDB o memoria si es single-instance).
"""
import json
import time
from collections import deque
from datetime import datetime, timedelta

# Ventana deslizante de eventos (simplificado, en memoria)
events_window = deque()  # (event_ts, event_type)

def process_record(record):
    data = json.loads(record["Data"])
    event_ts = datetime.strptime(data["event_ts"], "%Y-%m-%dT%H:%M:%SZ")
    events_window.append((event_ts, data["event_type"]))
    # Limpiar eventos fuera de la ventana máxima (ej. 60 min)
    cutoff = datetime.utcnow() - timedelta(minutes=60)
    while events_window and events_window[0][0] < cutoff:
        events_window.popleft()

def get_counts(window_minutes: int):
    cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
    searches = reservations = cancellations = 0
    for ts, etype in events_window:
        if ts >= cutoff:
            if etype == "search_performed":
                searches += 1
            elif etype == "reservation_created":
                reservations += 1
            elif etype == "reservation_cancelled":
                cancellations += 1
    conversion = (reservations / searches) if searches else 0.0
    return {
        "window_minutes": window_minutes,
        "searches": searches,
        "reservations": reservations,
        "cancellations": cancellations,
        "conversion": round(conversion, 4),
    }

# En un Lambda o loop: leer del stream y llamar process_record
# El endpoint /analytics/live llama a get_counts(window)
