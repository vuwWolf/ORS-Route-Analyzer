#!/usr/bin/env python3
"""
Route Analyzer - Анализ маршрутов и построение карт для логистики
Объединяет функциональность построения карт и расчета матрицы расстояний
"""

import folium
import openrouteservice
import pandas as pd
import itertools
import time
import warnings
import argparse
import os
import json
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from threading import Lock
from openrouteservice.exceptions import ApiError
from API_ORS_key import orskey
from points import points

# Инициализация клиента OpenRouteService
client = openrouteservice.Client(key=orskey)

# Глобальные переменные для кэширования и синхронизации
cache_lock = Lock()
distance_cache = {}
route_cache = {}

def _get_cache_key(coord1, coord2):
    """Генерирует уникальный ключ для кэша на основе координат"""
    coord_str = f"{coord1[0]:.6f},{coord1[1]:.6f}-{coord2[0]:.6f},{coord2[1]:.6f}"
    return hashlib.md5(coord_str.encode()).hexdigest()

def _load_cache():
    """Загружает кэш из файла"""
    global distance_cache, route_cache
    try:
        with open('distance_cache.json', 'r') as f:
            distance_cache = json.load(f)
        with open('route_cache.json', 'r') as f:
            route_cache = json.load(f)
        print(f"📦 Загружено {len(distance_cache)} расстояний и {len(route_cache)} маршрутов из кэша")
    except FileNotFoundError:
        print("📦 Кэш не найден, начинаем с чистого листа")

def _save_cache():
    """Сохраняет кэш в файл"""
    with cache_lock:
        try:
            with open('distance_cache.json', 'w') as f:
                json.dump(distance_cache, f)
            with open('route_cache.json', 'w') as f:
                json.dump(route_cache, f)
        except Exception as e:
            print(f"⚠️ Ошибка сохранения кэша: {e}")
