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

def get_distance_truck(coord1, coord2, max_attempts=3):
    """
    Получение расстояния между двумя точками для грузовика
    
    Args:
        coord1: Координаты первой точки (lat, lon)
        coord2: Координаты второй точки (lat, lon)
        max_attempts: Максимальное количество попыток
    
    Returns:
        float: Расстояние в километрах или None при ошибке
    """
    # Проверяем кэш
    cache_key = _get_cache_key(coord1, coord2)
    with cache_lock:
        if cache_key in distance_cache:
            return distance_cache[cache_key]
    
    for attempt in range(max_attempts):
        try:
            # Подавляем предупреждения о rate limit
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                route = client.directions(
                    coordinates=[coord1[::-1], coord2[::-1]],  # ORS ждёт (lon, lat)
                    profile='driving-hgv',
                    format='geojson'
                )

            dist_m = route['features'][0]['properties']['segments'][0]['distance']
            dist_km = round(dist_m / 1000, 2)
            
            # Сохраняем в кэш
            with cache_lock:
                distance_cache[cache_key] = dist_km
            
            return dist_km

        except ApiError as e:
            msg = str(e).lower()
            if "rate limit" in msg:
                wait = min(15, (attempt + 1) * 5)  # Уменьшаем время ожидания
                print(f"⚠️ Лимит API превышен, ждём {wait} сек... (попытка {attempt + 1}/{max_attempts})")
                time.sleep(wait)
                continue
            elif "could not find routable point" in msg:
                print(f"❌ Не удалось построить маршрут {coord1} → {coord2}")
                return None
            else:
                print(f"❌ Ошибка API: {e}")
                return None
        except Exception as e:
            print(f"⚠️ Неожиданная ошибка {e}, попытка {attempt + 1}")
            time.sleep(2)  # Уменьшаем время ожидания
            continue
    print("🚫 Не удалось получить маршрут после всех попыток")
    return None

def _process_distance_pair(pair_data):
    """Обрабатывает одну пару точек для матрицы расстояний"""
    i, j, names, points_data = pair_data
    name_i, name_j = names[i], names[j]
    coord_i, coord_j = points_data[name_i], points_data[name_j]
    
    dist = get_distance_truck(coord_i, coord_j)
    
    if dist is not None:
        print(f"{name_i} ↔ {name_j}: {dist:.2f} км")
        return i, j, dist
    else:
        print(f"⚠️ Пропущено {name_i} ↔ {name_j}")
        return i, j, "-"
