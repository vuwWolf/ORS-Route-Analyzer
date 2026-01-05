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

def build_distance_matrix(max_workers=3):
    """
    Построение матрицы расстояний между всеми точками
    """
    # Загружаем кэш при старте
    _load_cache()
    
    names = list(points.keys())
    n = len(names)

    # Создаём DataFrame и ставим диагональ "X"
    dist_df = pd.DataFrame(index=names, columns=names, dtype=object)
    for i in range(n):
        dist_df.iloc[i, i] = "X"

    # Загружаем частично сохранённую матрицу, если есть
    try:
        existing = pd.read_csv("distance_matrix_partial.csv", index_col=0)
        for i in existing.index:
            dist_df.loc[i, existing.columns] = existing.loc[i]
        print("🔹 Загружены частично сохранённые данные.")
    except FileNotFoundError:
        pass

    # Подготовка пар для обработки
    pairs_to_process = []
    for i in range(n):
        for j in range(i+1, n):
            if pd.isna(dist_df.iloc[i, j]) or dist_df.iloc[i, j] in ["-", ""]:
                pairs_to_process.append((i, j, names, points))
    
    total = len(pairs_to_process)
    if total == 0:
        print("✅ Все расстояния уже рассчитаны")
        return dist_df
    
    print(f"📊 Нужно обработать {total} пар точек")
    
    # Параллельная обработка с ограниченным числом потоков
    done = 0
    save_counter = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Отправляем задачи на выполнение
        future_to_pair = {executor.submit(_process_distance_pair, pair): pair for pair in pairs_to_process}
        
        for future in as_completed(future_to_pair):
            try:
                i, j, result = future.result()
                dist_df.iloc[i, j] = result
                dist_df.iloc[j, i] = result
                
                done += 1
                save_counter += 1
                
                # Сохраняем прогресс каждые 5 обработанных пар
                if save_counter >= 5:
                    dist_df.to_csv("distance_matrix_partial.csv", encoding="utf-8-sig")
                    print(f"Прогресс {done}/{total} сохранён")
                    save_counter = 0
                    # Краткая пауза для сохранения кэша
                    _save_cache()
                
            except Exception as e:
                print(f"⚠️ Ошибка обработки пары: {e}")
                done += 1

    # Финальное сохранение
    dist_df.to_csv("distance_matrix_partial.csv", encoding="utf-8-sig")
    _save_cache()  # Сохраняем кэш в конце
    
    # Финальное сохранение в Excel с защитой от блокировки
    outfile = "distance_matrix.xlsx"
    for i in range(5):  # Уменьшаем количество попыток
        try:
            dist_df.to_excel(outfile, index=True)
            print(f"\n✅ Финальная матрица сохранена в {outfile}")
            break
        except PermissionError:
            print(f"⚠️ Файл {outfile} открыт. Сохраняем как новую версию.")
            outfile = f"distance_matrix_{i+1}.xlsx"

    return dist_df

def _process_route_pair(pair_data):
    """Обрабатывает одну пару точек для построения маршрута на карте"""
    (name1, coords1), (name2, coords2), current_route, total_routes = pair_data
    
    print(f"Обработка маршрута {current_route}/{total_routes}: {name1} ↔ {name2}")
    
    # Проверяем кэш маршрутов
    cache_key = _get_cache_key(coords1, coords2)
    with cache_lock:
        if cache_key in route_cache:
            route_data = route_cache[cache_key]
            return name1, name2, route_data, True
    
    try:
        # Подавляем предупреждения о rate limit
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            route = client.directions(
                coordinates=[coords1[::-1], coords2[::-1]],  # ORS ждёт (lon, lat)
                profile='driving-hgv',
                format='geojson'
            )
        
        # Сохраняем маршрут в кэш
        route_coords = [list(reversed(coord)) for coord in route['features'][0]['geometry']['coordinates']]
        with cache_lock:
            route_cache[cache_key] = route_coords
        
        print(f"✅ Маршрут {name1} ↔ {name2} добавлен")
        return name1, name2, route_coords, True
        
    except Exception as e:
        print(f"⚠️ Ошибка для {name1} ↔ {name2}: {e}")
        return name1, name2, None, False

def create_route_map(max_workers=2):
    """
    Создание интерактивной карты с маршрутами между всеми точками
    """
    # Загружаем кэш при старте
    _load_cache()
    
    # Создаём карту с центром, вычисленным по всем точкам
    center = compute_center_from_points(points)
    m = folium.Map(location=center, zoom_start=12)

    # Добавляем маркеры
    for name, coords in points.items():
        folium.Marker(coords, popup=name).add_to(m)

    # Подготовка пар маршрутов для обработки
    route_pairs = list(itertools.combinations(points.items(), 2))
    total_routes = len(route_pairs)
    
    print(f"🗺️ Построение {total_routes} маршрутов на карте")
    
    # Обработка маршрутов с ограниченным числом потоков
    processed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Подготовка задач с номерами для отслеживания прогресса
        tasks = []
        for i, ((name1, coords1), (name2, coords2)) in enumerate(route_pairs, 1):
            tasks.append(((name1, coords1), (name2, coords2), i, total_routes))
        
        # Отправляем задачи на выполнение
        future_to_task = {executor.submit(_process_route_pair, task): task for task in tasks}
        
        for future in as_completed(future_to_task):
            try:
                name1, name2, route_coords, success = future.result()
                
                if success and route_coords:
                    folium.PolyLine(
                        locations=route_coords,
                        color="blue", weight=1, opacity=0.5
                    ).add_to(m)
                
                processed += 1
                
                # Периодическое сохранение кэша
                if processed % 5 == 0:
                    _save_cache()
                    print(f"Прогресс {processed}/{total_routes}")
                
            except Exception as e:
                print(f"⚠️ Ошибка обработки маршрута: {e}")
                processed += 1

    # Финальное сохранение кэша и карты
    _save_cache()
    m.save("all_routes_map.html")
    print("✅ Карта сохранена в all_routes_map.html")
    return m
