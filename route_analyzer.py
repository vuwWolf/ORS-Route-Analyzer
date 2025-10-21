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
from openrouteservice.exceptions import ApiError
from API_ORS_key import orskey
from points import points

# Инициализация клиента OpenRouteService
client = openrouteservice.Client(key=orskey)

def get_distance_truck(coord1, coord2, max_attempts=5):
    """
    Получение расстояния между двумя точками для грузовика
    
    Args:
        coord1: Координаты первой точки (lat, lon)
        coord2: Координаты второй точки (lat, lon)
        max_attempts: Максимальное количество попыток
    
    Returns:
        float: Расстояние в километрах или None при ошибке
    """
    for attempt in range(max_attempts):
        try:
            # Подавляем предупреждения о rate limit, так как они обрабатываются автоматически
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                route = client.directions(
                    coordinates=[coord1[::-1], coord2[::-1]],  # ORS ждёт (lon, lat)
                    profile='driving-hgv',
                    format='geojson'
                )

            dist_m = route['features'][0]['properties']['segments'][0]['distance']
            return round(dist_m / 1000, 2)

        except ApiError as e:
            msg = str(e).lower()
            if "rate limit" in msg:
                wait = min(30, (attempt + 1) * 10)  # Увеличиваем время ожидания, но ограничиваем максимум
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
            time.sleep(5)
            continue
    print("🚫 Не удалось получить маршрут после всех попыток")
    return None

def build_distance_matrix():
    """
    Построение матрицы расстояний между всеми точками
    """
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

    # Заполнение матрицы расстояний
    total = n*(n-1)//2
    done = 0

    for i in range(n):
        for j in range(i+1, n):
            name_i, name_j = names[i], names[j]
            if pd.notna(dist_df.iloc[i, j]) and dist_df.iloc[i, j] not in ["-", ""]:
                done += 1
                continue

            coord_i, coord_j = points[name_i], points[name_j]
            dist = get_distance_truck(coord_i, coord_j)

            if dist is not None:
                dist_df.iloc[i, j] = dist
                dist_df.iloc[j, i] = dist
                print(f"{name_i} ↔ {name_j}: {dist:.2f} км")
            else:
                dist_df.iloc[i, j] = "-"
                dist_df.iloc[j, i] = "-"
                print(f"⚠️ Пропущено {name_i} ↔ {name_j}")

            done += 1
            # Частичное сохранение прогресса
            dist_df.to_csv("distance_matrix_partial.csv", encoding="utf-8-sig")
            print(f"Прогресс {done}/{total} сохранён")
            
            # Адаптивная пауза в зависимости от прогресса
            if done % 10 == 0:  # Каждые 10 запросов делаем большую паузу
                time.sleep(5)
            else:
                time.sleep(1.5)

    # Финальное сохранение в Excel с защитой от блокировки
    outfile = "distance_matrix.xlsx"
    for i in range(10):
        try:
            dist_df.to_excel(outfile, index=True)
            print(f"\n✅ Финальная матрица сохранена в {outfile}")
            break
        except PermissionError:
            print(f"⚠️ Файл {outfile} открыт. Сохраняем как новую версию.")
            outfile = f"distance_matrix_{i+1}.xlsx"

    return dist_df

def create_route_map():
    """
    Создание интерактивной карты с маршрутами между всеми точками
    """
    # Создаём карту с центром на складе
    m = folium.Map(location=points["Склад"], zoom_start=14)

    # Добавляем маркеры
    for name, coords in points.items():
        folium.Marker(coords, popup=name).add_to(m)

    # Строим маршруты между каждой парой точек
    total_routes = len(list(itertools.combinations(points.items(), 2)))
    current_route = 0
    
    for (name1, coords1), (name2, coords2) in itertools.combinations(points.items(), 2):
        current_route += 1
        print(f"Обработка маршрута {current_route}/{total_routes}: {name1} ↔ {name2}")
        
        try:
            # Подавляем предупреждения о rate limit
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                route = client.directions(
                    coordinates=[coords1[::-1], coords2[::-1]],  # ORS ждёт (lon, lat)
                    profile='driving-hgv',
                    format='geojson'
                )
            
            folium.PolyLine(
                locations=[list(reversed(coord)) for coord in route['features'][0]['geometry']['coordinates']],
                color="blue", weight=1, opacity=0.5
            ).add_to(m)
            print(f"✅ Маршрут {name1} ↔ {name2} добавлен")
        except Exception as e:
            print(f"⚠️ Ошибка для {name1} ↔ {name2}: {e}")

        # Адаптивная пауза в зависимости от прогресса
        if current_route % 5 == 0:  # Каждые 5 маршрутов делаем большую паузу
            time.sleep(5)
        else:
            time.sleep(2)

    # Сохраняем карту
    m.save("all_routes_map.html")
    print("✅ Карта сохранена в all_routes_map.html")
    return m

def main():
    """
    Основная функция с поддержкой аргументов командной строки
    """
    parser = argparse.ArgumentParser(description='Route Analyzer - Анализ маршрутов и построение карт')
    parser.add_argument('--mode', choices=['map', 'matrix', 'both'], default='both',
                       help='Режим работы: map (только карта), matrix (только матрица), both (оба)')
    parser.add_argument('--clean', action='store_true',
                       help='Очистить временные файлы перед началом')
    
    args = parser.parse_args()
    
    if args.clean:
        # Очистка временных файлов
        temp_files = ['distance_matrix_partial.csv', 'all_routes_map.html', 'distance_matrix.xlsx']
        for file in temp_files:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️ Удален файл: {file}")
    
    print("🚀 Запуск Route Analyzer")
    print(f"📊 Точки для анализа: {len(points)}")
    
    if args.mode in ['map', 'both']:
        print("\n🗺️ Создание карты маршрутов...")
        create_route_map()
    
    if args.mode in ['matrix', 'both']:
        print("\n📈 Построение матрицы расстояний...")
        build_distance_matrix()
    
    print("\n✅ Анализ завершен!")

if __name__ == "__main__":
    main()
