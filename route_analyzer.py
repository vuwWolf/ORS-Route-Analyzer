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
