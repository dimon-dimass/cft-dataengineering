import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
import logging

from extract import open_meteo_api
from transform import OpenMeteo,transform_unit
from load import load_to_csv, load_to_db
from main import open_meteo_etl

# Настройка логирования для тестов
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestETLProcess(unittest.TestCase):
    def setUp(self):
        """Настройка перед каждым тестом."""
        self.connection_string = "dbname=test user=admin password=admin host=localhost port=5433"
        self.schema = "public"
        self.table_name = "test"
        # ("https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,visibility,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-05-28&end_date=2025-05-30")
        self.daily_array = ['sunrise','sunset']
        self.hourly_array = ['temperature_2m','relative_humidity_2m','wind_speed_10m','visibility','snowfall']
        self.start_date = "2025-05-28"
        self.end_date = "2025-05-30"

        # Пример данных для мока API
        self.mock_api_response = {
            "results": {"latitude":55.0,
                        "longitude":83.0,
                        "generationtime_ms":3.981351852416992,
                        "utc_offset_seconds":25200,
                        "timezone":"Asia/Novosibirsk",
                        "timezone_abbreviation":"GMT+7",
                        "elevation":135.0,
                        "hourly_units":
                            {"time":"unixtime",
                             "temperature_2m":"°F",
                             "relative_humidity_2m":"%",
                             "wind_speed_10m":"kn",
                             "visibility":"ft",
                             "snowfall":"inch"},
                        "hourly":
                            {"time":[1748365200,1748368800,1748372400,1748376000,1748379600,1748383200,1748386800,1748390400,1748394000,1748397600,
                                     1748401200,1748404800,1748408400,1748412000,1748415600,1748419200,1748422800,1748426400,1748430000,1748433600,
                                     1748437200,1748440800,1748444400,1748448000,1748451600,1748455200,1748458800,1748462400,1748466000,1748469600,
                                     1748473200,1748476800,1748480400,1748484000,1748487600,1748491200,1748494800,1748498400,1748502000,1748505600,
                                     1748509200,1748512800,1748516400,1748520000,1748523600,1748527200,1748530800,1748534400,1748538000,1748541600,
                                     1748545200,1748548800,1748552400,1748556000,1748559600,1748563200,1748566800,1748570400,1748574000,1748577600,
                                     1748581200,1748584800,1748588400,1748592000,1748595600,1748599200,1748602800,1748606400,1748610000,1748613600,
                                     1748617200,1748620800],
                            "temperature_2m":[38.8,38.8,38.7,38.7,39.0,38.9,38.8,38.5,38.5,39.2,39.7,40.3,39.9,40.2,41.9,42.6,44.4,44.8,45.1,46.4,
                                              44.7,44.0,42.2,40.7,39.5,37.0,36.3,35.5,34.9,34.3,33.8,34.8,37.9,41.6,44.6,47.0,49.0,51.3,52.9,53.8,
                                              54.3,55.1,54.8,57.3,56.2,55.0,52.6,50.8,49.2,47.0,46.1,45.2,44.5,43.9,44.2,48.3,49.6,51.0,53.2,56.5,
                                              59.9,62.1,64.9,63.6,65.2,66.7,66.0,67.4,66.8,65.5,63.2,61.6],
                            "relative_humidity_2m":[63,59,59,55,55,56,64,77,82,86,87,86,84,78,72,70,64,60,57,55,62,65,72,75,78,79,81,83,84,85,87,
                                                    92,79,68,59,54,49,34,33,33,32,34,39,37,38,44,52,59,66,73,75,77,80,83,84,76,67,62,60,57,52,45,
                                                    41,50,49,45,47,49,50,54,60,63],
                            "wind_speed_10m":[2.7,3.8,4.5,5.0,4.9,4.9,5.2,5.8,6.0,5.7,3.9,4.1,5.8,6.6,8.2,8.2,8.4,8.1,8.2,7.1,6.1,4.4,3.0,2.6,2.5,
                                              2.2,1.9,1.7,1.5,1.4,1.2,1.8,1.6,2.3,3.5,3.9,4.2,4.7,5.3,5.7,5.6,5.8,5.7,4.5,3.9,2.3,2.1,2.1,2.3,2.5,
                                              2.4,2.2,2.2,2.2,2.4,3.6,4.7,5.3,5.5,6.5,7.6,7.8,8.8,8.6,8.6,9.3,9.1,7.0,6.3,4.7,4.5,3.3],
                            "visibility":[79199.477,79199.477,79199.477,79199.477,79199.477,76706.039,1509.186,131.234,131.234,65.617,65.617,65.617,
                                          65.617,65.617,131.234,787.402,59383.203,37664.043,75721.789,78871.391,78608.922,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,55446.195,42125.984,35433.070,50590.551,79199.477,70866.141,
                                          79199.477,78608.922,79199.477,79199.477],
                            "snowfall":[0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,
                                        0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,
                                        0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,
                                        0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000]},
                            "daily_units":
                                {"time":"unixtime",
                                "sunrise":"unixtime",
                                "sunset":"unixtime"},
                            "daily":
                                {"time":[1748365200,1748451600,1748538000],
                                 "sunrise":[1748383239,1748469570,1748555905],
                                 "sunset":[1748443792,1748530276,1748616758]}},
            "status": "OK"
        }

        # Пример DataFrame для тестов
        self.test_df_hourly = pd.DataFrame({
            "time": [1748365200,1748368800,1748372400,1748376000,1748379600,1748383200,1748386800,1748390400,1748394000,1748397600,
                                     1748401200,1748404800,1748408400,1748412000,1748415600,1748419200,1748422800,1748426400,1748430000,1748433600,
                                     1748437200,1748440800,1748444400,1748448000,1748451600,1748455200,1748458800,1748462400,1748466000,1748469600,
                                     1748473200,1748476800,1748480400,1748484000,1748487600,1748491200,1748494800,1748498400,1748502000,1748505600,
                                     1748509200,1748512800,1748516400,1748520000,1748523600,1748527200,1748530800,1748534400,1748538000,1748541600,
                                     1748545200,1748548800,1748552400,1748556000,1748559600,1748563200,1748566800,1748570400,1748574000,1748577600,
                                     1748581200,1748584800,1748588400,1748592000,1748595600,1748599200,1748602800,1748606400,1748610000,1748613600,
                                     1748617200,1748620800],
            "temperature_2m": [38.8,38.8,38.7,38.7,39.0,38.9,38.8,38.5,38.5,39.2,39.7,40.3,39.9,40.2,41.9,42.6,44.4,44.8,45.1,46.4,
                                              44.7,44.0,42.2,40.7,39.5,37.0,36.3,35.5,34.9,34.3,33.8,34.8,37.9,41.6,44.6,47.0,49.0,51.3,52.9,53.8,
                                              54.3,55.1,54.8,57.3,56.2,55.0,52.6,50.8,49.2,47.0,46.1,45.2,44.5,43.9,44.2,48.3,49.6,51.0,53.2,56.5,
                                              59.9,62.1,64.9,63.6,65.2,66.7,66.0,67.4,66.8,65.5,63.2,61.6],
            "relative_humidity_2m": [63,59,59,55,55,56,64,77,82,86,87,86,84,78,72,70,64,60,57,55,62,65,72,75,78,79,81,83,84,85,87,
                                                    92,79,68,59,54,49,34,33,33,32,34,39,37,38,44,52,59,66,73,75,77,80,83,84,76,67,62,60,57,52,45,
                                                    41,50,49,45,47,49,50,54,60,63],
            "wind_speed_10m": [2.7,3.8,4.5,5.0,4.9,4.9,5.2,5.8,6.0,5.7,3.9,4.1,5.8,6.6,8.2,8.2,8.4,8.1,8.2,7.1,6.1,4.4,3.0,2.6,2.5,
                                              2.2,1.9,1.7,1.5,1.4,1.2,1.8,1.6,2.3,3.5,3.9,4.2,4.7,5.3,5.7,5.6,5.8,5.7,4.5,3.9,2.3,2.1,2.1,2.3,2.5,
                                              2.4,2.2,2.2,2.2,2.4,3.6,4.7,5.3,5.5,6.5,7.6,7.8,8.8,8.6,8.6,9.3,9.1,7.0,6.3,4.7,4.5,3.3],
            "visibility": [79199.477,79199.477,79199.477,79199.477,79199.477,76706.039,1509.186,131.234,131.234,65.617,65.617,65.617,
                                          65.617,65.617,131.234,787.402,59383.203,37664.043,75721.789,78871.391,78608.922,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,79199.477,
                                          79199.477,79199.477,79199.477,79199.477,79199.477,55446.195,42125.984,35433.070,50590.551,79199.477,70866.141,
                                          79199.477,78608.922,79199.477,79199.477],
            "snowfall":[0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,
                                        0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,
                                        0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,
                                        0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.000]
        })
        self.test_df_hourly['time'] += 25200
        self.test_df_hourly['date'] = pd.to_datetime(self.test_df_hourly['time'], unit='s').dt.normalize().astype('int64')//10**9

        self.test_df_daily = pd.DataFrame({
            "time": [1748365200,1748451600,1748538000],
            "sunrise": [1748383239,1748469570,1748555905],
            "sunset": [1748443792,1748530276,1748616758],
        })
        self.test_df_daily += 25200
        self.test_df_daily.rename(columns={'time':'date'})

        # Создаем тестовую таблицу
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
                date INTEGER NOT NULL PRIMARY KEY,
                avg_metric_24h numeric,
                total_metric_24h numeric,
                avg_metric_daylight numeric,
                total_metric_daylight numeric,
                some_metric nummeric,
                time_iso varchar(25)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

    def tearDown(self):
        """Очистка после каждого теста."""
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {self.schema}.{self.table_name}")
        conn.commit()
        cursor.close()
        conn.close()

    @patch('extract.requests.get')
    def test_extract_data(self, mock_get):
        """Тест функции open_meteo_api"""
        # Мокаем ответ API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_response
        mock_get.return_value = mock_response

        # Вызываем функцию
        result = open_meteo_api(daily=self.daily_array, hourly=self.hourly_array, start_date=self.start_date, end_date=self.end_date)
        
        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["daily"]["time"], [1748365200,1748451600,1748538000])
        logger.info("Тест extract_data пройден")

    def test_transform_data(self):
        """Тест класса OpenMeteo"""
        # Создаем экземпляр трансформера
        openmeteo_obj = OpenMeteo([self.mock_api_response["results"]])
        
        # Выполняем трансформацию
        result = openmeteo_obj.avg_for_24h(['temperature_2m','relative_humidity_2m','wind_speed_10m','visibility'])
        
        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result, self.test_df_hourly.groupby('date').agg('mean'))

        # Выполняем трансформацию
        result = openmeteo_obj.avg_for_daylight(['temperature_2m','relative_humidity_2m','wind_speed_10m','visibility'])
        
        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result[0].keys()), ["avg_sunrise_24h", "avg_sunset_24h", "avg_day_length_24h"])

                # Выполняем трансформацию
        result = openmeteo_obj.total_for_24h(['snowfall'])
        
        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result[0].keys()), ["avg_sunrise_24h", "avg_sunset_24h", "avg_day_length_24h"])

                # Выполняем трансформацию
        result = openmeteo_obj.total_for_daylight(['temperature_2m','relative_humidity_2m','wind_speed_10m','visibility'])
        
        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result[0].keys()), ["avg_sunrise_24h", "avg_sunset_24h", "avg_day_length_24h"])

                # Выполняем трансформацию
        result = openmeteo_obj.daylight_hours(['sunrise','sunset'])
        
        # Проверяем результат
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result[0].keys()), ["avg_sunrise_24h", "avg_sunset_24h", "avg_day_length_24h"])
        logger.info("Тест transform_data пройден")

    def test_save_to_db(self):
        """Тест функции load_to_db"""
        # Подготовка данных
        transformed_df = pd.DataFrame({
            "id": [1, 2],
            "avg_sunrise_24h": ["2025-05-16T05:30:00Z", "2025-05-17T05:29:00Z"],
            "avg_sunset_24h": ["2025-05-16T20:00:00Z", "2025-05-17T20:01:00Z"],
            "avg_day_length_24h": [51300, 51400]
        })

        # Вызываем функцию
        result = save_to_db(transformed_df, self.schema, self.table_name, self.connection_string, conflict_key="id")
        
        # Проверяем результат
        self.assertTrue(result)
        
        # Проверяем данные в базе
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.schema}.{self.table_name}")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 1)  # Проверяем id первой строки
        logger.info("Тест save_to_db пройден")

    def test_save_to_db_duplicates(self):
        """Тест обработки дубликатов в save_to_db."""
        # Вставляем начальные данные
        df = pd.DataFrame({
            "id": [1],
            "avg_sunrise_24h": ["2025-05-16T05:30:00Z"],
            "avg_sunset_24h": ["2025-05-16T20:00:00Z"],
            "avg_day_length_24h": [51300]
        })
        load_to_db(df, self.schema, self.table_name, self.connection_string, conflict_key="id")

        # Пытаемся вставить дубликат с измененными данными
        df_duplicate = pd.DataFrame({
            "id": [1],
            "avg_sunrise_24h": ["2025-05-16T05:31:00Z"],
            "avg_sunset_24h": ["2025-05-16T20:01:00Z"],
            "avg_day_length_24h": [51400]
        })
        result = save_to_db(df_duplicate, self.schema, self.table_name, self.connection_string, conflict_key="id")
        
        # Проверяем результат
        self.assertTrue(result)
        
        # Проверяем, что данные обновились
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT avg_day_length_24h FROM {self.schema}.{self.table_name} WHERE id = 1")
        day_length = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        self.assertEqual(day_length, 51400)  # Проверяем обновленное значение
        logger.info("Тест save_to_db_duplicates пройден")

    @patch('extract.requests.get')
    def test_run_etl(self, mock_get):
        """Интеграционный тест всего ETL-процесса."""
        # Мокаем ответ API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_response
        mock_get.return_value = mock_response

        # Вызываем ETL-процесс
        result = run_etl(self.api_url, self.schema, self.table_name, self.connection_string, self.start_date)
        
        # Проверяем результат
        self.assertTrue(result)
        
        # Проверяем данные в базе
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {self.schema}.{self.table_name}")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 1)  # Проверяем id
        logger.info("Тест run_etl пройден")

if __name__ == '__main__':
    unittest.main()