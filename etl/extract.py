from typing import List
import requests


def open_meteo_api(latitude: str = '55.0344', longitude: str = '82.9434', daily: List[str] = ["sunrise","sunset","daylight_duration"]
                   ,hourly: List[str] = ['temperature_2m','relative_humidity_2m','dew_point_2m','apparent_temperature','temperature_80m','temperature_120m','wind_speed_10m'
                                         ,'wind_speed_80m','wind_direction_10m','wind_direction_80m','visibility','evapotranspiration','weather_code','soil_temperature_0cm'
                                         ,'soil_temperature_6cm','rain','showers','snowfall']
                    ,timezone: str = 'auto', timeformat: str = 'unixtime', wind_speed_unit: str = 'kn', temperature_unit : str = 'fahrenheit'
                    ,precipitation_unit: str = 'inch', start_date: str = '2025-05-16', end_date: str = '2025-05-30'):
    '''
    Выполняет запрос данных по API open-meteo и извлекает данные в формате JSON

    Параметры:
        latitude: Широта запрашевоемого местоположения
        longitude: Долгота запрашеваемого местоположения
        daily: Список ежедневных данных
        hourly: Список почасовых данных
        timezone: Часовой пояс запрашеваемого местоположения
        timeformat: Формат времени для запршеваемых данных
        wind_speed_unit: Единица измерения скорости ветра
        temperature_unit: Единица измерения температуры
        precipitation_unit: Единица измерения атмосферных осадков
        start_date: Начальная дата интервала запроса 
        end_date: Крайняя дата интервала запроса
    Возвращает:
        Словарь (результат запроса)
    '''

    url = (f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily={','.join(daily)}&"
        f"hourly={','.join(hourly)}&timezone={timezone}&timeformat={timeformat}&wind_speed_unit={wind_speed_unit}&temperature_unit={temperature_unit}&"
        f"precipitation_unit={precipitation_unit}&start_date={start_date}&end_date={end_date}")

    try:
        r = requests.get(url)
        r.raise_for_status()

        return r.json()
    except requests.RequestException as e:
        print(f"Ошибка при извлечении данных: {e}")
        return None