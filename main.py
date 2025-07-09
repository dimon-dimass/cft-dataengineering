import argparse
import pandas as pd

from etl import extract,transform,load

def open_meteo_etl(start_date='2025-05-16',end_date='2025-05-30',file_path = ['res/hourly.csv','res/daily.csv'], conflict_resolve = 'NOTHING'):
    '''
    Запуск ETL процесса OpenMeteoAPI данных
    Параметры:
    start_date: Дата начала интервала выгрузки данных по API (например, '2025-05-16')
    end_date: Крайняя дата интервала выгрузки данных по API (например, '2025-05-16')
    file_path: Путь/ти в системе выгрузки .csv файлов
    conflict_resolve: Вариант решения проблемы выгрузки дубликатов в БД ('NOTHING' - игнорирование дублирующих записей
                                                                         'UPDATE' - обновление дублирующих записей)
    '''
    try:
        om_obj = transform.OpenMeteo(extract.open_meteo_api(start_date=start_date,end_date=end_date))

        # В отдельных переменных определим столбцы времени/дат, для дальнейших соединений транформированных столбцов
        hours = pd.DataFrame(om_obj.hourly[['time','relative_humidity_2m']]).set_index('time')
        days = pd.DataFrame(om_obj.daily[['date']]).set_index('date')

        # Обновление данных датафрейма hourly класса OpenMeteo 
        om_obj.hourly = (
            hours.join(om_obj.kn_to_mps(['wind_speed_10m', 'wind_speed_80m']))
            .join(om_obj.ft_to_m(['visibility']))
            .join(om_obj.fah_to_cel(['temperature_2m','dew_point_2m','apparent_temperature','temperature_80m','temperature_120m','soil_temperature_0cm','soil_temperature_6cm']))
            .join(om_obj.inch_to_mm(['rain', 'showers', 'snowfall']))
            .reset_index()
        )

        # Список новых столбцов таблицы hourly класса OpenMeteo
        columns = om_obj.hourly.columns.tolist()

        # Переменная table1 содержит датафрейм с агрегированными метриками итоговой таблицы
        table1 = (
            days.join(om_obj.avg_for_24h([columns[6]]+[columns[2]]+columns[7:11]+columns[3:6]))
            .join(om_obj.total_for_24h(columns[13:]))
            .join(om_obj.avg_for_daylight([columns[6]]+[columns[2]]+columns[7:11]+columns[3:6]))
            .join(om_obj.total_for_daylight(columns[13:]))
            .join(om_obj.daylight_hours())
            .join(om_obj.unix_to_iso(['sunrise', 'sunset']))
            .reset_index()
            .rename(columns={'date':'date_unix'})
        )

        # Переменная table2 содержит датафрейм с конвертированными метриками итоговой таблицы
        table2 = (
            om_obj.hourly.drop(['date','relative_humidity_2m','dew_point_2m_celsius','visibility_m'],axis=1)
            .rename(columns={'time':'time_unix'})
        )

        print(f'Выгрузка первой части итоговой таблицы по пути {file_path[0]}')
        load.load_to_csv(table2.set_index('time_unix'), file_path[0])

        print(f'Выгрузка второй части итоговой таблицы по пути {file_path[1]}')
        load.load_to_csv(table1.set_index('date_unix'), file_path[1])

        print(f'Выгрузка в БД')
        load.load_to_db(table2, 'hourly', 'time_unix', conflict_resolve = conflict_resolve)
        load.load_to_db(table1, 'daily', 'date_unix', conflict_resolve= conflict_resolve)

    except Exception as e:
        print(f"Ошибка в ETL процессе: {e}")
        return False
    
def parse_arguments():
    '''
    Настраивает и парсит аргументы командной строки.
    
    Returns:
    argparse.Namespace: Объект с распаршенными аргументами
    '''
    parser = argparse.ArgumentParser(
        description="ETL-процесс для извлечения данных из API, их трансформации и сохранения в CSV."
    )
    
    # Обязательный аргумент
    parser.add_argument(
        '--start_date','-sdt',
        type=str,
        default='2025-05-16',
        help='Начальная дата интервала запроса (по умолчанию: 2025-05-16)'
    )

    parser.add_argument(
        '--end_date','-edt',
        type=str,
        default='2025-05-30',
        help='Крайняя дата интервала запроса (по умолчанию: 2025-05-30)'
    )

    parser.add_argument(
        '--file_path',
        type=str,
        default=['res/hourly.csv','res/daily.csv'],
        help='Путь для сохранения CSV-файлов (по умолчанию: res/daily.csv, res/hourly.csv)'
    )

    parser.add_argument(
        '--conflict_resolve',
        type=str,
        default= 'NOTHING',
        help='Способ борьбы с дубликатами записей при выгрузке в БД (по умолчанию: NOTHING)' \
        'Варианты: - ''NOTHING'' - игнорирование дублирующий по ключу записей,' \
        '          - ''UPDATE'' - обновление дублирующий по ключу записей'
    )
    
    return parser.parse_args()

if __name__ == '__main__':

    args = parse_arguments()

    open_meteo_etl(
        start_date = args.start_date,
        end_date = args.end_date,
        file_path = args.file_path
    )