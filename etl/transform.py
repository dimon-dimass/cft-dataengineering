from typing import List
import pandas as pd
import numpy as np


class OpenMeteo:
    '''
    Обработка и трасформация данных open-meteo API
    Класс содержит методы вычисления, конвертации, агрегации и преобразования данных

    Атрибуты:
        meteo_data (dict): Raw данные запроса в формате json
        hourly (pd.DataFrame): Преобразованные в датафрейм, почасовые данные из запроса
        daily (pd.DataFrame): Преобразованные в датафрейм, суточные данные из запроса
    '''

    def __init__(self, meteo_data):
        self.json_data = meteo_data
        self.hourly = pd.DataFrame(self.json_data['hourly'])      
        self.daily = pd.DataFrame(self.json_data['daily'])

        # Перенос временных данных в новый столбец с окончанием "_utc"
        self.hourly['time_utc'] = self.hourly['time']
        self.daily['time_utc'] = self.daily['time']
        self.daily['sunrise_utc'] = self.daily['sunrise']
        self.daily['sunset_utc'] = self.daily['sunset']

        # Преобразование временных данный с учетом временной зоны
        if self.json_data['hourly_units']['time'] == 'unixtime':
            if '+' in self.json_data['timezone_abbreviation']:
                self.hourly['time'] = self.hourly['time']+self.json_data['utc_offset_seconds']
            else:
                self.hourly['time'] = self.hourly['time']-self.json_data['utc_offset_seconds']
            self.hourly['date'] = pd.to_datetime(self.hourly['time'], unit='s').dt.normalize().astype('int64')//10**9
        else:
            self.hourly['time'] = pd.to_datetime(self.hourly['time'])
            if '+' in self.json_data['timezone_abbreviation']:
                self.hourly['time'] = self.hourly['time']+pd.to_timedelta(self.json_data['utc_offset_seconds'],unit='s')
            else:
                self.hourly['time'] = self.hourly['time']-pd.to_timedelta(self.json_data['utc_offset_seconds'],unit='s')
            self.hourly['date'] = self.hourly['time'].dt.date

        if self.json_data['daily_units']['time'] == 'unixtime':
            if '+' in self.json_data['timezone_abbreviation']:
                self.daily['date'] = self.daily['time']+self.json_data['utc_offset_seconds']
            else:
                self.daily['date'] = self.daily['time']-self.json_data['utc_offset_seconds']
        else:
            self.hourly['time'] = pd.to_datetime(self.hourly['time'])
            if '+' in self.json_data['timezone_abbreviation']:
                self.hourly['time'] = self.hourly['time']+pd.to_timedelta(self.json_data['utc_offset_seconds'],unit='s')
            else:
                self.hourly['time'] = self.hourly['time']-pd.to_timedelta(self.json_data['utc_offset_seconds'],unit='s')
            self.hourly['date'] = self.hourly['time'].dt.date

        if 'sunset' in self.json_data['daily_units'] and self.json_data['daily_units']['sunset'] == 'unixtime':
            if '+' in self.json_data['timezone_abbreviation']:

                self.daily['sunset'] = self.daily['sunset']+self.json_data['utc_offset_seconds']
            else:
                self.daily['sunset'] = self.daily['sunset']-self.json_data['utc_offset_seconds']

        if 'sunrise' in self.json_data['daily_units'] and self.json_data['daily_units']['sunrise'] == 'unixtime':
            if '+' in self.json_data['timezone_abbreviation']:
                self.daily['sunrise'] = self.daily['sunrise']+self.json_data['utc_offset_seconds']
            else:
                self.daily['sunrise'] = self.daily['sunrise']-self.json_data['utc_offset_seconds']

    def avg_for_24h(self, units: List[str]):
        '''
        Вычисляет средние значения за 24 часа.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''

        agg_dict = {unit: 'mean' for unit in units}
        replace_array = ['_celsius','_m_per_s','_m']
                
        units_new = [transform_unit(unit,replace_array,'avg_','_24h') for unit in units]
        rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}

        avg_units_24h = (
            self.hourly.groupby('date')
            .agg(agg_dict|{'time':'count'})
            .round(3)
            .reset_index()
        )

        avg_units_24h.loc[avg_units_24h['time'] != 24, units] = np.nan

        return avg_units_24h.drop('time', axis=1).rename(columns=rename_dict).set_index('date')

    def avg_for_daylight(self, units: List[str]):
        '''
        Вычисляет средние значения за промежуток светового дня.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''

        agg_dict = {unit: 'mean' for unit in units}
        replace_array = ['_celsius','_m_per_s','_m']

        units_new = [transform_unit(unit,replace_array,'avg_','_daylight') for unit in units]
        rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}
        
        hourly_dl = (
            self.hourly[['time','date']+units].set_index('date')
            .join(self.daily[['date','sunrise','sunset']].set_index('date'))
            .reset_index()
        )

        avg_units_dl = (hourly_dl[
                ((hourly_dl['time'] >= hourly_dl['sunrise']) &
                (hourly_dl['time'] <= hourly_dl['sunset'])) |
                (hourly_dl['sunrise'].isna()) |
                (hourly_dl['sunset'].isna())    
            ].groupby('date')
            .agg(agg_dict|{'sunrise':'mean', 'sunset':'mean'})
            .round(3)
            .reset_index()
        )

        avg_units_dl.loc[avg_units_dl['sunrise'].isna() | avg_units_dl['sunset'].isna(), units] = np.nan

        return avg_units_dl.drop(['sunrise','sunset'], axis=1).rename(columns=rename_dict).set_index('date')

    def total_for_24h(self, units: List[str]):
        '''
        Вычисляет общие значения за 24 часа.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''

        agg_dict = {unit: 'sum' for unit in units}
        replace_array = ['_mm']

        units_new = [transform_unit(unit,replace_array,'total_','_24h') for unit in units]
        rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}

        total_units_24h = (
            self.hourly.groupby('date')
            .agg(agg_dict|{'time':'count'})
            .round(3)
            .reset_index()
        )

        total_units_24h.loc[total_units_24h['time'] != 24, units] = np.nan

        return total_units_24h.drop('time', axis=1).rename(columns=rename_dict).set_index('date')

    def total_for_daylight(self, units: List[str]):
        '''
        Вычисляет общие значения за промежуток светового дня.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''

        agg_dict = {unit: 'sum' for unit in units}
        replace_array = ['_mm']

        units_new = [transform_unit(unit,replace_array,'total_','_daylight') for unit in units]
        rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}

        hourly_dl = (
            self.hourly[['time','date']+units].set_index('date')
            .join(self.daily[['date','sunrise','sunset']].set_index('date'))
            .reset_index()
        )

        total_units_dl = (hourly_dl[
                ((hourly_dl['time'] >= hourly_dl['sunrise']) &
                (hourly_dl['time'] <= hourly_dl['sunset'])) |
                (hourly_dl['sunrise'].isna()) |
                (hourly_dl['sunset'].isna())    
            ].groupby('date')
            .agg(agg_dict|{'sunrise':'mean','sunset':'mean'})
            .round(3)
            .reset_index()
        )

        total_units_dl.loc[total_units_dl['sunrise'].isna() | total_units_dl['sunset'].isna(), units] = np.nan

        return total_units_dl.drop(['sunrise','sunset'], axis=1).rename(columns=rename_dict).set_index('date')

    def fah_to_cel(self, units: List[str]):
        '''
        Преобразует значения измеряющиеся в градусах Фаренгейта в градусы Цельсия.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''
        
        if all((attr,'°F') in self.json_data['hourly_units'].items() for attr in units):
            if all('avg' not in unit or 'total' not in unit for unit in units):
                units_new = [unit+'_celsius' for unit in units]
                rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}
            else: rename_dict = {}

            fah_units = self.hourly[['time','date']+units]
            fah_units.loc[:,units] = ((fah_units.loc[:,units]-32)*5/9).round(1)

            return fah_units.rename(columns=rename_dict).set_index(['time','date'])
        else: raise ValueError('Передаваемый список столбцов представлены не в Фаренгейтах(°F), обновите список!')

    def kn_to_mps(self, units: List[str]):
        '''
        Преобразует значения измеряющиеся в узлах в метры в секунду.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''
    
        if all((attr,'kn') in self.json_data['hourly_units'].items() for attr in units):
            if all('avg' not in unit or 'total' not in unit for unit in units):
                units_new = [unit+'_m_per_s' for unit in units]
                rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}
            else: rename_dict = {}

            kn_units = self.hourly[['time','date']+units]
            kn_units.loc[:,units] = (kn_units.loc[:,units]*0.514).round(1)

            return kn_units.rename(columns=rename_dict).set_index(['time','date'])
        else: raise ValueError('Передаваемый список столбцов представлены не в Узлах(knots/kn), обновите список!')

    def inch_to_mm(self, units: List[str]):
        '''
        Преобразует значения измеряющиеся в дюймах в миллиметры.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''
    
        if all((attr,'inch') in self.json_data['hourly_units'].items() for attr in units):
            if all('avg' not in unit or 'total' not in unit for unit in units):
                units_new = [unit+'_mm' for unit in units]
                rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}
            else: rename_dict = {}

            inch_units = self.hourly[['time','date']+units]
            inch_units.loc[:,units] = (inch_units.loc[:,units]*25.4).round(1)

            return inch_units.rename(columns=rename_dict).set_index(['time','date'])
        else: raise ValueError('Передаваемый список столбцов представлены не в Дюймах(inch), обновите список!')

    def ft_to_m(self, units: List[str]):
        '''
        Преобразует значения измеряющиеся в футах в метры.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''
    
        if all((attr,'ft') in self.json_data['hourly_units'].items() for attr in units):
            if all('avg' not in unit or 'total' not in unit for unit in units):
                units_new = [unit+'_m' for unit in units]
                rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}
            else: rename_dict = {}

            ft_units = self.hourly[['time','date']+units]
            ft_units.loc[:,units] = (ft_units.loc[:,units]*0.3048).round(1)

            return ft_units.rename(columns=rename_dict).set_index(['time','date'])
        else: raise ValueError('Передаваемый список столбцов представлены не в Футах(ft), обновите список!')

    def daylight_hours(self):
        '''
        Вычисляет промежуток светого дня, как разницу между временем восхода и временем заката солнца.

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''

        if self.json_data['daily_units']['sunrise'] == 'unixtime' and self.json_data['daily_units']['sunset'] == 'unixtime':
            daylight_duration = self.daily[['date','sunrise','sunset']]
            daylight_duration['daylight_hours'] = ((daylight_duration.loc[:,'sunset']-daylight_duration.loc[:,'sunrise'])/3600).round(1)

        return daylight_duration.drop(columns=['sunrise','sunset']).set_index('date')

    def unix_to_iso(self, units:List[str]):
        '''
        Преобразует временные данные из unix формата в формат ISO 8601 ('YYYY-mm-ddTHH:MM:SSZ').

        Параметры:
            units (List[str]): Список имен столбцов
        Возвращает:
            pd.DataFrame
        '''
                
        units_new = [unit+'_iso' for unit in units]
        rename_dict = {unit:unit_new for unit,unit_new in zip(units,units_new)}

        if all(unit in self.json_data['daily_units'].keys() for unit in units):
            iso_df = self.daily[['date']+units]
            iso_df.loc[:,units] = iso_df[units].apply(pd.to_datetime, unit='s').apply(lambda col: col.dt.strftime('%Y-%m-%dT%H:%M:%SZ'))
            return iso_df.rename(columns=rename_dict).set_index('date')
        elif all(unit in self.json_data['hourly_units'].keys() for unit in units):
            iso_df = self.hourly[['time']+units]
            iso_df.loc[:,units] = iso_df[units].apply(pd.to_datetime, unit='s').apply(lambda col: col.dt.strftime('%Y-%m-%dT%H:%M:%SZ'))
            return iso_df.rename(columns=rename_dict).set_index('time')
        else:
            raise ValueError('Передаваемый список столбцов невозможно перевести в ISO 8601 формат, обновите список столбцов')

def transform_unit(unit, replace_array, agg, replace_val):
    '''
    Преобразует передаваемые имена столбцов в новые.
    
    Параметры:
        unit: Наименование столбца
        replace_array: Массив значений подлежащих замене
        agg: Приставка нового наименования столбца
        replace_val: Окончание для нового наименования
    Возвращает:
        Строку
    '''

    for metric in replace_array:
        if metric in unit: 
            return agg + unit.replace(metric, replace_val)
    return agg + unit + replace_val