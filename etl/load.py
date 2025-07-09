import os
import pandas as pd
import psycopg2
from psycopg2 import sql

def load_to_csv(df:pd.DataFrame, file_path: str, separator=',',encoding='utf-8'):
    '''
    Сохраняет передаваемый датафрейм в формате .csv по указанному пути

    Параметры:

    '''

    try:
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        
        df.to_csv(file_path, sep=separator, encoding=encoding)
        print(f"Датафрейм успешно сохранен в {file_path}")
        return True
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
        return False 
    
def load_to_db(df: pd.DataFrame, table_name, table_key, db = 'open_meteo_stats', user = 'admin', password = 'admin'
               , host = 'localhost', port = '5433', schema = 'nsk_plus_7gt', conflict_resolve = 'NOTHING'):
    '''
    
    '''
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(f"dbname={db} user={user} password={password} host={host} port={port}")
        cursor = conn.cursor()
        print('Подключение к БД прошло успешно')

        insert_query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            sql.SQL('{schm}.{tbl}').format(schm=sql.Identifier(schema), tbl=sql.Identifier(table_name))
            ,sql.SQL(', ').join(map(sql.Identifier, df.columns.tolist()))
            ,sql.SQL(', ').join(sql.Placeholder()*len(df.columns.tolist()))
        )
        
        if conflict_resolve != 'NOTHING':
            update_columns = [col for col in df.columns.tolist() if col != table_key]
            update_clause = sql.SQL(', ').join(
                sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(col), sql.Identifier(col))
                for col in update_columns
            )
            conflict_query = sql.SQL(' ON CONFLICT ({}) DO UPDATE SET {}').format(
                sql.Identifier(table_key), update_clause
            )
        else:
            conflict_query = sql.SQL(' ON CONFLICT ({}) DO NOTHING').format(
                sql.Identifier(table_key)
            )

        query = insert_query + conflict_query

        for row in df.itertuples():
            try:
                cursor.execute(query, row[1:len(df.columns.tolist())+1])
                print(f'{row[0]} Строка успешно загружена в БД')
            except Exception as e:
                print(f'Ошибка: Вставка строки {row.get(table_key, 'unknown')} прошла некорректно, {e}')
                conn.rollback()
                continue

        conn.commit()
        print(f'Выгрузка таблицы в БД завершена!')
        return True
        
    except Exception as e:
        print(f'Ошибка: Действия с БД были прерваны по причине: \n {e}')
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print('Соединение с БД разорвано')