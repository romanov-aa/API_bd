import pandas as pd
from sqlalchemy import create_engine, inspect
import numpy as np
import datetime
import time




def timer(func):
    """        
    Функция timer показывает время выполнения других функций в формате "hh-mm-ss".

    """
    def wrapper(*args, **kwargs):
        now = datetime.datetime.now()
        result = func(*args, **kwargs)
        now1 = datetime.datetime.now()
        now2 = now1 - now
        # print(f"Время выполнения функции {func.__name__}: {now2}")
        # Это я оставил, чтобы показать, что фунция работает, просто время выполнения происходит меньше, чем за 1 секунду.
        now2 = str(now2)
        now2 = now2.split('.')[0]
        print(f"Время выполнения функции {func.__name__}: {now2}")


        return result
    return wrapper



class DB_api():
    def __init__(self, engine):
        self.engine = engine


    @timer
    def create_table(self, name_table, dataframe):
        """        
        Функция create_table создаёт таблицу

        name_table - название таблицы, которую хотите создать
        dataframe - название датафрейма, на основе которого вы хотите сделать таблицу

        """
        dataframe.to_sql(name_table, self.engine, if_exists='replace', index=False)
        return print("Таблица создана")


    @timer
    def insert_sql(self, name_table, dataframe, param):
        """        
        Функция insert_sql позволяет добавить данные в таблицу

        name_table - название таблицы, в которую хотите добавить данные
        dataframe - название датафрейма, которым вы хотите наполнить таблицу
        param - параметр, которым вы можете задать, что хотите сделать с таблицей, если она существует.
        Если таблицы не существует, то появится новая.
        Параметр append означает, что мы просто добавляем записи.
        Параметр replace означает, что мы просто заменяем таблицу на новую.

        """
        if param == "append":
            dataframe.to_sql(name_table, self.engine, if_exists="append", index=False)
            print(f"Данные успешно добавлены в таблицу {name_table}.")
        elif param == "replace":
            dataframe.to_sql(name_table, self.engine, if_exists="replace", index=False)
            print(f"Данные успешно добавлены в таблицу {name_table}.")
    

    @timer
    def read_sql(self, name_table):
        """        
        Функция read_sql позволяет прочитать таблицу из бд и создать на её основе датафрейм.

        name_table - название таблицы, из которой хотите взять данные

        """
        check_table = inspect(self.engine)
        if name_table not in check_table.get_table_names():
            print(f"Таблица {name_table} не существует.")
            return None
        else:
            result = pd.read_sql_query(f"SELECT * FROM {name_table}", self.engine)
            return result

    
    @timer
    def truncate_table(self, name_table):
        """        
        Функция truncate_table позволяет удалить все данные из таблицы

        name_table - название таблицы, в которой вы хотите удалить данные

        """
        check_table = inspect(self.engine)
        if name_table not in check_table.get_table_names():
            return print(f'Таблица {name_table} не существует')
        else:
            with self.engine.connect() as connection:
                connection.execute(f'TRUNCATE TABLE {name_table}')
            print(f"Таблица {name_table} теперь пуста")
    

    @timer
    def execute(self, query):
        """        
        Функция execute позволяет вам выполнить свой SQL запрос

        query - ваш запрос.

        """
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return result


    @timer
    def delete_from_table(self, name_table, condition):
        """        
        Функция delete_from_table позволяет удалить данные из таблицы с заданным условием/ями

        name_table - название таблицы, в которой вы хотите удалить данные
        condition - условие. Условия пушутся в формате: condition = {"column1": "value1", "column2" : "value1"}
        где column - название колонки, а value - значение, по которому надо удалить запись.
        """
        check_table = inspect(self.engine)
        if name_table not in check_table.get_table_names():
            return print(f'Таблица {name_table} не существует')
        else:
            with self.engine.connect() as connection:
                columns = condition.keys()
                conditer = ''
                for column in columns:
                    if len(conditer) == 0:
                        conditer += f"{column} = '{condition[column]}'"
                    else:
                        conditer += f" or {column} = '{condition[column]}'"
                connection.execute(f'DELETE FROM {name_table} WHERE ' + conditer)


    