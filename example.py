from api import DB_api
import pandas as pd
from sqlalchemy import create_engine


with open ('data.txt', 'r', encoding='UTF-8') as file:
    text = file.readlines()

data = text[0].split(' ')
engine = create_engine(f'postgresql+psycopg2://{data[1]}:{data[2]}@{data[0]}/{data[3]}')



df = pd.DataFrame([['Earth', 1], ['Moon', 0.606], ['Mars', 0.107]], columns=['name', 'mass_to_earth'])


test = DB_api(engine)

# Создание таблицы
# test.create_table('kek', df)


# Инсёрт
# test.insert_sql('kek', df, 'append')


# Чтение данных из таблицы
# df_read = test.read_sql('kek')
# print(df_read)

# Удаление данных из таблицы
# test.truncate_table('kek')


# Выполнение произвольного SQL-запроса
# query = "insert into kek VALUES ('Jupiter', 0.123)"
# result = test.execute(query)
# print(result)



# Удаление данных из таблицы
# condition = {"name": "Mars", "mass_to_earth" : 0.606}
# test.delete_from_table('kek', condition)
# print(f"Удалена запись из таблицы")



