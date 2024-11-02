# _______________________ test_database_con_querry_v03.py _______________________


from airflow import DAG # Базовый импорт

from airflow.operators.dummy_operator          import DummyOperator  # Оператор пустышка
from airflow.operators.python_operator         import PythonOperator # Оператор выполняетт функции написанные на Python
from airflow.providers.postgres.hooks.postgres import PostgresHook   # Встроенный коннектор к БД на базе Postgres

from datetime import datetime
from datetime import date

import pandas as pd
import os

# Библиотека для соединения с БД если не использовать встроенный коннектор PostgresHook
# import psycopg2


# Объявление переменных

today = date.today()
now   = datetime.now()

cur_dir    = os.getcwd()         # Получаем путь рабочей директории, по умолчанию это /opt/airflow/
files_list = os.listdir(cur_dir) # Смотрим что содержиться в рабочей папке

source_dir      = '/data/airflow/_source/'      # Для удобства можно установить пути Истоник/Получатель
destination_dir = '/data/airflow/_destination/' # Для удобства можно установить пути Истоник/Получатель



# Параметры DAG

dag_id       = os.path.basename(__file__).replace(".py", "") 
                                      # Формируем название DAG из имени файла, без расширения .py
tags         = ['test_dags', 'debug'] # Устанавливаем теги, по ним удобно фильтровать даги в Airflow
default_args = {'owner': 'dakosar3'}  # Устанавливаем ответственного за данный DAG




# Функция для теста
def say_hello():
    print('-' * 50)
    print('Hello from my function\n')
    print('Сегодня:     ', today)
    print('Точное время:', now)
    print('-' * 50)
    print('Рабочая директория:', cur_dir)
    print('Список файлов в рабочей директории:')
    print(files_list)
    
    print('Директория чтения файла файла:', source_dir)
    print(f'Полный путь до читаемого файла: {source_dir}test_df.csv')
    print('-' * 50)
    
    print('Просмотр директории /data/airflow/')
    print(os.listdir('/data/airflow/'))


# Функция коннектится к БД с помощью встроенного коннектора
# Выполняет запрос
# Записывает результат запроса сюда f'{destination_dir}{today}_kion_df.csv'
def read_dataset():

    PG_CONN_ID = "conn_name_in_airflow_connectors"  # имя коннектора для PostgresHook. Используется вместо psycopg2
    
    # Параметры подключения через psycopg2
    #db_config = {
    #            'host':     '', 
    #            'port':     '',
    #            'user':     '',       
    #            'password': '',
    #            'database': ''
    #            }
        
    with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn: # соединение через коннектор
    
    #для для соединения через psycopg2 вместо коннектора используем строчку ниже
    #with psycopg2.connect(**db_config) as conn:
        
        print("-" * 100, "\n", "Подключение к базе данных - Успешно!")

        # Собственно запрос
        querry = f"""
            SELECT *
            FROM prd_mediatv_v_user.rep_substn_sales_oper
            LIMIT 5
            ;
        """
    
        cur = conn.cursor() # Инициализируем курсор
        cur.execute(querry) # Выполняем запрос с помощью курсора

        print("-" * 100, "\n", "Выполнение запроса - Успешно!")
                
        result = cur.fetchall() # Получаем результат запроса в переменную
        
        df = pd.DataFrame(result, columns=[col[0] for col in cur.description])
                                      # [col[0] for col in cur.description] считывает заголовки полей в виде списка
                                      
                                      # cur.description - возвращает список кортежей содержащих инфу о полях запроса
                                      # 0 - имя поля
                                      # 1 - тип данных поля
                                      # 2 - размер поля
                                      # 3 - индекс поля (порядковый номер)
                                      # 4 - флаг наличия NULL
                                      # 5 - имя таблицы, которой принадлежит поле
                                      # 6 - имя схемы в которой лежит таблица
                                      
        
        print("-" * 100, "\n", "Получение результата запроса - Успешно!")
        
        df.to_csv(
                  f'{destination_dir}{today}_kion_df.csv', 
                  sep=',', 
                  index=False,
                  header=True,
                  encoding='utf-8-sig' 
                 )
    
        print("-" * 100, "\n", "Запись CSV - Успешно!")



# Инициализируем DAG

dag = DAG(dag_id,                                    # Имя дага, отображается в списке Airflow
          default_args     = default_args,           # распаковываем словарь с аргументами из переменной
          tags             = tags,                   # теги из переменной
          description      = 'test database_read',   # Описание дага
          schedule_interval= '@daily',               # Расписание запуска дага
          start_date       = datetime(2024, 10, 20), # Дата старта дага
          catchup          = False                   # Если текущая дата больше стартовой, 
                                                     # флаг указывает, надо ли прогонять даг по расписанию 
                                                     # от start_date до текущей
                                          
          )

# Инициализация задач
hello_task = PythonOperator(task_id='hello_task',      # имя задачи в грАфе Airflow
                            python_callable=say_hello, # Вызываем функцию написанную выше
                            dag=dag)                   # даг к которому относится оператор. По умолчанию пишем текущий
                                                       # если нам не требуется связывать этот оператор с другими дагами

read_task = PythonOperator(task_id='read_task', 
                           python_callable=read_dataset,
                           dag=dag)   

# Формируем порядок запуска задач
hello_task >> read_task


# Задачи можно параллелить если они друг другу не мешают 
# Например:
#               task_1 >> task_2 >> task_3 >> [task_4, task_5, task_6] >> task_7

# Порядок можно указывать в столбик, для этого обертываем список в скобки
# (
#        task_1
#     >> task_2
#     >> task_3
#     >> [task_4, task_5, task_6]
#     >> task_7
# )



# _______________________ test_dag_v17.py _______________________


from airflow import DAG
from airflow.operators.dummy_operator  import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
from datetime import date

import pandas as pd
import os

import psycopg2

# Объявление переменных

today = date.today()
now   = datetime.now()

cur_dir    = os.getcwd()
files_list = os.listdir(cur_dir)

source_dir      = '/data/airflow/_source/'
destination_dir = '/data/airflow/_destination/'



# Параметры DAG

dag_id       = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
tags         = ['test_dags', 'debug']
default_args = {'owner': 'dakosar3'}





def say_hello():
    print('-' * 50)
    print('Hello from my function\n')
    print('Сегодня:     ', today)
    print('Точное время:', now)
    print('-' * 50)
    print('Рабочая директория:', cur_dir)
    print('Список файлов в рабочей директории:')
    print(files_list)
    
    print('Директория чтения файла файла:', source_dir)
    print(f'Полный путь до читаемого файла: {source_dir}test_df.csv')
    print('-' * 50)
    
    print('Просмотр директории /data/airflow/')
    print(os.listdir('/data/airflow/'))

def read_dataset():
    sample_df = pd.read_csv(f'{source_dir}test_df.csv')
    
    print('-' * 50)
    print(f'Датасет {source_dir}test_df.csv:\n')
    print('\n\n',sample_df)
    print('-' * 50)
        

def dataset_write():
    data = {
            'name_directory': ['cur_dir', 'destination_dir'],
            'path_directory': [cur_dir, destination_dir],
            'random_numbers': [22, 55]
            }
    df = pd.DataFrame(data)
    
    print('-' * 50)
    print('\nДатасет:\n')
    print(df)
    
    df.to_csv(f'{destination_dir}{today}_test_df.csv')
    
    print('-' * 50)
    print(f'Датасет записан в файл: {destination_dir}{today}_test_df.csv')
    print('-' * 50)



# Инициализируем DAG

dag = DAG(dag_id, 
          default_args     = default_args,
          tags             = tags,
          description      = 'Test daily DAG', 
          schedule_interval= '@daily', 
          start_date       = datetime(2024, 10, 20), 
          catchup          = False          
          )

dummy_task = DummyOperator(task_id='dummy_task', 
                           retries=3,
                           dag=dag)

hello_task = PythonOperator(task_id='hello_task', 
                            python_callable=say_hello,
                            dag=dag)

read_task = PythonOperator(task_id='read_task', 
                           python_callable=read_dataset,
                           dag=dag)   

write_task = PythonOperator(task_id='write_task', 
                            python_callable=dataset_write,
                            dag=dag)                            

dummy_task >> hello_task >> read_task >> write_task



