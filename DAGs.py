from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import os
from os.path import expanduser, join


class con_pg:
    """
    default value:
    port=5432,
    host='db',
    dbname='my_database',
    user='root',
    password='postgres',
    delimiter=',',
    file_type='CSV'
    """

    def __init__(self, port=5432,
                 host='db',
                 dbname='my_database',
                 user='root',
                 password='postgres',
                 delimiter=',',
                 file_type='CSV'):
        self.conn_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
        self.__tables = self.__get_table_name()
        self.__delimiter = delimiter
        self.__file_type = file_type
        self.local_path = join(expanduser("~"), 'airflow_data/')
        self.file = [f'{self.local_path}resultsfile_table_', f'.{self.__file_type.lower()}']

    def __get_table_name(self):
        with psycopg2.connect(self.conn_string) as conn, conn.cursor() as cursor:
            sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            cursor.execute(sql)
            tables = cursor.fetchall()
            table_list = []
            for table in tables:
                table_list.append(table[0])
        return table_list

    def read_from_db(self):
        with psycopg2.connect(self.conn_string) as conn, conn.cursor() as cursor:
            if not os.path.exists(self.local_path):  # Если пути не существует создаем его
                os.makedirs(self.local_path)
            for el in self.__tables:
                filename = self.file[0] + str(el) + self.file[1]
                q_from = f"COPY {el} TO STDOUT WITH DELIMITER '{self.__delimiter}' {self.__file_type} HEADER;"
                with open(filename, 'w') as file:
                    cursor.copy_expert(q_from, file)
                    print(f'данные из таблицы {el} записаны в файл {filename}')

    def write_to_db(self):
        with psycopg2.connect(self.conn_string) as conn, conn.cursor() as cursor:
            for el in self.__tables:
                filename = self.file[0] + str(el) + self.file[1]
                # filename = join(self.file[0], str(el), self.file[1])
                q_to = f"COPY {el} FROM STDIN WITH DELIMITER '{self.__delimiter}' {self.__file_type} HEADER;"
                with open(filename, 'r') as file:
                    cursor.copy_expert(q_to, file)
                    conn.commit()
                    print(f'файл {filename} загружен в таблицу {el}')
                    if os.path.isfile(filename):
                        os.remove(filename)


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 13),
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}


def _read_data():
    pg_source = con_pg(host='db2')
    pg_source.read_from_db()


def _write_data():
    pg_target = con_pg(host='db')
    pg_target.write_to_db()


with DAG(
    dag_id="copy-data-flow-pg",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as post_dag:
    read_data = PythonOperator(
        task_id="read_data",
        python_callable=_read_data,
        dag=post_dag,
    )

    write_data = PythonOperator(
        task_id="write_data",
        python_callable=_write_data,
        dag=post_dag,
    )

    read_data >> write_data


if __name__ == '__main__':
    _read_data()
    print('data is read')
    _write_data()
    print('data writed')
    print()
