import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG,
                    filename='myapp.log',
                    format='%(asctime)s %(levelnmae)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Connection:

    def __init__(self, conn_path: str) -> None:
        self.conn_path = conn_path

    def _create_connection(self):
        return sqlite3.connect(self.conn_path)

    @staticmethod
    def open_file(file_path: str) -> str:
        with open(file_path, 'r') as f:
            file = f.read()
        return file

    def read_sql_script(self, sql_path: str, file_script: bool = True) -> None:
        conn = self._create_connection()
        try:
            cur = conn.cursor()
            if file_script:
                sql = self.open_file(sql_path)
            else:
                sql = sql_path
            cur.executescript(sql)
            conn.commit()
        except IOError as io_err:
            print(f'IOError: {io_err}')
            logger.info(io_err)
        except sqlite3.Error as db_err:
            print(f'DBError: {db_err}')
            logger.info(db_err)
        finally:
            conn.close()

    def insert_data(self, table: str, data: pd.DataFrame) -> None:
        data = data.astype('str')
        conn = self._create_connection()

        query = f"""INSERT INTO {table}({",".join(list(data))})
             VALUES ({",".join(['?' for x in range(data.shape[1]) if x != 
             'default'])})"""

        try:
            curr = conn.cursor()
            curr.executemany(query, list(data.to_records(index=False)))
            conn.commit()
        except IOError as io_err:
            print(f'IOError: {io_err}')
        except sqlite3.Error as db_err:
            print(f'DBError: {db_err}')
            logger.info(db_err)
        finally:
            conn.close()
