import pandas as pd
from py_scripts.connection import Connection


class EtlTask:

    def __init__(self, db_path):
        self.db_path = db_path

    def initialize_stg(self, stg_tbl_name: str) -> None:
        connect = Connection(self.db_path)
        connect.read_sql_script(f'sql_scripts/create_{stg_tbl_name}.sql')

    def ist_data_to_stg(self, stg_tbl_name: str, data: pd.DataFrame) -> None:
        connect = Connection(self.db_path)
        connect.insert_data(stg_tbl_name, data)

    def make_dwh(self, dwh_tbl_name: str) -> None:
        connect = Connection(self.db_path)
        connect.read_sql_script(dwh_tbl_name)
