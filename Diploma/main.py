import datetime
import os
import shutil
import logging


logging.info('importing')
from py_scripts.utils import (
    create_db,
    create_new_dwh,
    download_data,
    stg_and_dwh,
)

logging.info("Starting processing")

DATASET_NAME = 'aungpyaeap/supermarket-sales'
FILE_NAME = 'supermarket_sales - Sheet1.csv'
CUR_DT = str(datetime.datetime.now().date())


def main():

    input_path = 'input_data/'
    archive_path = 'archive/'

    create_db()
    download_data(DATASET_NAME, input_path)

    input_files = os.listdir(input_path)
    archive_files = os.listdir(archive_path)

    new_files = [x for x in input_files if x
                 + f'-{CUR_DT}.dump' not in archive_files]
    
    for i in new_files:
        stg_and_dwh(input_path, i)
        shutil.copy(f"{input_path}{i}", f"{archive_path}{i}.dump")

    create_new_dwh()


if __name__ == '__main__':
    main()

    logging.info("Finished prediction")
