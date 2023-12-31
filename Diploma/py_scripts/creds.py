tables_info = {
    'TRANSACTIONS': {
        'std': './sql_scripts/create_STG_TRANSACTIONS.sql',
        'dwh_create': './sql_scripts/create_DWH_FACT_TRANSACTIONS.sql',
        'dwh_insert': './sql_scripts/insert_DWH_FACT_TRANSACTIONS.sql',
    },
    'CUSTOMER_ADDRESS': {
        'std': './sql_scripts/create_STG_CUSTOMER_ADDRESS.sql',
        'dwh_create': './sql_scripts/create_DWH_DIM_CUSTOMER_ADDRESS.sql',
        'dwh_insert': './sql_scripts/insert_DWH_DIM_CUSTOMER_ADDRESS.sql',
    },
    'CUSTOMER_DEMOGRAPHIC': {
        'std': './sql_scripts/create_STG_CUSTOMER_DEMOGRAPHIC.sql',
        'dwh_create': './sql_scripts/create_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql',
        'dwh_insert': './sql_scripts/insert_DWH_DIM_CUSTOMER_DEMOGRAPHIC.sql',
    },
    'CUSTOMERS': {
        'dwh_create': './sql_scripts/create_DWH_DIM_CUSTOMERS.sql',
        'dwh_insert': './sql_scripts/insert_DWH_DIM_CUSTOMERS.sql',
    },
    'PRODUCTS': {
        'dwh_create': './sql_scripts/create_DWH_DIM_PRODUCTS.sql',
        'dwh_insert': './sql_scripts/insert_DWH_DIM_PRODUCTS.sql',
    },
    'FULL_TRANSACTIONS': {
        'dwh_create': './sql_scripts/create_DWH_FULL_TRANSACTIONS.sql',
        'dwh_insert': './sql_scripts/insert_DWH_FULL_TRANSACTIONS.sql',
    },
}

DB_PATH = 'mydb.db'
