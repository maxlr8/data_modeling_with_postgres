from create_db_tables import main as create_tables_main
from etl import main as etl_main

if __name__ == "__main__":
    create_tables_main()
    etl_main()
