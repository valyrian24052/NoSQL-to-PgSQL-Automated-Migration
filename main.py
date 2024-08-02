from insert_table import insert_main
from Resources import get_mongo_schema

from input import Mongoconnstr


def main():
    schema = get_mongo_schema(Mongoconnstr)

    for table in schema.keys():
        insert_main(table, schema)


if __name__ == '__main__':
    main()
