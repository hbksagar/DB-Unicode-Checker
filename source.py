from sqlalchemy import MetaData, create_engine, select, String, text
from sqlalchemy.sql import table, literal_column
import json

# Replace with database name
db_name = 'db_name'
db_username = 'root'
db_password = ''
db_host = 'localhost'
db_base = 'mysql://{0}:{1}@{2}'.format(db_username, db_password, db_host)
db_config = '{0}/{1}'.format(db_base, db_name)

# destination data type
d_type = 'ascii'

def build_condition(columns):
    conditions = []
    for column in columns:
        conditions.append(
            "{0} != CONVERT({0} USING {1})".format(column, d_type)
        )
    return " or ".join(conditions)

def prepare_sql(table, columns):
    return select([
        text(','.join(columns))
    ]).\
    where(build_condition(columns)).\
    select_from('{0}.{1}'.format(db_name, table))

def non_ascii_data(table, columns):
    sql = prepare_sql(table, columns)
    try:
        result = engine.execute(sql)
        keys = result.keys()
        data_all =[]
        for row in result:
            data = {}
            for index, key in enumerate(keys, start=0):
                if row[index]:
                    data[key] = row[index]
            data_all.append(data)
        return data_all
    except Exception as e:
        print e.message

def non_ascii_data_per_table(tables):
    table_data = {}
    for table in tables:
        columns = map(lambda x: '{0}.{1}'.format(table, x), tables[table])
        data_all = non_ascii_data(table, columns)
        if len(data_all):
            table_data[table] = {}
            table_data[table]['data'] = data_all
            table_data[table]['count'] = len(data_all)
        
    return table_data

def fetch_tables_metadata(m):
    tables = {}
    for table in m.tables.values():
        string_cols = set()
        for column in table.c:
            if column.type.python_type is str:
                string_cols.add(column.name)
        if string_cols:
            tables[table.name] = string_cols
    return tables

def get_engine(db_config):
    return create_engine(
        db_config,
        convert_unicode=True
    )

engine = get_engine(db_config)
m = MetaData()
m.reflect(engine)
tables = fetch_tables_metadata(m)
print non_ascii_data_per_table(tables)
