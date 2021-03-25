from io import StringIO

import psycopg2
import sqlparse
import logging

from column_index import ColumnIndex
from special_columns import bool_columns

logger = logging.getLogger('root')


class Postgres(object):
    def __init__(self):
        self.ora_to_pg_type_mapping = {
            'NUMBER': 'numeric',
            'CHAR': 'char',
            'VARCHAR': 'varchar',
            'VARCHAR2': 'varchar',
            'NVARCHAR2': 'varchar',
            'DATE': 'timestamp',
            'CLOB': 'text',
            'NCLOB': 'text',
            'BLOB': 'bytea',
            'TIMESTAMP': 'timestamp',
            "TIMESTAMP(0)": 'timestamp'
        }
        self.ora_to_pg_index_mapping = {
            'normal': 'btree',
            'bitmap': 'bitmap'
        }

    def init_conn(self, host, username, password, database):
        """
        初始化连接
        :param host: 主机
        :param username: 用户名
        :param password: 密码
        :param database: 数据库
        :return:
        """
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.db = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            database=database)

    def _get_tables(self):
        """
            获取所有表
        """
        cursor = self.db.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        rows = cursor.fetchall()
        tables = []
        for row in rows:
            tables.append(row[0])
        cursor.close()
        return tables

    def _drop_table(self, table_name):
        """
        删除指定表
        """
        cursor = self.db.cursor()
        cursor.execute(f"drop table if exists {table_name} CASCADE")
        cursor.close()

    def drop_all_tables(self):
        """
        删除所有表
        :return:
        """
        tables = self._get_tables()
        for table in tables:
            self._drop_table(table)

    def create_table(self, table_name: str, columns, primary_key: str):
        self._drop_table(table_name)
        logger.info(f'create table {table_name.lower()}')
        sql = f"create table {table_name.lower()}("
        column_types = []
        for i in range(len(columns)):
            column = columns[i]
            column_name = column.column_name
            if column_name in bool_columns:
                column_type = 'bool'
            else:
                column_type = self.convert_type(column.datatype, column.char_length, column.data_precision,
                                                column.data_scale)
            column_types.append(column_type)
            sql += f"{column.column_name} {column_type} "
            sql += f"{column.get_nullable()} "
            if column.data_default is not None:
                sql += column.get_default(column_type)
            if primary_key.lower() == column.column_name.lower():
                sql += "PRIMARY KEY"
            if i != len(columns) - 1:
                sql += ","
        sql += ")"
        sql = sqlparse.format(sql, reindent=True, keyword_case='lower')
        logger.debug(f"create table {table_name.lower()} ddl: {sql}")
        with self.db.cursor() as cursor:
            cursor.execute(sql)
        return column_types

    def migrate_data(self, table_name, data_csv):
        """
        导入csv格式数据
        :param table_name:
        :param data_csv:
        :return:
        """
        sio = StringIO()
        sio.write(data_csv.replace("\u0000", ""))
        sio.seek(0)
        with self.db.cursor() as cursor:
            try:
                cursor.execute("SET CLIENT_ENCODING TO 'UTF8';")
                cursor.copy_expert(f"""COPY {table_name} FROM STDIN DELIMITER '|' CSV HEADER""", sio)
                self.db.commit()
            except Exception as e:
                self.db.rollback()
                logger.error(f"migrate data failed {table_name}: {e} ❌")
                with open(f'{table_name}.csv', 'w') as f:
                    f.write(data_csv)

    def migrate_index(self, table_name, primary_key, column_indexes):
        logger.debug(f"migrate index for {table_name}")
        for column_index in column_indexes:
            column_name = column_index.column_name.lower()
            # 主键无需创建索引
            if primary_key.lower() == column_name:
                continue
            # 暂时不处理函数索引
            if 'function-based' in column_index.index_type:
                logger.warning(f"ignore function based index {column_index.column_name}")
                continue
            with self.db.cursor() as cursor:
                # index_type = self.ora_to_pg_index_mapping[column_index.index_type]
                # 默认采用btree索引，对于等值运算优先hash索引
                cursor.execute(f"""drop index IF EXISTS idx_{table_name}_{column_name}""")
                cursor.execute(f"""create index idx_{table_name.lower()}_{column_name} on {table_name} ({column_name})""")

    def convert_type(self, datatype, char_length, data_precision, data_scale):
        # 数值类型
        if datatype == 'NUMBER':
            # 整型
            if data_scale == 0 or data_scale is None:
                if data_precision is None:
                    return "smallint"
                # > 10 使用bigint
                elif data_precision >= 10:
                    return "bigint"
                elif data_precision == 1:
                    return "bool"
                return 'smallint' if data_precision <= 5 else 'integer'
            else:
                return f'numeric({data_precision}, {data_scale})'
        elif "TIMESTAMP" in datatype:
            return f"timestamp({char_length})"
        convert_type = self.ora_to_pg_type_mapping.get(datatype)
        if convert_type in ['text', 'bytea']:
            return convert_type
        return f"{convert_type}({char_length})"
