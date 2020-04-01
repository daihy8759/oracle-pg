from io import StringIO

import psycopg2
import sqlparse
import logging

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
        cursor.execute(f"drop table if exists {table_name}")
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
        for i in range(len(columns)):
            column = columns[i]
            sql += f"{column.column_name} {self.convert_type(column.datatype, column.char_length, column.data_precision, column.data_scale)}  "
            sql += f"{column.get_nullable()} "
            if column.data_default is not None:
                sql += column.get_default()
            if primary_key.lower() == column.column_name.lower():
                sql += "PRIMARY KEY"
            if i != len(columns) - 1:
                sql += ","
        sql += ")"
        sql = sqlparse.format(sql, reindent=True, keyword_case='lower')
        logger.debug(f"create table {table_name.lower()} ddl: {sql}")
        with self.db.cursor() as cursor:
            cursor.execute(sql)

    def migrate_data(self, table_name, data_csv):
        """
        导入csv格式数据
        :param table_name:
        :param data_csv:
        :return:
        """
        sio = StringIO()
        sio.write(data_csv)
        sio.seek(0)
        with self.db.cursor() as cursor:
            try:
                cursor.copy_expert(f"""COPY {table_name} FROM STDIN DELIMITER '|' CSV HEADER""", sio)
                self.db.commit()
            except Exception as e:
                self.db.rollback()
                logger.error(f"migrate data failed {table_name}: {e}")

    def convert_type(self, datatype, char_length, data_precision, data_scale):
        # 数值类型
        if datatype == 'NUMBER':
            # 整型
            if data_scale == 0 or data_scale is None:
                return 'smallint' if data_precision == 1 else 'integer'
            else:
                return f'numeric({data_precision}, {data_scale})'
        elif "TIMESTAMP" in datatype:
            return f"timestamp({char_length})"
        convert_type = self.ora_to_pg_type_mapping.get(datatype)
        if convert_type in ['text', 'bytea']:
            return convert_type
        return f"{convert_type}({char_length})"
