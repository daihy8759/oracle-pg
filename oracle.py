import logging

import cx_Oracle

from column import Column

logger = logging.getLogger('root')


class Oracle(object):
    def __init__(self):
        self.page_size = 10000

    def init_conn(self, host, username, password, database):
        """
        初始化连接
        :return:
        """
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.db = cx_Oracle.connect(username, password, f'{host}/{database}')

    def get_tables(self):
        """
            获取所有表
        """
        cursor = self.db.cursor()
        cursor.execute("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")
        rows = cursor.fetchall()
        tables = []
        for row in rows:
            tables.append(row[0])
        cursor.close()
        return tables

    def get_primary_key(self, table_name, owner: str):
        """
        获取主键
        :param table_name:
        :param owner:
        :return:
        """
        cursor = self.db.cursor()
        cursor.execute(f"""SELECT
	        column_name
            FROM
	        all_cons_columns
            WHERE
        constraint_name = (SELECT constraint_name FROM user_constraints WHERE UPPER( table_name) = '{table_name}'
        AND CONSTRAINT_TYPE = 'P' )
        AND owner = '{owner.upper()}'""")
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row is not None else ""

    def count_table(self, table_name):
        cursor = self.db.cursor()
        cursor.execute(f"select count(1) from {table_name}")
        count_row = cursor.fetchone()
        total_count = count_row[0]
        cursor.close()
        return total_count

    def get_data_with_csv_format(self, table_name, page):
        cursor = self.db.cursor()
        data = []
        cursor.execute(f"""select a.* from (
            select t.*,rownum rowno from {table_name} t where rownum <= {page * self.page_size} ) a
            where a.rowno >= {(page - 1) * self.page_size + 1}""")
        rows = cursor.fetchall()
        columns_types = []
        column_names = []
        for column in cursor.description:
            column_names.append(column[0].lower())
            columns_types.append(column[1])
        # 移除row num 列
        del column_names[-1]
        data.append('|'.join(column_names))
        for row in rows:
            csv = ''
            for idx in range(len(row)):
                # 最后一列为行号
                if idx == len(row) - 1:
                    continue
                if columns_types[idx] is cx_Oracle.NUMBER:
                    csv += f"{row[idx]}|" if row[idx] is not None else "|"
                elif columns_types[idx] is cx_Oracle.DATETIME:
                    csv += f"{row[idx].strftime('%m/%d/%Y %H:%M:%S')}|" if row[idx] is not None else "|"
                elif columns_types[idx] is cx_Oracle.CLOB or columns_types[idx] is cx_Oracle.NCLOB \
                        or columns_types[idx] is cx_Oracle.BLOB:
                    csv += f"\"{row[idx].read()}\"|" if row[idx] is not None else "|"
                else:
                    csv += f"\"{row[idx]}\"|" if row[idx] is not None else "|"
            csv = csv[0: len(csv) - 1]
            data.append(csv)
        cursor.close()
        return "\n".join(data)

    def get_table_structure(self, table_name):
        """
        获取指定表结构信息
        """
        cursor = self.db.cursor()
        cursor.execute(f"""select t.TABLE_NAME     AS tableName,
               t.COLUMN_NAME    AS columnName,
               c.COMMENTS       AS columnComment,
               t.NULLABLE       AS nullable,
               t.DATA_DEFAULT,
               t.DATA_TYPE      AS dataType,
               t.CHAR_LENGTH    AS strLength,
               t.DATA_PRECISION AS numLength,
               t.DATA_SCALE     AS numBit
              from user_tab_columns t, user_col_comments c
             where t.TABLE_NAME = c.TABLE_NAME
               and t.COLUMN_NAME = c.COLUMN_NAME
               and t.TABLE_NAME = '{table_name}'
             order by t.TABLE_NAME, t.COLUMN_ID""")
        columns = []
        rows = cursor.fetchall()
        for row in rows:
            columns.append(Column(column_name=row[1].lower(), comments=row[2], nullable=row[3],
                                  data_default=row[4], datatype=row[5], char_length=row[6],
                                  data_precision=row[7], data_scale=row[8]))
        cursor.close()
        return columns
