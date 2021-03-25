import math
import os
import sys

import log
from oracle import Oracle
from postgres import Postgres

# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
logger = log.setup_custom_logger('root')

ignore_tables = ['tb_document_third']
include_tables = []


class MainWindow():
    def __init__(self):
        super().__init__()
        self.source_ds = Oracle()
        self.dest_ds = Postgres()
        self.init_ui()

    def init_ui(self):
        self.source_ds.init_conn('192.168.88.52', 'bztest', 'bztest', 'orcl')
        self.dest_ds.init_conn('192.168.88.181', 'postgres', 'postgres', 'bz')
        self.mirgrate_all()

    def backup_views(self):
        """
        备份视图
        :return:
        """

    def mirgrate_all(self):
        tables = self.source_ds.get_tables()
        for table_name in tables:
            self.mirgrate(table_name)

    def mirgrate(self, table_name):
        """
        迁移指定表
        :return:
        """
        logger.info(f'migrate {table_name} start ⚡️')
        lower_table = table_name.lower()
        if len(include_tables) == 0 or lower_table in include_tables:
            columns = self.source_ds.get_table_structure(table_name)
            primary_key = self.source_ds.get_primary_key(
                table_name, self.source_ds.username)
            dest_column_types = self.dest_ds.create_table(table_name, columns, primary_key)
            column_indexes = self.source_ds.get_table_index(table_name)
            self.dest_ds.migrate_index(table_name, primary_key, column_indexes)
            if lower_table not in ignore_tables:
                total_count = self.source_ds.count_table(table_name)
                total_page = math.ceil(float(total_count) / self.source_ds.page_size)
                for page in range(total_page):
                    data = self.source_ds.get_data_with_csv_format(
                        table_name, dest_column_types, page + 1)
                    self.dest_ds.migrate_data(table_name, data)
                logger.info(f'migrate {table_name} complete ❇️')


if __name__ == '__main__':
    mainWindow = MainWindow()
