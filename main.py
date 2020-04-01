import math
import os
import sys

from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QMainWindow, QApplication, QWidget, QVBoxLayout

import log
from oracle import Oracle
from postgres import Postgres

# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
logger = log.setup_custom_logger('root')


class MainWindow():
    def __init__(self):
        super().__init__()
        self.source_ds = Oracle()
        self.dest_ds = Postgres()
        self.init_ui()

    def init_ui(self):
        # self.setGeometry(300, 300, 680, 460)
        # self.setWindowTitle('数据迁移工具')
        # if getattr(sys, 'frozen', False):
        #     bundle_dir = sys._MEIPASS
        # else:
        #     bundle_dir = os.path.dirname(os.path.abspath(__file__))
        # self.setWindowIcon(QIcon(bundle_dir + '/ico.ico'))
        # self.central_widget = QWidget()  # define central widget
        # self.setCentralWidget(self.central_widget)
        # self.layout = QVBoxLayout()
        # self.layout.setSpacing(1)
        # self.central_widget.setLayout(self.layout)
        # self.show()
        self.source_ds.init_conn('192.168.88.52', 'bztest', 'bztest', 'orcl')
        self.dest_ds.init_conn('192.168.88.181', 'postgres', 'postgres', 'bz')
        self.mirgrate_all()

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
        columns = self.source_ds.get_table_structure(table_name)
        primary_key = self.source_ds.get_primary_key(
            table_name, self.source_ds.username)
        self.dest_ds.create_table(table_name, columns, primary_key)
        total_count = self.source_ds.count_table(table_name)
        total_page = math.ceil(float(total_count) / self.source_ds.page_size)
        for page in range(total_page):
            data = self.source_ds.get_data_with_csv_format(
                table_name, page + 1)
            self.dest_ds.migrate_data(table_name, data)
        logger.info(f'migrate {table_name} complete ❇️')


if __name__ == '__main__':
    # app = QApplication(sys.argv)
    mainWindow = MainWindow()
    # sys.exit(app.exec_())
