from dataclasses import dataclass


@dataclass
class ColumnIndex(object):
    column_name: str
    descend: str
    index_type: str
