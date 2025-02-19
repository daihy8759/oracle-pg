from dataclasses import dataclass


@dataclass
class Column(object):
    column_name: str
    comments: str
    data_default: str
    nullable: bool
    datatype: str
    char_length: int
    data_precision: int
    data_scale: int

    def get_nullable(self):
        return "null" if self.nullable == 'Y' else "not null"

    def get_default(self, column_type):
        if column_type == 'bool':
            return 'default True' if self.data_default == 1 else 'default False'
        return "" if self.data_default is None else f"default {self.data_default}"
