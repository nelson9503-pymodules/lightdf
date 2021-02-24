from .column_object import Column
from datetime import datetime
import csv


class Dataframe:

    def __init__(self, key_col_name: str, key_col_type: type):
        """
        types: int, float, str, bool, None(Not control data type)
        """
        self.key_column_name = key_col_name
        self.key_column_type = key_col_type
        self.__key_column = Column(key_col_type, True, True, False)
        self.__columns = {}
        self.__next_id = 0

    def add_col(self, col_name: str, col_type: type, unique: bool = False, none_value: bool = True):
        if col_name in self.__columns or col_name == self.key_column_name:
            raise KeyError("Duplicated column name.")
        self.__columns[col_name] = Column(col_type, False, unique, none_value)
        ids = self.__columns[col_name].list_id()
        key_ids = self.__key_column.list_id()
        for id in key_ids:
            if not id in ids:
                self.__columns[col_name].initialize_value(id)

    def drop_col(self, col_name: str):
        if not col_name in self.__columns:
            raise KeyError("Dropping column not exists.")
        del self.__columns[col_name]

    def write(self, key_value: any, col_name: any, value: any):
        id = self.__key_column.get_key(key_value)
        if id == None:
            id = self.__next_id
            self.__next_id += 1
            self.__key_column.update_value(id, key_value)
            for col in self.__columns:
                self.__columns[col].initialize_value(id)
        if not col_name in self.__columns:
            raise KeyError("Column not exists.")
        self.__columns[col_name].update_value(id, value)

    def read(self, key_value: any, col_name: any) -> any:
        id = self.__key_column.get_key(key_value)
        if id == None:
            raise KeyError("Key value not exists.")
        if not col_name in self.__columns:
            raise KeyError("Column not exists.")
        return self.__columns[col_name].get_value(id)

    def drop(self, key_value: any):
        id = self.__key_column.get_key(key_value)
        if id == None:
            raise KeyError("Key value not exists.")
        self.__key_column.drop_value(id)
        for col_name in self.__columns:
            self.__columns[col_name].drop_value(id)

    def set_unique(self, col_name: any, unique: bool):
        if col_name == self.key_column_name:
            raise AttributeError("Key column cannot use unique setting.")
        if not col_name in self.__columns:
            raise KeyError("Column not exists.")
        self.__columns[col_name].set_unique_column(unique)

    def set_none(self, col_name: any, none: bool):
        if col_name == self.key_column_name:
            raise AttributeError("Key column cannot use none setting.")
        if not col_name in self.__columns:
            raise KeyError("Column not exists.")
        self.__columns[col_name].set_none_value(none)

    def set_col_type(self, col_name: any, data_type: type):
        """
        types: int, float, str, bool, None(Not control data type)
        """
        if not data_type in [int, float, str, bool, None]:
            raise TypeError("Invalid data type")
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        if col_name == self.key_column_name:
            self.__key_column.convert_value_type(data_type)
        else:
            self.__columns[col_name].convert_value_type(data_type)

    def set_key_col(self, col_name: any, keep: bool = True):
        if col_name == self.key_column_name:
            return
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        if keep == True:
            self.__key_column.set_key_col(False)
            self.__columns[self.key_column_name] = self.__key_column
        self.__columns[col_name].set_key_col(True)
        self.__key_column = self.__columns[col_name]
        self.key_column_name = col_name
        del self.__columns[col_name]

    def get_key_list(self) -> list:
        return self.__key_column.list_value()

    def get_col_list(self) -> list:
        return list(self.__columns.keys())

    def get_col_type(self, col_name: str) -> type:
        if col_name == self.key_column_name:
            return self.__key_column.data_type
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        return self.__columns[col_name].data_type

    def get_col_data(self, col_name: str) -> list:
        if col_name == self.key_column_name:
            return self.__key_column.list_value()
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        return self.__columns[col_name].list_value()

    def get_row_data(self, key_value: any) -> list:
        row_data = [key_value]
        for col_name in self.__columns:
            row_data.append(self.read(key_value, col_name))
        return row_data

    def to_list(self) -> list:
        li = []
        headers = [self.key_column_name]
        for col_name in self.__columns:
            headers.append(col_name)
        li.append(headers)
        for key_value in self.get_key_list():
            row = self.get_row_data(key_value)
            li.append(row)
        return li

    def to_dict(self) -> dict:
        d = {}
        for key_value in self.get_key_list():
            d[key_value] = {}
            for col_name in self.__columns:
                d[key_value][col_name] = self.read(key_value, col_name)
        return d

    def to_csv(self, save_path: str):
        li = self.to_list()
        with open(save_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(li)

    def print(self):
        header = [self.key_column_name]
        header.extend(self.get_col_list())
        print("header\t| {}".format(header))
        key_values = self.get_key_list()
        for i in range(5):
            if i >= len(key_values):
                return
            key_value = key_values[i]
            print("{}\t| {}".format(i, self.get_row_data(key_value)))
        if len(key_values) > 6:
            print("... ...")
        for i in range(len(key_values)-5, len(key_values)):
            if i < 5:
                continue
            key_value = key_values[i]
            print("{}\t| {}".format(i, self.get_row_data(key_value)))

    def from_list(self, li: list):
        col_names = list(self.__columns.keys())
        for row in li:
            if len(row) == 0:
                continue
            if self.__key_column.data_type == None:
                key_value = row[0]
            else:
                key_value = self.__key_column.data_type(row[0])
            for i in range(1, len(row)):
                self.write(key_value, col_names[i-1], row[i])

    def from_dict(self, d: dict):
        for key_value in d:
            for col_name in d[key_value]:
                self.write(key_value, col_name, d[key_value][col_name])

    def from_csv(self, csv_path: str, header: bool = True):
        with open(csv_path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            li = []
            for line in rows:
                li.append(line)
        if header == True:
            li = li[1:]
        self.from_list(li)

    def join_dataframe(self, df: object):
        if not df.key_column_name == self.key_column_name:
            raise KeyError("Can only join the dataframe with same key column.")
        if not df.key_column_type == self.key_column_type:
            raise KeyError("Can only join the dataframe with same key column.")
        for key_value in df.get_key_list():
            for col_name in df.get_col_list():
                if not col_name in self.get_col_list():
                    self.add_col(col_name, df.get_col_type(col_name))
                self.write(key_value, col_name, df.read(key_value, col_name))

    def filter_contains(self, col_name: any, contains: any) -> object:
        from .dataframe_object import Dataframe
        df = Dataframe(self.key_column_name,
                       self.get_col_type(self.key_column_name))
        for col in self.get_col_list():
            data_type = self.get_col_type(col)
            unique = self.__columns[col].unique
            none = self.__columns[col].none
            df.add_col(col, data_type, unique, none)
        col_names = self.get_col_list()
        for key_value in self.get_key_list():
            if contains in self.read(key_value, col_name):
                for col in col_names:
                    df.write(key_value, col,
                             self.read(key_value, col))
        return df

    def filter_range(self, col_name: any, greater_than: float, smaller_than: float) -> object:
        from .dataframe_object import Dataframe
        df = Dataframe(self.key_column_name,
                       self.get_col_type(self.key_column_name))
        for col in self.get_col_list():
            data_type = self.get_col_type(col)
            unique = self.__columns[col].unique
            none = self.__columns[col].none
            df.add_col(col, data_type, unique, none)
        col_names = self.get_col_list()
        for key_value in self.get_key_list():
            value = self.read(key_value, col_name)
            if value == None:
                continue
            if value > greater_than and value < smaller_than:
                for col in col_names:
                    df.write(key_value, col,
                             self.read(key_value, col))
        return df

    def drop_duplicated(self, col_name: any):
        check_dict = {}
        for key_value in self.get_key_list():
            value = self.read(key_value, col_name)
            if value in check_dict:
                self.drop(key_value)
            else:
                check_dict[value] = None

    def sort_col(self, col_name: any, ascending: bool = True):
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        if col_name == self.key_column_name:
            id_map = self.__key_column.sort_value(ascending)
            for col in self.__columns:
                self.__columns[col].reset_id(id_map)
        else:
            id_map = self.__columns[col_name].sort_value(ascending)
            self.__key_column.reset_id(id_map)
            for col in self.__columns:
                if col == col_name:
                    continue
                self.__columns[col].reset_id(id_map)

    def round_col(self, col_name: any, decimal: int):
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        for key_value in self.get_key_list():
            value = self.read(key_value, col_name)
            if value == None:
                continue
            self.write(key_value, col_name, round(value, decimal))

    def col_timestamp_to_datestring(self, col_name: any):
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        self.set_col_type(col_name, str)
        if col_name == self.key_column_name:
            for id in self.__key_column.data_dict:
                value = self.__key_column.get_value(id)
                d = datetime.fromtimestamp(int(value))
                self.__key_column.update_value(id, d.strftime("%Y-%m-%d"))
        else:
            for key_value in self.get_key_list():
                value = self.read(key_value, col_name)
                if value == None:
                    continue
                d = datetime.fromtimestamp(int(value))
                self.write(key_value, col_name, d.strftime("%Y-%m-%d"))

    def col_datestring_to_timestamp(self, col_name: any):
        if not col_name in self.__columns and not col_name == self.key_column_name:
            raise KeyError("Column not exists.")
        if col_name == self.key_column_name:
            for id in self.__key_column.data_dict:
                value = self.__key_column.get_value(id)
                d = datetime.strptime(value, "%Y-%m-%d")
                timestamp = int(d.timestamp())
                self.__key_column.update_value(id, timestamp)
        else:
            for key_value in self.get_key_list():
                value = self.read(key_value, col_name)
                if value == None:
                    continue
                d = datetime.strptime(value, "%Y-%m-%d")
                timestamp = int(d.timestamp())
                self.write(key_value, col_name, timestamp)
        self.set_col_type(col_name, int)
