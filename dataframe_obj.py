from .column_obj import Column
import csv


class Dataframe:

    def __init__(self, key_name: str, key_type: type):
        self.__keys = Column(key_name, key_type, True)
        self.__columns = {}

    def add_col(self, col_name: str, col_type: type, unique: bool = False, none: bool = True):
        if col_name in self.__columns:
            raise KeyError("Duplicated column name.")
        if col_name == self.__keys.name:
            raise KeyError("Column name duplicated to key column name.")
        self.__columns[col_name] = Column(col_name, col_type, unique, none)
        for _ in range(len(self.__keys.data)):
            self.__columns[col_name].append(None)

    def drop_col(self, col_name: str):
        if not col_name in self.__columns.keys():
            raise KeyError("column not exists.")
        del self.__columns[col_name]

    def set_col_type(self, col_name: str, data_type: type):
        self.__columns[col_name].convert_data_type(data_type)

    def list_col(self) -> list:
        return list(self.__columns.keys())

    def list_col_type(self) -> list:
        li = []
        for column in self.__columns:
            li.append(self.__columns[column].data_type)
        return li

    def list_keys(self) -> list:
        return self.__keys.data

    def get_keys_type(self) -> type:
        return self.__keys.data_type

    def set_keys_column(self, col_name: str, keep: bool = True):
        if keep == True:
            self.__columns[self.__keys.name] = self.__keys
        self.__columns[col_name].to_unique_column()
        self.__keys = self.__columns[col_name]
        del self.__columns[col_name]

    def write(self, row_key: any, col_key: any, value: any):
        if row_key in self.__keys.data:
            row = self.__check_row_num(row_key)
            self.__columns[col_key].update(row, value)
        else:
            row = len(self.__keys.data)
            self.__keys.append(row_key)
            self.__columns[col_key].append(value)
            for col_name in self.list_col():
                if col_name == col_key:
                    continue
                self.__columns[col_name].append(None)

    def read(self, row_key: any, col_key: any) -> any:
        row = self.__check_row_num(row_key)
        data = self.__columns[col_key].data[row]
        return data

    def delete(self, row_key: any):
        row = self.__check_row_num(row_key)
        self.__keys.delete(row)
        for col_key in self.__columns:
            self.__columns[col_key].delete(row)

    def print(self):
        print("{}\t| {}".format(self.__keys.name, list(self.__columns.keys())))
        length = len(self.__keys.data)
        for i in range(5):
            if i >= length:
                break
            li = []
            for col_name in self.__columns:
                li.append(self.__columns[col_name].data[i])
            print("{}\t| {}".format(self.__keys.data[i], li))
        if length > 5:
            print("\t... ...")
        for i in range(length-5, length):
            if i < 5:
                continue
            li = []
            for col_name in self.__columns:
                li.append(self.__columns[col_name].data[i])
            print("{}\t| {}".format(self.__keys.data[i], li))

    def to_dict(self) -> dict:
        data = {}
        for key in self.list_keys():
            row = {}
            for col_name in self.list_col():
                row[col_name] = self.read(key, col_name)
            data[key] = row
        return data

    def to_list(self) -> list:
        data = []
        li = [self.__keys.name]
        for col_name in self.list_col():
            li.append(col_name)
        data.append(li)
        for key in self.list_keys():
            li = [key]
            for col_name in self.list_col():
                li.append(self.read(key, col_name))
            data.append(li)
        return data

    def to_csv(self, path: str):
        li = self.to_list()
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(li)

    def from_dict(self, d: dict):
        for key in d:
            for col in d[key]:
                self.write(key, col, d[key][col])

    def from_list(self, li: list):
        cols = self.list_col()
        cols_type = self.list_col_type()
        key_type = self.get_keys_type()
        for row in li:
            key = key_type(row[0])
            for i in range(1, len(row)):
                self.write(key, cols[i-1], cols_type[i-1](row[i]))

    def from_csv(self, path: str, header: bool = True):
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            li = []
            for line in rows:
                li.append(line)
        if header == True:
            li = li[1:]
        self.from_list(li)

    def join_dataframe(self, df: object):
        col_names = df.list_col()
        col_types = df.list_col_type()
        for i in range(len(col_names)):
            col = col_names[i]
            if not col in self.list_col():
                self.add_col(col, col_types[i])
        for key in df.list_keys():
            for col in df.list_col():
                self.write(key, col, df.read(key, col))

    def __check_row_num(self, row_key: any) -> int:
        num = self.__keys.data.index(row_key)
        return num
