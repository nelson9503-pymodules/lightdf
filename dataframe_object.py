from .column_object import Column
from datetime import datetime, timedelta
import csv

# Dataframe manages multiple Column objects to provide 2-d table structure frame.
# Users read and write the data by using key value and column name.


class Dataframe:

    # Every initializing dataframe, user should define the name of key column name.
    def __init__(self, key_col_name: any, key_col_type: type):
        self.key_col_name = key_col_name
        self.__key_column = Column(key_col_type, is_unique=True)
        # The value columns (not key) are stored in dictionary mapping by column names.
        self.__columns = {}
        # initialize id
        self.__next_id = 0

    def add_col(self, col_name: any, col_type: type, is_unique: bool = False, is_not_none: bool = False):
        """
        Add a new column to the dataframe.
        """
        # the column names cannot be duplicated
        if col_name in self.__columns or col_name == self.key_col_name:
            raise KeyError("Duplicated column name.")
        self.__columns[col_name] = Column(col_type, is_unique, is_not_none)
        # sync to keys column id
        ids = self.__key_column.list_id()
        for id in ids:
            self.__columns[col_name].insert_initial_value(id)

    def drop_col(self, col_name: any):
        """
        Drop a column from the dataframe.
        """
        check = self.__columns.pop(col_name, False)
        if check == False:
            raise KeyError("Cannot drop a column not exists")

    def write(self, key_value: any, col_name: any, value: any):
        """
        Write value to cell located by key value and column name.
        """
        # we query the id from key column first
        id = self.__key_column.get_id_by_value(key_value)
        if id == None:  # means key value not exists
            # get new id
            id = self.__next_id
            self.__next_id += 1
            # update key column
            self.__key_column.update_value(id, key_value)
            # insert a initial value to each column
            for col in self.__columns:
                self.__columns[col].insert_initial_value(id)
        # check if no this column name
        if not col_name in self.__columns:
            raise KeyError("Column name not exists")
        # finally, we write the value to column
        self.__columns[col_name].udpate_value(id, value)

    def read(self, key_value: any, col_name: any) -> any:
        """
        Read data from cell located by key value and column name.
        """
        # we query the id from key column first
        id = self.__key_column.get_id_by_value(key_value)
        # check if no this key value
        if id == None:
            raise KeyError("Key value not exists")
        # check if no this column name
        if not col_name in self.__columns:
            raise KeyError("Column name not exists")
        return self.__columns[col_name].get_value(id)

    def drop(self, key_value: any):
        """
        Drop the entire row of data by using key value.
        """
        # we query the id from key column first
        id = self.__key_column.get_id_by_value(key_value)
        # check if no this key value
        if id == None:
            raise KeyError("Key value not exists")
        # drop from key column
        self.__key_column.drop_value(id)
        # drop from each value columns
        for col_name in self.__columns:
            self.__columns[col_name].drop_value(id)

    def set_unique(self, col_name: any, is_unique: bool):
        """
        Set the column is_unique data policy.
        """
        # check if no this column name
        if not col_name in self.__columns:
            raise KeyError("Column name not exists")
        self.__columns[col_name].set_unique(is_unique)

    def set_not_none(self, col_name: any, is_not_none: bool):
        """
        Set the column is_not_none data policy.
        """
        # check if no this column name
        if not col_name in self.__columns:
            raise KeyError("Column name not exists")
        self.__columns[col_name].set_not_none(is_not_none)

    def set_data_type(self, col_name: any, data_type: type):
        """
        Set the data type of column.
        """
        # check if no this column name
        if not col_name in self.__columns and not col_name == self.key_col_name:
            raise KeyError("Column name not exists")
        if col_name == self.key_col_name:
            self.key_col_name.set_data_type(data_type)
        else:
            self.__columns[col_name].set_data_type(data_type)

    def set_column_to_key(self, col_name: any, keep: bool = True):
        """
        Set a value column to be the key column.
        If keep is True, the original key column will be appended to value column.
        """
        # check if no this column name
        if not col_name in self.__columns and not col_name == self.key_col_name:
            raise KeyError("Column name not exists")
        # If keep is True, the original key column will be appended to value column.
        if keep == True:
            self.__key_column.set_not_none(False)
            self.__columns[self.key_col_name] = self.__key_column
        # pop out the column and set it to be key column
        self.__key_column = self.__columns.pop(col_name, None)
        if self.__key_column == None:
            raise KeyError("Column name not exists")
        self.__key_column.set_unique(True)
        self.key_col_name = col_name

    def get_key_list(self) -> list:
        """
        Get the list of key values.
        """
        return self.__key_column.list_value()

    def get_col_list(self) -> list:
        """
        Get the list of column names.
        """
        return list(self.__key_column.keys())

    def get_data_type(self, col_name: any) -> type:
        """
        Get the data type of column.
        """
        # check if no this column name
        if not col_name in self.__columns and not col_name == self.key_col_name:
            raise KeyError("Column name not exists")
        return self.__columns[col_name].data_type

    def get_col_data(self, col_name: any) -> list:
        """
        Get the list of data from a column.
        """
        # check if no this column name
        if not col_name in self.__columns and not col_name == self.key_col_name:
            raise KeyError("Column name not exists")
        return self.__columns[col_name].list_value()

    def get_row_data(self, key_value: any) -> list:
        """
        Get the list of data from a row.
        """
        data = [key_value]
        for col_name in self.__columns:
            data.append(self.read(key_value, col_name))
        return data

    def drop_duplicated(self, col_name: any):
        """
        Drop duplicated values from column.
        This keep the first-found value.
        """
        check_dict = {}
        for key_value in self.get_key_list():
            value = self.read(key_value, col_name)
            if value in check_dict:
                self.drop(key_value)
            else:
                check_dict[value] = None

    def sort(self, col_name: any, ascending: bool = True):
        # check if no this column name
        if not col_name in self.__columns and not col_name == self.key_col_name:
            raise KeyError("Column name not exists")
        # sort column
        if col_name == self.key_col_name:
            id_map = self.__key_column.sort_value(ascending)
        else:
            id_map = self.__columns[col_name].sort_value(ascending)
        # sync other columns
        if not col_name == self.key_col_name:
            self.__key_column.sync_id_map(id_map)
        for col in self.__columns:
            if col == col_name:
                continue
            self.__columns[col].sync_id_map(id_map)

    def to_list(self) -> list:
        """
        Dataframe to list.
        """
        li = []
        headers = [self.key_col_name]
        for col_name in self.__columns:
            headers.append(col_name)
        li.append(headers)
        for key_value in self.get_key_list():
            row = self.get_row_data(key_value)
            li.append(row)
        return li

    def to_dict(self) -> dict:
        """
        Dataframe to dictionary.
        """
        d = {}
        for key_value in self.get_key_list():
            d[key_value] = {}
            for col_name in self.__columns:
                d[key_value][col_name] = self.read(key_value, col_name)
        return d

    def to_csv(self, save_path: str):
        """
        Dataframe to csv.
        """
        li = self.to_list()
        with open(save_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(li)

    def from_list(self, li: list):
        """
        Update dataframe from list.
        """
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
        """
        Update dataframe from dictionary.
        """
        for key_value in d:
            for col_name in d[key_value]:
                self.write(key_value, col_name, d[key_value][col_name])

    def from_csv(self, csv_path: str, ignore_header_row: bool = False):
        """
        Update dataframe from csv.
        if ignore_header_row is True, the update method will start from line 2.
        """
        with open(csv_path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            li = []
            for line in rows:
                li.append(line)
        if ignore_header_row == True:
            li = li[1:]
        self.from_list(li)

    def from_df(self, df: object):
        """
        Update dataframe from anonther dataframe.
        """
        for key_value in df.get_key_list():
            for col_name in df.get_col_list():
                # if dataframe has not this column, create new
                if not col_name in self.get_col_list():
                    self.add_col(col_name, df.get_col_type(col_name))
                self.write(key_value, col_name, df.read(key_value, col_name))

    def print(self):
        """
        Print dataframe to terminal.
        """
        # headers
        header = [self.key_col_name]
        header.extend(self.get_col_list())
        print("header\t| {}".format(header))
        # data types
        t = [self.__key_column.data_type]
        for col_name in self.__columns:
            t.append(self.__columns[col_name].data_type)
        print("types\t| {}".format(t))
        # data values
        key_values = self.get_key_list()
        # print first 5 rows
        for i in range(5):
            if i >= len(key_values):
                return
            key_value = key_values[i]
            print("{}\t| {}".format(i, self.get_row_data(key_value)))
        # print a break
        if len(key_values) > 6:
            print("... ...")
        # print last 5 rows
        for i in range(len(key_values)-5, len(key_values)):
            if i < 5:
                continue
            key_value = key_values[i]
            print("{}\t| {}".format(i, self.get_row_data(key_value)))
