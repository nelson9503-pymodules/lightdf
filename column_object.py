
class Column:

    def __init__(self, data_type: type, key_col: bool, unique_values: bool, none_values: bool):
        """
        data_type: int, float, str, bool, None (None -> disable data type checking.)

        key_col: True -> must be unique and not none. Plus, build a key dictionary for query.
        unique_values: True -> is unique and must not none.
        none_values: True -> not none but can not be unique.
        """
        if not data_type in [int, float, str, bool, None]:
            raise TypeError(
                "data_type can only be int, float, str, bool or None.")
        self.data_type = data_type
        if key_col == True:
            self.key_col = True
            self.unique = True
            self.none = False
        elif unique_values == True:
            self.key_col = False
            self.unique = True
            self.none = False
        elif none_values == False:
            self.key_col = False
            self.unique = False
            self.none = False
        else:
            self.key_col = False
            self.unique = False
            self.none = True
        self.key_dict = {}
        self.data_dict = {}

    def initialize_value(self, id: int):
        if self.none == False:
            if self.data_type in [str, None]:
                self.update_value(id, "")
            elif self.data_type == int:
                self.update_value(id, 0)
            elif self.data_type == float:
                self.update_value(id, 0.0)
            elif self.data_type == bool:
                self.update_value(id, False)
        else:
            self.update_value(id, None)

    def update_value(self, id: int, value: any):
        value = self.__check_data_type(value)
        if self.none == False and value == None:
            raise ValueError("Cannot input None value to a non-none column.")
        if self.unique == True:
            for check_id in self.data_dict:
                if check_id == id:
                    continue
                if self.data_dict[check_id] == value:
                    raise ValueError("Duplicated value in unique column.")
        if self.key_col == True and id in self.data_dict:
            key_value = self.data_dict[id]
            del self.key_dict[key_value]
        self.data_dict[id] = value
        if self.key_col == True:
            self.key_dict[value] = id

    def drop_value(self, id: int):
        if self.key_col == True:
            key_value = self.data_dict[id]
            del self.key_dict[key_value]
        del self.data_dict[id]

    def list_value(self) -> list:
        li = []
        ids = list(self.data_dict.keys())
        ids.sort()
        for id in ids:
            li.append(self.data_dict[id])
        return li
    
    def list_id(self) -> list:
        return list(self.data_dict.keys())

    def sort_value(self, ascending: bool = True) -> dict:
        values = self.list_value()
        if ascending == True:
            reverse = False
        else:
            reverse = True
        values.sort(reverse=reverse)
        id_map = {}
        data_dict = {}
        for i in range(len(values)):
            value = values[i]
            for id in self.data_dict:
                if self.data_dict[id] == value:
                    self.data_dict.pop(id, None)
                    data_dict[i] = value
                    id_map[id] = i
                    break
        self.data_dict = data_dict
        if self.key_col == True:
            self.key_dict = {}
            for id in self.data_dict:
                self.key_dict[self.data_dict[id]] = id
        return id_map

    def reset_id(self, id_map: dict):
        data_dict = {}
        for id in self.data_dict:
            data_dict[id_map[id]] = self.data_dict[id]
        self.data_dict = data_dict
        if self.key_col == True:
            self.key_dict = {}
            for id in self.data_dict:
                self.key_dict[self.data_dict[id]] = id

    def get_value(self, id: int) -> any:
        return self.data_dict[id]

    def get_key(self, key_value: any) -> any:
        if key_value in self.key_dict:
            return self.key_dict[key_value]
        return None

    def convert_value_type(self, data_type: type):
        self.data_type = data_type
        if data_type == None:
            return
        for id in self.data_dict:
            self.data_dict[id] = self.__check_data_type(self.data_type(self.data_dict[id]))
        if self.key_col == True:
            self.key_dict = {}
            for id in self.data_dict:
                self.key_dict[self.data_dict[id]] = id

    def set_none_value(self, none: bool):
        if none == False:
            for id in self.data_dict:
                if self.data_dict[id] == None:
                    raise ValueError("Set not none failed.")
            self.none = False
        else:
            self.none = True
            if self.unique == True:
                self.set_unique_column(False)

    def set_unique_column(self, unique: bool):
        if unique == True:
            self.set_none_value(False)
            check_dict = {}
            for id in self.data_dict:
                if self.data_dict[id] in check_dict:
                    raise ValueError("Set unique column failed.")
                check_dict[self.data_dict[id]] = None
            self.unique = True
        else:
            self.unique = False
            self.set_none_value(True)

    def set_key_col(self, key_col: bool):
        if key_col == True:
            self.set_unique_column(True)
            self.key_dict = {}
            for id in self.data_dict:
                self.key_dict[self.data_dict[id]] = id
            self.key_col = True
        else:
            self.key_col = False
            self.key_dict = {}

    def __check_data_type(self, value: any) -> any:
        if self.data_type == None:
            return value
        elif self.data_type in [int, float, bool, str]:
            try:
                if value == None:
                    return value
                value = self.data_type(value)
                return value
            except:
                raise TypeError("Value with invalid data type.")
        raise Exception(
            "Invalid data type(Not Expected). You may using lightdf in danger way.")
