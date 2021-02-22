
class Column:

    def __init__(self, name: str, data_type: type, unique_values: bool = False, none_values: bool = True):
        if not type(name) == str:
            raise TypeError("Column name must be string.")
        self.name = name
        if not type(data_type) == type:
            raise TypeError(
                "data_type must be python type classes. (int, str, float, bool...)")
        self.data_type = data_type
        self.unique = unique_values
        self.none = none_values
        if self.unique == True: # unique values column never contains none value
            self.none = False
        self.data = []

    def append(self, data: any):
        data = self.check_data_type(data)
        self.data.append(data)

    def update(self, position: int, data: any):
        if position >= len(self.data):
            raise IndexError("Column position out of range.")
        elif position < 0:
            raise IndexError("Negative column position.")
        data = self.check_data_type(data)
        self.data[position] = data

    def convert_data_type(self, data_type: type):
        if data_type == self.data_type:
            return
        self.data_type = data_type
        for i in range(len(self.data)):
            self.data[i] = self.check_data_type(self.data[i])

    def delete(self, position: int):
        if position >= len(self.data):
            raise IndexError("Column position out of range.")
        elif position < 0:
            raise IndexError("Negative column position.")
        if position == len(self.data) - 1:
            self.data = self.data[:position]
        else:
            a = self.data[:position]
            b = self.data[position+1:]
            a.extend(b)
            self.data = a

    def to_unique_column(self):
        data = []
        for d in self.data:
            if not d in data:
                data.append(d)
            else:
                raise ValueError(
                    "Column with duplicated values cannot be unique.")
        self.unique = True

    def check_data_type(self, data: any) -> any:
        if data == None:
            # None is ok for non-unique column
            if self.unique == False and self.none == True:
                return data
            elif self.unique == True:
                raise ValueError("Unique-values column cannot input None.")
            else:
                raise ValueError("Non-none column cannot input None.")
        elif not type(data) == self.data_type:
            # try convert it
            data = self.data_type(data)
        if self.unique == True and data in self.data:
            raise ValueError("Duplicated in unique column.")
        return data

    def get_initialize_value(self) -> any:
        if self.none == True:
            return None
        elif self.data_type == int:
            return 0
        elif self.data_type == float:
            return 0.0
        elif self.data_type == bool:
            return False
        elif self.data_type == str:
            return ""