
class Column:

    def __init__(self, name: str, data_type: type, unique_values: bool = False):
        if not type(name) == str:
            raise TypeError("Column name must be string.")
        self.name = name
        if not type(data_type) == type:
            raise TypeError(
                "data_type must be python type classes. (int, str, float, bool...)")
        self.data_type = data_type
        self.unique = unique_values
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
            if self.unique == False:
                return data
            else:
                raise ValueError("Unique-values column cannot input None.")
        elif not type(data) == self.data_type:
            # try convert it
            data = self.data_type(data)
        if self.unique == True and data in self.data:
            raise ValueError("Duplicated in unique column.")
        return data
