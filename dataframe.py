from datetime import datetime
import csv


class Dataframe:

    def __init__(self, keyName: str, keyType: type):
        self.keyName = keyName
        self.__data = {}
        self.__cols = {keyName: keyType}

    def listKeys(self) -> list:
        """
        List keys.
        """
        if self.__cols[self.keyName] == datetime:
            li = []
            for key in self.__data.keys():
                li.append(key.strftime("%Y-%m-%d"))
            return li
        return list(self.__data.keys())

    def listCols(self) -> dict:
        """
        List columns and types.
        """
        return self.__cols

    def getData(self, key: any, column: any) -> any:
        """
        Get data.
        """
        if self.__cols[self.keyName] == datetime and type(key) == str:
            key = datetime.strptime(key, "%Y-%m-%d")
        if not key in self.__data:
            raise KeyError("Key not exists.")
        if not column in self.__cols:
            raise AttributeError("Column not exits.")
        if not column in self.__data[key]:
            return None
        if self.__cols[column] == datetime:
            return self.__data[key][column].strftime("%Y-%m-%d")
        return self.__data[key][column]

    def writeData(self, key: any, column: any, value: any):
        """
        Write Data.
        """
        if self.__cols[self.keyName] == datetime and type(key) == str:
            key = datetime.strptime(key, "%Y-%m-%d")
        if not key in self.__data:
            self.__data[key] = {}
        if not column in self.__cols:
            raise AttributeError("Column not exists.")
        if self.__cols[column] == datetime and type(value) == str:
            value = datetime.strptime(value, "%Y-%m-%d")
        if not type(value) == self.__cols[column]:
            raise TypeError("Unmatch type of value to column.")
        self.__data[key][column] = value

    def dropData(self, key: any):
        """
        Drop Data.
        """
        if self.__cols[self.keyName] == datetime and type(key) == str:
            key = datetime.strptime(key, "%Y-%m-%d")
        if not key in self.__data:
            raise KeyError("Key not exists.")
        del self.__data[key]

    def addCol(self, column: str, dataType: type):
        """
        Add column.
        """
        if str(column) in self.__cols:
            raise KeyError("Duplicted keys.")
        self.__cols[str(column)] = dataType

    def dropCol(self, column: str):
        """
        Drop column.
        """
        if column in self.__cols:
            del self.__cols[column]
        else:
            return
        for key in self.__data:
            if column in self.__data[key]:
                del self.__data[key][column]

    def changeColType(self, column: str, newType: type):
        """
        Change the type of column.
        """
        if not column in self.__cols:
            raise AttributeError("Column not exists.")
        self.__cols[column] = newType
        for key in self.__data:
            if column in self.__data[key] and not self.__data[key][column] == None:
                try:
                    if newType == datetime:
                        self.__data[key][column] = datetime.strptime(
                            self.__data[key][column], "%Y-%m-%d")
                    elif type(self.__data[key][column]) == datetime and newType == str:
                        self.__data[key][column] = datetime.strftime(
                            "%Y-%m-%d")
                    else:
                        self.__data[key][column] = newType(
                            self.__data[key][column])
                except TypeError:
                    raise TypeError("Fail to convert value's type.")

    def changeKeyType(self, newType: type):
        """
        Change the type of key.
        """
        data = {}
        self.__cols[self.keyName] = newType
        for key in self.__data:
            if newType == datetime and type(key) == str:
                data[datetime.strptime(key, "%Y-%m-%d")] = self.__data[key]
            elif newType == datetime and type(key) == datetime:
                data[key] = self.__data[key]
            elif newType == datetime:
                raise TypeError("Fail to convert datetime type.")
            else:
                data[newType(key)] = self.__data[key]
        self.__data = data

    def to_list(self) -> list:
        """
        Export to 2d list.
        """
        li = []
        cols = self.listCols()
        row = []
        for col in cols:
            row.append(col)
        li.append(row)
        for key in self.__data:
            if self.__cols[self.keyName] == datetime:
                row = [key.strftime("%Y-%m-%d")]
            else:
                row = [key]
            for col in cols:
                if col == self.keyName:
                    continue
                elif not col in self.__data[key]:
                    row.append(None)
                elif self.__cols[col] == datetime:
                    row.append(self.__data[key][col].strftime("%Y-%m-%d"))
                else:
                    row.append(self.__data[key][col])
            li.append(row)
        return li

    def to_dict(self) -> dict:
        """
        Export to dict.
        """
        data = {}
        for key in self.__data:
            if self.__cols[self.keyName] == datetime:
                key2 = key.strftime("%Y-%m-%d")
            else:
                key2 = key
            data[key2] = {}
            for col in self.__cols:
                if col == self.keyName:
                    continue
                if not col in self.__data[key]:
                    continue
                if self.__cols[col] == datetime:
                    data[key2][col] = self.__data[key][col].strftime(
                        "%Y-%m-%d")
                else:
                    data[key2][col] = self.__data[key][col]
        return data

    def to_csv(self, path: str):
        """
        Save dataframe to csv.
        """
        li = self.to_list()
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(li)


def new(keyName: str, keyType: type, cols: dict = None) -> Dataframe:
    """
    Create new empty dataframe.
    """
    df = Dataframe(keyName, keyType)
    if not cols == None:
        for col in cols:
            df.addCol(col, cols[col])
    return df


def from_dict(d: dict, keyName: str, keyType: type) -> Dataframe:
    """
    Create dataframe from dictionary.
    """
    df = Dataframe(keyName, keyType)
    for key in d:
        if not type(key) == keyType:
            raise TypeError("Unmatch key type.")
        for col in d[key]:
            if not col in df.listCols() and not d[key][col] == None:
                df.addCol(str(col), type(d[key][col]))
            df.writeData(key, col, d[key][col])
    return df


def from_list(li: list) -> Dataframe:
    """
    Create dataframe from 2d list.
    """
    if len(li) <= 1:
        raise AttributeError("Empty list.")
    cols = li[0]
    df = Dataframe(cols[0], type(li[1][0]))
    for i in range(1, len(cols)):
        df.addCol(cols[i], type(li[1][i]))
    for r in range(1, len(li)):
        for i in range(1, len(cols)):
            df.writeData(li[r][0], cols[i], li[r][i])
    return df


def from_csv(path: str) -> Dataframe:
    """
    Create dataframe from csv file.
    """
    with open(path, newline='') as csvfile:
        rows = csv.reader(csvfile)
        li = []
        for line in rows:
            li.append(line)
    df = from_list(li)
    return df
