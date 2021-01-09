class dfcore:
    def __init__(self):
        self.tb = []
        self.iDict1 = {}
        self.iDict2 = {}
        self.hDict1 = {}
        self.hDict2 = {}

    def get_data(self, index: "str/int", header: "str/int"):
        """
        Get the value from a data point that is located by giving index and header.
        """
        index = self.__checkIndexInputs(index)
        header = self.__checkHeaderInputs(header)
        return self.tb[index][header]

    def write_data(self, index: "str/int", header: "str/int", value: any):
        """
        Write the value to a data point that is located by giving index and header.
        """
        index = self.__checkIndexInputs(index)
        header = self.__checkHeaderInputs(header)
        self.tb[index][header] = value

    def add_index(self, index: str):
        """
        Add a index (row) that naming by index name passed in.
        """
        if index in self.iDict1:
            raise KeyError("index name already existed")
        num = len(self.iDict1)
        self.iDict1[index] = num
        self.iDict2[num] = index
        self.__expandToDataSize()

    def drop_index(self, index: "str/int"):
        """
        Delete the index (row) that is located by giving index.
        """
        index = self.__checkIndexInputs(index)
        for key in self.iDict1:
            if self.iDict1[key] > index:
                self.iDict1[key] -= 1
        del self.iDict1[self.iDict2[index]]
        for i in range(index, len(self.iDict2)-1):
            self.iDict2[i] = self.iDict2[i+1]
        del self.iDict2[len(self.iDict2)-1]
        li1 = self.tb[:index]
        for item in self.tb[index+1:]:
            li1.append(item)
        self.tb = li1

    def list_index(self) -> list:
        """
        List out index.
        """
        li = []
        for i in range(len(self.iDict2)):
            li.append(self.iDict2[i])
        return li

    def add_header(self, header: str):
        """
        Add a header (column) that naming by header name passed in.
        """
        if header in self.hDict1:
            raise KeyError("header name already existed")
        num = len(self.hDict1)
        self.hDict1[header] = num
        self.hDict2[num] = header
        self.__expandToDataSize()

    def drop_header(self, header: "str/int"):
        """
        Delete the header (column) that is located by giving header.
        """
        header = self.__checkHeaderInputs(header)
        for key in self.hDict1:
            if self.hDict1[key] > header:
                self.hDict1[key] -= 1
        del self.hDict1[self.hDict2[header]]
        for i in range(header, len(self.hDict2)-1):
            self.hDict2[i] = self.hDict2[i+1]
        del self.hDict2[len(self.hDict2)-1]
        for r in range(len(self.tb)):
            li1 = self.tb[r][:header]
            for item in self.tb[r][header+1:]:
                li1.append(item)
            self.tb[r] = li1

    def list_header(self) -> list:
        """
        List out headers.
        """
        li = []
        for i in range(len(self.hDict2)):
            li.append(self.hDict2[i])
        return li

    def __checkIndexInputs(self, index: "index") -> "index":
        if type(index) == str:
            if index in self.iDict1:
                index = self.iDict1[index]
            else:
                raise KeyError("index name not exists")
        elif type(index) == int:
            if index >= len(self.iDict1):
                raise IndexError("index number out of range")
        else:
            raise TypeError("only accept index in string or integer")
        return index

    def __checkHeaderInputs(self, header: "header") -> "header":
        if type(header) == str:
            if header in self.hDict1:
                header = self.hDict1[header]
            else:
                raise KeyError("header name not exists")
        elif type(header) == int:
            if header >= len(self.hDict1):
                raise IndexError("header number out of range")
        else:
            raise TypeError("only accapt header in string or integer")
        return header

    def __expandToDataSize(self):
        for r in range(len(self.tb), len(self.iDict1)):
            self.tb.append([])
        for r in range(len(self.tb)):
            for _ in range(len(self.tb[r]), len(self.hDict1)):
                self.tb[r].append(None)
