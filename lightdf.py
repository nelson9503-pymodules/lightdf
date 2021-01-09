from .dfcore import dfcore
import csv

class lightdf:
    def __init__(self):
        self.__dfcore = dfcore()
        self.indexName = "Index"
    
    def data(self, index:"str/int", header:"str/int") -> any:
        """
        Get the data from data point.
        Each data point is double indexing, user can pass the index/header
        of string in name and integer in number.
        """
        return self.__dfcore.get_data(index, header)
    
    def wdata(self, index:"str/int", header:"str/int", value:any):
        """
        Get the data from data point.
        Each data point is double indexing, user can pass the index/header
        of string in name and integer in number.
        """
        if type(index) == str:
            if not index in self.__dfcore.iDict1:
                self.__dfcore.add_index(index)
        elif type(index) == int:
            x = len(self.__dfcore.iDict1)
            if index >= x:
                while str(x) in self.__dfcore.iDict1:
                    x += 1
                self.__dfcore.add_index(str(x))
        if type(header) == str:
            if not header in self.__dfcore.hDict1:
                self.__dfcore.add_header(header)
        elif type(header) == int:
            x = len(self.__dfcore.hDict1)
            if header >= x:
                while str(x) in self.__dfcore.hDict1:
                    x += 1
                self.__dfcore.add_header(str(x))
        self.__dfcore.write_data(index, header, value)
    
    def list_index(self) -> list:
        """
        List the index.
        """
        return self.__dfcore.list_index()
    
    def drop_index(self, index:"str/int"):
        """
        Drop the index.
        The row of this index would be deleted.
        """
        self.__dfcore.drop_index(index)
    
    def add_index(self, index:"str"=None):
        """
        Add an index.
        Pass None to auto generate an index name.
        New row of data would be created, which default values are None. 
        """
        if index == None:
            index = len(self.__dfcore.iDict1)
            while str(index) in self.__dfcore.iDict1:
                index += 1
            index = str(index)
        self.__dfcore.add_index(index)
    
    def set_index(self, indexlist:list):
        """
        Set the index with a indexlist.
        """
        newDict1 = {}
        for i in range(len(indexlist)):
            if not i >= len(self.__dfcore.iDict1):
                self.__dfcore.iDict2[i] = indexlist[i]
            else:
                self.add_index(indexlist[i])
            newDict1[indexlist[i]] = i
        self.__dfcore.iDict1 = newDict1
    
    def list_header(self) -> list:
        """
        List out headers.
        """
        return self.__dfcore.list_header()
    
    def drop_header(self, header:"str/int"):
        """
        Drop the header.
        The column of this header would be deleted.
        """
        self.__dfcore.drop_header(header)
    
    def add_header(self, header:"str"=None):
        """
        Add an header.
        Pass None to auto generate an header name.
        New column of data would be created, which default values are None. 
        """
        if header == None:
            header = len(self.__dfcore.hDict1)
            while str(header) in self.__dfcore.hDict1:
                header += 1
            header = str(header)
        self.__dfcore.add_header(header)
    
    def set_header(self, headerlist:list):
        """
        Set the header with a headerlist.
        """
        newDict1 = {}
        for i in range(len(headerlist)):
            if not i >= len(self.__dfcore.hDict1):
                self.__dfcore.hDict2[i] = headerlist[i]
            else:
                self.add_header(headerlist[i])
            newDict1[headerlist[i]] = i
        self.__dfcore.hDict1 = newDict1
    
    def to_list(self, index:bool, header:bool) -> list:
        """
        Output dataframe to 2d list.
        Pass True to include index and header.
        """
        li = []
        if header == True:
            line = []
            if index == True:
                line.append(self.indexName)
            for h in self.__dfcore.list_header():
                line.append(h)
            li.append(line)
        for i in self.__dfcore.list_index():
            line = []
            if index == True:
                line.append(i)
            for c in range(len(self.__dfcore.hDict2)):
                line.append(self.__dfcore.get_data(i, c))
            li.append(line)
        return li

    def to_dict(self) -> dict:
        """
        Output dataframe to dictionary.
        Index is first-level keys and
        header is second-level keys.
        """
        d = {}
        for index in self.__dfcore.list_index():
            d[index] = {}
            for header in self.__dfcore.list_header():
                d[index][header] = self.__dfcore.get_data(index, header)
        return d
    
    def to_csv(self, path:str, index:bool, header:bool):
        """
        Output dataframe to csv and save as path.
        Header is included;
        Header and index are included.
        """
        li = self.to_list(index, header)
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(li)
    
    def print(self):
        """
        Print information of dataframe in terminal.
        """
        print("IndexName: {}".format(self.indexName))
        print()
        print("Headers:\n{}".format(self.list_header()))
        print()
        print("Index:\n{}".format(self.list_index()))
        print()
        print("Data:\n")
        for row in self.__dfcore.tb:
            print(row)
        print()


def new(datalist:list=None, indexlist:list=None, headerlist:list=None) -> lightdf:
    """
    Create a empty dataframe by passing nothing.
    Or pass a 2d list to initalize dataframe.
    """
    df = lightdf()
    if not datalist == None:
        for r in range(len(datalist)):
            for c in range(len(datalist[r])):
                df.wdata(r, c, datalist[r][c])
    if not indexlist == None:
        df.set_index(indexlist)
    if not headerlist == None:
        df.set_header(headerlist)
    return df

def read_dict(d:dict) -> lightdf:
    """
    Read a dictionary to dataframe.
    The dictionary must be like this:
        {
            "index":{"header":value, ...},
            ...
        }
    """
    df = lightdf()
    for index in d:
        for header in d[index]:
            df.wdata(index, header, d[index][header])
    return df

def read_csv(path:str, header:bool) -> lightdf:
    """
    Read csv from the path.
    Pass True to make the first row be header.
    """
    df = lightdf()
    with open(path, newline='') as csvfile:
        rows = csv.reader(csvfile)
        headerlist = []
        r = 0
        for row in rows:
            if header == True:
                for h in row:
                    headerlist.append(h)
                header = False
                continue
            c = 0
            for val in row:
                df.wdata(r, c, val)
                c += 1
            r += 1
        if len(headerlist) > 0:
            df.set_header(headerlist)
    return df