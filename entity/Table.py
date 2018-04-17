#encoding:utf-8

class Table:
    """
    表视图
    """
    __db = "" #db 数据库连接信息 usr/password/ip:port/database
    __name = "" #name 表名 str
    __primarykey = [] #primaryKey 表主键 str[col1,col2,...]
    __structure = []  #structure 表结构 list[列名1，列名2，...]
    __data = []   #data 表数据 List[(col1,col2,col3),(col1,col2,col3),...]
    __attributes = {} #dict 属性信息{"peroid":2017,...}
    __view = False #标记是否是视图

    def __init__(self,db,name,primarykey,structure,data,attributes):
        self.__db = db
        self.__name = name
        self.__primarykey = primarykey
        self.__structure = structure
        self.__data = data
        self.__attributes = attributes

    def setView(self,flag):
        if isinstance(flag,bool):
            self.__view = flag
        else:
            raise ValueError("Flag view must be bool.")

    def getView(self):
        return self.__view

    def setDb(self,db):
        self.__db = db

    def getDb(self):
        return self.__db

    def setPrimaryKey(self,setPrimaryKey):
        self.__primarykey = setPrimaryKey

    def getPrimaryKey(self):
        return self.__primarykey

    def setName(self,name):
        self.__name = name

    def getName(self):
        return self.__name

    def setStructure(self,structure):
        self.__structure = structure

    def getStructure(self):
        return self.__structure

    def setData(self,data):
        self.__data = data

    def getData(self):
        return self.__data

    def getAttr(self):
        return self.__attributes

    def setAttr(self,attr):
        self.__attributes = attr

    def getInfo(self):
        list_attributes = [str(k) + "=" + str(v) for k,v in self.__attributes.items()]
        return "%s.%s %s" % (self.__db,self.__name," and ".join(list_attributes))