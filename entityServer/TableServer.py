# encoding:utf-8

from entity.Table import Table
from daoServer.DbServer import DbServer


class TableServer:
    def __init__(self, db, table, primarykey, attributes):
        self.__db = db
        self.__table = table
        self.__primarykey = [key.upper() for key in primarykey.split(",")] if primarykey != "" else []
        self.__attributes = attributes
        self.__dbServer = DbServer()

    def createTable(self):
        structure = self.__dbServer.listTableStructure(self.__db, self.__table)
        col_name = [col[0].upper() for col in structure]
        lostKey = [key for key in self.__primarykey if key not in col_name]
        if len(lostKey) > 0:  # 判断主键是否在列表中
            raise AttributeError("PrimaryKey<%s.%s> should be form [%s]\n Mismached primarykey: %s" % (self.__db,
                                                                                                       self.__table,
                                                                                                       ",".join(map(
                                                                                                           lambda
                                                                                                               i: str(
                                                                                                               i),
                                                                                                           col_name)),
                                                                                                       ",".join(map(
                                                                                                           lambda
                                                                                                               i: str(
                                                                                                               i),
                                                                                                           lostKey))))
        data = self.__dbServer.listData(self.__db, self.__table, self.__attributes)
        table = Table(self.__db, self.__table, self.__primarykey, structure, data, self.__attributes)
        return table

    def resetTable(self, colums):
        re_structure = [col.upper() for col in colums.split(",")]
        structure = self.__dbServer.listTableStructure(self.__db, self.__table)
        structure = [col.upper() for col in structure]
        lost_colums = [col for col in re_structure if col not in structure]
        if len(lost_colums) > 0:  # 判断视图列表是否在原列表中
            raise AttributeError("Primarykey<%s.%s> should be form [%s]\n Mismached primarykey:%s" % (
                self.__db, self.__table, ",".join(map(lambda i: str(i), re_structure)),
                ",".join(map(lambda i: str(i), lost_colums))
            ))
        lostKey = [key for key in self.__primarykey if key not in re_structure]
        if len(lostKey):  # 判断主键是否在视图列表中
            raise AttributeError("Primarykey<%s.%s> should be from [%s]\n  Mismached primarykey: %s" % (
                self.__db, self.__table, ",".join(map(lambda i: str(i), re_structure)),
                ",".join(map(lambda i: str(i), lostKey))
            ))
        data = self.__dbServer.listData(self.__db, self.__table, self.__attributes)
        re_structure_index = [structure.index(col.upper()) for col in re_structure]
        new_data = []
        for row in data:
            list_row = [row[index] for index in re_structure_index]
            new_data.append(tuple(list_row))
        table = Table(self.__db, self.__table, self.__primarykey, re_structure, new_data, self.__attributes)
        table.setView(True)  # 标记该表为视图
        return table
