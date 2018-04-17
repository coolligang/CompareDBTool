#encoding:utf-8

from Factory import Factory

class DbServer:
    def __init__(self):
        self.factory = Factory()

    def listAllTableName(self,db):
        dbServer = self.factory.getDbServer(db)
        list_name = dbServer.listAllTableName()
        return list_name

    def listTableStructure(self,db,table):
        dbServer = self.factory.getDbServer(db)
        list_structure = dbServer.listTableStructure(table)
        return list_structure

    def listData(self,db,table,attributes):
        dbServer = self.factory.getDbServer(db)
        list_rs = dbServer.listData(table,attributes)
        return list_rs