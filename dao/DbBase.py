# encoding:utf-8

class DbBase:
    failureException = IOError

    def __init__(self, db):
        self.type = db.split(",")[0]
        self.db = db.split(",")[1]

    def listAllTableName(self):
        pass

    def listTableStructure(self, table):
        pass

    def listData(self, table, attributes):
        pass
