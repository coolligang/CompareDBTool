# encoding:utf-8

# from dao.Dmp import Dmp
from dao.MySQL import MySQL


# from dao.Oracle import Oracle

class Factory:
    def __init__(self):
        pass

    def getDbServer(self, db):
        dbtype = db.split(",")[0]
        if dbtype.lower() == "mysql":
            dbServer = MySQL(db)
        # elif dbtype.lower() == "oracle":
        #     dbServer = Oracle(db)
        # elif dbtype == "hive":
        #     dbServer = Dmp(db)
        else:
            raise IOError("Database<%s> types can only be oracle,mysql and hive." % db)
        return dbServer
