#encoding:utf-8

import MySQLdb
import sys

reload(sys)
sys.setdefaultencoding("utf-8") #解决mysql中午乱吗
from DbBase import DbBase

class MySQL(DbBase):
    def __init__(self,db):
        DbBase.__init__(self,db)
        if self.type.lower() != "mysql":
            raise self.failureException("db type must bu mysql!")
    def __open(self):
        try:
            str_user = self.db.split("/")[0]
            str_passwd = self.db.split("@")[0].split("/")[1]
            str_host = self.db.split(":")[0].split("@")[1]
            int_port = int(self.db.split(":")[1].split("/")[0])
            str_db = self.db.split("/")[2]
            connect = MySQLdb.connect(host=str_host,port=int_port,user=str_user,passwd=str_passwd,db=str_db,charset="utf8")
        except Exception as e:
            raise self.failureException("%s,%s  %s" %(self.type,self.db,repr(e)))
        cursor = connect.cursor()
        cursor.execute("SET NAMES UTF8")
        return connect,cursor

    def __close(self,connect,cursor):
        try:
            if cursor != None:
                cursor.close()
            if connect != None:
                connect.close()
        except Exception as e:
            print("*ERROR*" + repr(e) )

    def listAllTableInfo(self):
        str_sql = "SELECT table_name FROM information_schema.tables " \
                  "WHERE table_type = 'base table' and table_schema='%s'" % (self.db.split("/")[2])
        rs = self.listDataBySQL(str_sql)
        return rs

    def listAllTableName(self):
        list_tableInfo = self.listAllTableInfo()
        list_tableName =[info[0] for info in list_tableInfo] if list_tableInfo != None else[]
        return list_tableName

    def listTableStructure(self,table):
        """
        获取表结构
        :param table:
        :return: 字典，字段名：字段属性信息
        """
        # sql = "SELECT ORDINAL_POSITION,COLUMN_NAME,DATA_TYPE FROM information_schema.`COLUMNS` " \
        #       "WHERE TABLE_NAME ='%s' ORDER BY ORDINAL_POSITION" % table
        # listStructure = self.listDataBySQL(sql)
        # tableStructure = [column[1] for column in listStructure]
        sql = "DESC %s" % table
        listStructure = self.listDataBySQL(sql)
        return listStructure

    def listData(self,table,attributes):
        list_attributes = ["1-1"]
        for k,v in attributes.items():
            list_attributes.append(str(k) + "=" + str(v))
        str_attributes = " and ".join(list_attributes)
        str_sql = "select * from %s where %s" % (table,str_attributes)
        list_rs = self.listDataBySQL(str_sql)
        return list_rs

    def listDataBySQL(self,str_sql):
        """
        获取所有查询结果
        :param str_sql:
        :return: 返回list列表，列表中的元素为TUPLE类型
        """
        conn,cur = self.__open()
        list_rs =[]
        try:
            cur.execute(str_sql)
            list_rs = cur.fetchall()
        except Exception as e:
            print("*ERROR* " + repr(e) + "\n str_sql:%s" % (str_sql))
        finally:
            self.__close(conn,cur)
        return list_rs

if __name__=="__main__":
    db = "mysql,root/123456@127.0.0.1:3306/db_flask"
    mysql_obj = MySQL(db)
    structure=mysql_obj.listTableStructure("answer")
    print(structure)