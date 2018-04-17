# #encoding:utf-8
#
# import cx_Oracle
# from DbBase import DbBase
# import os
#
# os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8" #解决oracle中文乱码
#
# class Oracle:
#     def __init__(self,db):
#         DbBase.__init__(self,db)
#
#     def __open(self):
#         try:
#             connect = cx_Oracle.connect(self.db)
#         except Exception as e:
#             raise self.failureException("%s,%s %s" % (self.type,self.db,e.message))
#         cursor = connect.cursor()
#         return connect,cursor
#
#     def __close(self,connect,cursor):
#         try:
#             if cursor != None:
#                 cursor.close()
#             if connect != None:
#                 connect.close()
#         except Exception as e:
#             print("*ERROR* " + repr(e))
#
#     def listAllTableInfo(self):
#         str_sql = "select * from USER_TABLES"
#         rs = self.listDataBySQL(str_sql)
#         return rs
#
#     def listAllTableName(self):
#         list_tableInfo = self.listAllTableInfo()
#         list_tableName = [info[0] for info in list_tableInfo] if list_tableInfo != None else []
#         return list_tableName
#
#     def listTableStructure(self,table):
#         sql = "SELECT COLUMN_ID,COLUMN_NAME,DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '%s' ORDER BY COLUMN_ID" % table
#         listStructure = self.listDataBySQL(sql)
#         tableStructure = [column[1] for column in listStructure]
#         return tableStructure
#
#     def listData(self,table,attributes):
#         list_attributes = ["1-1"]
#         for k,v in attributes.items():
#             list_attributes.append(str(k) + "=" + str(v))
#         str_attributes = " and ".join(list_attributes)
#         str_sql = "select * from %s where %s" % (table,str_attributes)
#         list_rs = self.listDataBySQL(str_sql)
#         return list_rs
#
#     def listDataBySQL(self,str_sql):
#         """
#         获取所有查询结果
#         :param str_sql:
#         :return: 返回list列表，列表中的元素为Tuple类型
#         """
#         conn,cur = self.__open()
#         list_rs = []
#         try:
#             cur.excute(str_sql)
#             list_rs = cur.fetchall()
#         except Exception as e:
#             print("*ERROR* " + repr(e))
#         finally:
#             self.__close(conn,cur)
#         return list_rs
