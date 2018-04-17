#encoding:utf-8

from daoServer.DbServer import DbServer
from entity.Table import Table

class Compare:
    def __init__(self):
        pass
    def listAllTableName(self,db):
        '''
        获取数据库中所有的表名
        :param db: "mysql,root/123456@127.0.0.1:3306/db_flask"
        :return: list 表名列表
        '''
        db_server = DbServer()
        list_tablenames = db_server.listAllTableName(db)
        return list_tablenames

    def tableFilter(self,db,tables):
        """
        过滤出数据库中存在的表
        :param db: "mysql,root/123456@127.0.0.1:3306/db_flask"
        :param tables: str tablename1,tablename2 (用英文逗号隔开) \n
        :return: list 存在的表名列表
        """
        list_tables = tables.split(",")
        list_db_tables = self.listAllTableName(db)
        tables_exsit = [table for table in list_tables if table in list_db_tables]
        return tables_exsit

    def listLostTables(self,db,tables):
        '''
        获取表tables中在数据库db中不存在的表
        :param db: dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param tables: str tablename1,tablename2 (用英文逗号隔开) \n
        :return: list 丢失的表名列表
        '''
        list_tables = tables.split(",")
        list_db_tables = self.listAllTableName(db)
        list_lost_tables = [table for table in list_tables if table not in list_db_tables]
        return list_lost_tables

    def listTablesStructure(self,db,tables):
        pass
    def checkTablesInDb(self,resourceDb,targetDb,tables=None):
        '''
        依次验证表是否存在与两个数据库中
        :param resourceDb: dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param targetDb: dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param tables: str tablename1,tablename2 (用英文逗号隔开) \n
        :return:
        '''
        # 获取两个数据库中丢失的表
        lost_tables_in_ref = self.listLostTables(resourceDb, tables)
        lost_tables_in_tar = self.listLostTables(targetDb, tables)
        mark = True
        if len(lost_tables_in_ref) > 0 or len(lost_tables_in_tar) > 0:
            mark = False
        return mark, lost_tables_in_ref, lost_tables_in_tar

    def compTablesStructure(self,resTable,tagTable,sequential="1",col_properties="1"):
        if isinstance(resTable,Table) and isinstance(tagTable,Table):
            #获取列名列表
            resColumns = [structure[0] for structure in resTable.getStructure()]
            tagColumns = [structure[0] for structure in tagTable.getStructure()]

            list_msg = []
            mark = True
            #验证列名个数
            if len(resColumns) != len(tagColumns):
                list_msg.append("table:%s columns:%d != table:%s columns:%d" %(resTable.getName(),len(resColumns),
                                                                   tagTable.getName(),len(tagColumns)))
            else:
                if sequential == "1":
                    for i in range(len(tagColumns)):
                        if tagColumns[i] != resColumns[i]:
                            list_msg.append("target:%s sequential != refence:%s sequential!" %(tagTable.getName(),resTable.getName()))

            #获取新增字段与减少字段
            columns_add = [column for column in tagColumns if column not in resColumns]
            columns_del = [column for column in resColumns if column not in tagColumns]
            if len(columns_add) > 0:
                list_msg.append("Added columns:%s" % ",".join(columns_add))
            if len(columns_del) > 0:
                list_msg.append("Deleted columns:%s" % ",".join(columns_del))

            #验证字段属性
            if col_properties == "1":
                columns_both = [column for column in tagColumns if column in resColumns]
                dict_res = {structure[0]:structure for structure in resTable.getStructure() if structure[0] in columns_both}
                dict_tag = {structure[0]:structure for structure in tagTable.getStructure() if structure[0] in columns_both}
                for col in columns_both:
                    if dict_res[col] != dict_tag[col]:
                        list_msg.append("refrence column-%s:%s != target column-%s:%s" %(col,dict_res[col],
                                                                                         col,dict_tag[col]))
            if len(list_msg)>0:
                mark = False
            msg = "\n".join(list_msg)
            return mark,msg






    def compareData(self,resTable,tagTable):
        if isinstance(resTable,Table) and isinstance(tagTable,Table):
            #开始对比数据
            list_msg = ["FAIL: %s != %s" % (resTable.getInfo(),tagTable.getInfo())]
            if len(resTable.getData()) != len(tagTable.getData()):
                list_msg.append("%s %d rows != %s %d rows" % (
                    resTable.getInfo(),len(resTable.getData()),tagTable.getInfo(),len(tagTable.getData())
                ))
            else:
                for line,resRow in enumerate(resTable.getData()):
                    if resRow not in tagTable.getData():  #该行不在目标表中，此行数据可能丢失
                        if len(resTable.getPrimarykey()) > 0: # 主键不为空，要进行详细对比
                            bool_row_status,str_rs = self.__compareRowData(resTable,tagTable,resRow)
                            if not bool_row_status:
                                list_msg.append(str_rs)
                            else:
                                list_msg.append("%s line %d is not found in %s\n(%s)" %(
                                    resTable.getInfo(),line+1,tagTable.getInfo(),",".join(map(lambda col:str(col),resRow))
                                ))
            if len(list_msg) == 1:
                print("*INFO* PASS: %s = %s " %(resTable.getInfo(),tagTable.getInfo()))
            msg = ("\n").join(list_msg)
            return len(list_msg) == 1, msg

    def __compareRowData(self,resTable,tagTable,resRow):
        list_resPri_index = [resTable.getStructure().index(key) for key in resTable.getPrimarykey()]
        list_tagPri_index = [tagTable.getStructure().index(key) for key in tagTable.getPrimarykey()]
        tag_index = []
        if not resTable.getView():
            tag_index = [tagTable.getStructure().index(resCol) for resCol in resTable.getStructure()]
        list_info = []
        for index in list_resPri_index:
            list_info.append("%s=%s" % (resTable.getStructure()[index],resRow[index]))
        line_info = " and ".join(list_info)
        errorMsg = ""
        find_row = False
        for tagRow in tagTable.getDb():
            flag = 0
            for i in xrange(len(list_resPri_index)):
                if resRow[list_resPri_index[i]] == tagRow[list_tagPri_index[i]]:
                    flag += 1
                else:
                    break
            if flag == len(list_resPri_index):  #说明找到该行，开始对比数据
                find_row = True
                list_msg = []
                if not resTable.getView():  #不是视图，列名全部同名
                    for i in xrange(len(tag_index)):
                        if resRow[i] != tagRow[tag_index[i]]:
                            str_msg = "%s(%s != %s)" %(resTable.getStructure()[i],resRow[i],tagRow[tag_index[i]])
                            list_msg.append(str_msg)
                else: #表为视图，列名可能不一样，但是对比的数据列序一一对应
                    for i in xrange(len(resRow)):
                        if resRow[i] != tagRow[i]:
                            str_msg = "%s.<%s> != %s.<%s>" % (
                            resTable.getStructure()[i],resRow[i],tagTable.getStructure()[i],tagRow[i])
                            list_msg.append(str_msg)
                if len(list_msg) > 0:
                    errorMsg = "Line %s    mismache columns:%s" % (line_info,"".join(list_msg))
                break # 找到这行，退出循环
        if not find_row:
            errorMsg = "Line %s is not found in %s" % (line_info,tagTable.getInfo())
        return errorMsg == "",errorMsg

