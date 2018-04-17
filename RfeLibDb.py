#encoding:utf-8

from entityServer.TableServer import TableServer
from daoServer.DbServer import DbServer
from biz.Compare import Compare

class RfeLibDb():
    ROBOT_LIBRARY_SCOPE = "GLOBAL"
    ROBOT_LIBRARY_VERSION = 1.0
    ROBOT_LIBRARY_DOC_FARMAT = "TEXT"

    failureException = AssertionError

    def __init__(self):
       self.comp = Compare()

    def listAllTableName(self,db):
        '''
        获取数据库中所有的表名
        :param db: "mysql,root/123456@127.0.0.1:3306/db_flask"
        :return: list 表名列表
        '''
        list_tablenames = self.comp.listAllTableName(db)
        return list_tablenames

    def getTablesInDb(self,db,tables):
        pass

    def removeTables(self,tables,target):
        pass

    def listLostTables(self,db,tables):
        '''
        获取表tables中在数据库db中不存在的表
        :param db: "mysql,root/123456@127.0.0.1:3306/db_flask"
        :param tables: str tablename1,tablename2 (用英文逗号隔开) \n
        :return: list 丢失的表名列表
        '''
        list_lost_tables = self.comp.listLostTables(db,tables)
        return list_lost_tables

    def tableFilter(self,db,tables):
        """
        过滤出数据库中存在的表
        :param db: "mysql,root/123456@127.0.0.1:3306/db_flask"
        :param tables: str tablename1,tablename2 (用英文逗号隔开) \n
        :return: list 存在的表名列表
        """
        tables_exsit = self.comp.tableFilter(db,tables)
        return tables_exsit

    def listTableStructure(self,db,table):
        db_server = DbServer()
        table_structure = db_server.listTableStructure(db,table)
        return table_structure

    def listData(self,db,table,**attributes):
        pass

    def getStringFromList(self,list,join=","):
        return ",".join(list)

    def assertTablesInDb(self,resourceDb,targetDb,tables=None):
        '''
        依次验证表是否存在与两个数据库中
        :param resourceDb: dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param targetDb: dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param tables: str tablename1,tablename2 (用英文逗号隔开) ,当不传时默认None，表示验证两个数据库中所有表
        :return:
        '''

        if tables == None:  # 验证两个数据库中所有表名
            tables = ",".join(self.listAllTableName(targetDb))

        # 判断要验证的表是否为空
        if len(tables) == 0:
            print("*WARN* No tables needs to check!")
            self.fall()
        mark,lost_tables_in_ref,lost_tables_in_tar = self.comp.checkTablesInDb(resourceDb,targetDb,tables)
        if mark:
            print("*INFO* All tablenames marched!")
        else:
            if len(lost_tables_in_ref) > 0:
                print("*WARN* db:%s,missed tables:%s" % (resourceDb, ",".join(lost_tables_in_ref)))
            if len(lost_tables_in_tar) > 0:
                print("*WARN* db:%s,missed tables:%s" % (targetDb, ",".join(lost_tables_in_tar)))

    def assertTablesStructure(self,resourceDb,targetDb,tables=None,sequential = "1",col_properties="1",primarykey="",**attributes):
        """
        比较两个库中同名表结构是否一致
        :param resourceDb: str dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param targetDb: str dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param tables: str tablename1,tablename2 (用英文逗号隔开) 不传时默认比较所有目标数据库表结构\n
        :param sequential: 当为1时表示需对比表字段的顺序
        :param col_properties: 当为1时表示需对比字段的属性如类型长度等是否完全一致
        :return:
        """
        # 验证两个数据库中所有表结构
        if tables == None:
            tables = ",".join(self.listAllTableName(targetDb))
        # 判断要验证的表是否为空
        if len(tables) == 0:
            print("*WARN* No tables needs to check!")
            self.fall()
        #验证表名：
        self.assertTablesInDb(resourceDb,targetDb,tables)
        #过滤两数据库中同名表：
        list_tables = self.tableFilter(targetDb,tables)
        list_tables = self.tableFilter(resourceDb,",".join(list_tables))
        #比较同名表结构
        list_msg = []
        marched_tables = []
        unmarched_tables = []
        for table in list_tables:
            tableServer = TableServer(resourceDb,table,primarykey,attributes)
            resTable = tableServer.createTable()
            tableServer = TableServer(targetDb, table, primarykey, attributes)
            tagTable = tableServer.createTable()

            mark,msg=self.comp.compTablesStructure(resTable,tagTable,sequential,col_properties)
            if mark:
                marched_tables.append(tagTable.getName())
                print("*INFO* Table:%s structure match!" % tagTable.getName())
            else:
                unmarched_tables.append(tagTable.getName())
                print("*WARN* Table:%s structure not match!" % tagTable.getName())
                print(msg)
        if len(marched_tables)>0:
            print("*INFO* structure march tables:%s" % ",".join(marched_tables))
        if len(unmarched_tables)>0:
            print("*WARN* structure not march tables:%s" % ",".join(unmarched_tables))


    def assertTablesData(self,resourceDb,targetDb,tables,primaryKey="",**attributes):
        """
        比较两个库中同名表数据是否一致 \n
        * hive数据对比时，hive同一个路径下的文件前缀必须一致。\n
        :param resourceDb: str dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param TargetDb: str dbtype,user/pwd@ip:port/dbname (mysql,oracle) \n
        :param tables: str tablename1,tablename2 (用英文逗号隔开) \n
        :param primaryKey: str 主键，如果传入则根据主键报详细错误 不分列序 *多张表时，主键可能不一致这时需要分多次比较 \n
        :param attributes: dict 根据属性查询表数据进行对比，多个属性时做 与 查询 * 多张表时，须确认是否有共同的属性 \n
        :return: 断言结果
        """
        list_msg = []
        for table in tables.split(","):
            tableServer = TableServer(resourceDb,table,primaryKey,attributes)
            resTable = tableServer.createTable()
            tableServer = TableServer(targetDb,table,primaryKey,attributes)
            tagTable = tableServer.createTable()
            status,errorMsg = self.comp.compareData(resTable,tagTable)
            if not status:
                list_msg.append(errorMsg)
            if len(list_msg) > 0:
                self.failureException("\n".join(list_msg))

if __name__=="__main__":
    # oracleDb = "oracle,pdwdata_uat/123456@10.20.112.123:1521/pdw"
    # mysqlDb = "mysql,pdw2/Paic1234@10.20.81.16:33515/pdw2"
    # tables = "busi_fm_cz_bc_project_info"
    # test = RfeLibDb()
    # test.assertTablesData(oracleDb,mysqlDb,tables)
    print("hello world")
    # resoureDb = "mysql,root/123456@127.0.0.1:3306/db_flask"
    # targetDb = "mysql,root/123456@127.0.0.1:3306/db_flask_clone"
    dev = "mysql,root/6tfc^YHN@10.0.127.16:3306/sodap"
    pro = "mysql,root/6tfc^YHN@10.0.127.10:3306/sodap"
    ref_obj = RfeLibDb()

    # list_tablenames = ref_obj.listAllTableName(resoureDb)
    # print(list_tablenames)

    # lost_tablenames = ref_obj.listLostTables(resoureDb,"anse,user,quest")
    # print(lost_tablenames)

    # ref_obj.assertTablesInDb(resoureDb,targetDb)

    ref_obj.assertTablesStructure(dev,pro)
