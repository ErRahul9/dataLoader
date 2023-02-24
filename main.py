from helpers.databaseConn import DatabaseConnection
from dotenv import load_dotenv
import os


class main():

    def __init__(self):


        load_dotenv()
        self.host = os.environ['CORE_HOST']
        self.pwd = os.environ['CORE_PW']
        self.user = os.environ['CORE_USER']
        self.port = os.environ['CORE_PORT']
        self.db = os.environ['CORE_DATABASE']

        self.reportHost = os.environ['REPORT_HOST']
        self.reportPort = os.environ['REPORT_PORT']
        self.reportUser = os.environ['REPORT_USER']
        self.reportPass = os.environ['REPORT_PW']
        self.reportDb = os.environ['REPORT_DATABASE']


    def returnCoreData(self):
        sql = "select distinct(private_marketplace_id) from summarydata.all_facts WHERE hour >= '2023-02-22 16:00:00' AND hour < '2023-02-23 16:00:00' and advertiser_id = 32286 and private_marketplace_id not in ('-1','-2','-3','-4','-5','-6','-7')"
        return DatabaseConnection(self.db,self.host,self.user,self.pwd,self.port).connectToPostgres(sql)


    def returnReportData(self):
        sql = "select distinct(private_marketplace_id) from  sum_by_private_marketplace_by_day  where DAY IN ('2023-02-23') and advertiser_id = 32286"
        return DatabaseConnection(self.reportDb,self.reportHost,self.reportUser,self.reportPass,self.reportPort).connectToPostgres(sql)

    def dataComp(self):
        coreD = [data[0] for data in main().returnCoreData()]
        repD = [info[0] for info in main().returnReportData()]
        print(coreD)
        print(repD)
        missing = [data for data in repD if data not in coreD]
        common = [data for data in repD if data  in coreD]
        return missing , common

    # def countMatch(self):


if __name__ == '__main__':
    print(main().dataComp())
