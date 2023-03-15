import re

from helpers.databaseConn import DatabaseConnection
from dotenv import load_dotenv
import os
from helpers.jsonReader import jsonReader
from sys import argv





class main():

    def __init__(self):
        load_dotenv()
        self.baseDb = argv[1]
        self.tgtDb = argv[2]
        self.baseDate = argv[3]
        self.tgtDate = argv[4]
        self.adv_id = argv[5]

    def returnData(self,query,src):
        host = os.environ['{}_HOST'.format(src.upper())]
        pwd = os.environ['{}_PW'.format(src.upper())]
        user = os.environ['{}_USER'.format(src.upper())]
        port = os.environ['{}_PORT'.format(src.upper())]
        db = os.environ['{}_DATABASE'.format(src.upper())]
        sqlData = self.readSql(src)
        sql = sqlData.get(query).lower()
        advsql = self.setAdvertiserId(sql)
        finalSQL = self.setStartEndDate(advsql)
        print(finalSQL)
        return DatabaseConnection(db, host, user, pwd,port).connectToPostgres(finalSQL)


    def setAdvertiserId(self,sql):
        sqlUpdated = re.sub("\\d{5}",self.adv_id,sql)
        return sqlUpdated

    def setStartEndDate(self,updatedQuery):
        if "hour" in updatedQuery.lower():
            tTime = self.baseDate + " 00:00:00"
            eTime = self.tgtDate + " 00:00:00"
            updatedStartTime = re.sub(r">= '2023-02-22 00:00:00'",">= '{}'".format(tTime),updatedQuery)
            updateQueryTime = re.sub(r"< '2023-02-23 00:00:00'","< '{}'".format(eTime),updatedStartTime)
        elif "day" in updatedQuery.lower():
            updatedStartTime = re.sub(r">= '2023-02-22'", ">= '{}'".format(self.baseDate), updatedQuery)
            updateQueryTime = re.sub(r"< '2023-03-01'", "< '{}'".format(self.tgtDate), updatedStartTime)

        return updateQueryTime

    def readSql(self,datType):
        jsonObject = jsonReader("data","sqlVals.json").readJson()
        data =  jsonObject.get(datType)
        return data

    def setQueryfromParameters(self,query,src):
        sqlData = self.readSql(src)
        sql = sqlData.get(query).lower()
        sqlWithAdv = self.setStartEndDate(self.setAdvertiserId(sql))
        return sqlWithAdv


    def dataComp(self):
        jsonObjectCore = self.readSql(self.baseDb)
        for keys in jsonObjectCore.keys():
            print(keys)
            coreD = [data[0] for data in main().returnData(keys,self.baseDb)]
            print(coreD)
            repD = [info[0] for info in main().returnData(keys,self.tgtDb)]
            print(repD)
            missing = [data for data in repD if data not in coreD]
            common = [data for data in repD if data  in coreD]
            print(missing)
            print(common)

if __name__ == '__main__':
    print(main().dataComp())
