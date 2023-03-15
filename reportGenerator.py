import csv
import datetime
import glob
import os
import re
import shutil
from sys import argv
import pandas as pd
from helpers.databaseConn import DatabaseConnection
from helpers.jsonReader import jsonReader
from dotenv import load_dotenv


class ReportGenerator:

    def __init__(self):
        load_dotenv()
        self.baseDb = argv[1]
        self.tgtDb = argv[2]
        self.baseDate = argv[3]
        self.tgtDate = argv[4]
        if len(argv) == 5:
            self.typeOfData = ""

        else:
            self.typeOfData = argv[5]



    def setStartEndDate(self,updatedQuery):
        if "hour" in updatedQuery.lower():
            tTime = self.baseDate + " 00:00:00"
            eTime = self.tgtDate + " 00:00:00"
            updatedStartTime = re.sub(r">= '2023-02-22 00:00:00'",">= '{}'".format(tTime),updatedQuery)
            updateQueryTime = re.sub(r"< '2023-02-23 00:00:00'","< '{}'".format(eTime),updatedStartTime)
        elif "day" in updatedQuery.lower():
            updatedStartTime = re.sub(r">='2023-02-23'", ">= '{}'".format(self.baseDate), updatedQuery)
            updateQueryTime = re.sub(r"<'2023-03-01'", "< '{}'".format(self.tgtDate), updatedStartTime)
        return updateQueryTime


    def returnData(self, src, allSqls=[]):
        host = os.environ['{}_HOST'.format(src.upper())]
        pwd = os.environ['{}_PW'.format(src.upper())]
        user = os.environ['{}_USER'.format(src.upper())]
        port = os.environ['{}_PORT'.format(src.upper())]
        db = os.environ['{}_DATABASE'.format(src.upper())]
        if len(self.typeOfData) == 0:
            allSqls = os.listdir("data/sql")
        else:
            allSqls.append(self.typeOfData+".sql")
        for sqls in allSqls:
            sql = open("data/sql/{}".format(sqls),"r").read()
            updatedSQL = self.setStartEndDate(sql)
            print(updatedSQL)
            results =  DatabaseConnection(db, host, user, pwd,port).connectToPostgres(updatedSQL)
            header =  open("data/reportTemplate.csv").read()
            headers =[]
            for recs in header.split(","):
                headers.append(recs.strip())
            print(headers)
            with open("sqlResults/report_{}_{}.csv".format(sqls.split(".sql")[0],datetime.datetime.now().strftime('%Y-%m-%d')),"w+") as outFile:
                writer = csv.DictWriter(outFile,fieldnames=headers)
                writer.writeheader()
                writeRows = csv.writer(outFile)
                for rows in results:
                    writeRows.writerow(rows)
            outFile.close()

    def combineResults(self,allReports=[]):
        allReports = os.path.join("sqlResults","report_*.csv")
        files = glob.glob(allReports)
        print(files)
        df = pd.concat(map(pd.read_csv, files), ignore_index=True)
        newDf = df.sort_values(by='day')
        newDf.to_csv("sqlResults/final_report_{}.csv".format(datetime.datetime.now()))
        for file in files:
            destPath = os.path.join("sqlResults/archive",file.split("/")[1])
            shutil.move(file,destPath)




if __name__ == '__main__':
    print(ReportGenerator().returnData("report"))
    print(ReportGenerator().combineResults("report"))