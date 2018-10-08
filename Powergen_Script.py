import urllib.request
import pyodbc
import json
from pandas import read_sql
import xlrd
import xlrd as xl
from pandas import ExcelWriter
from pandas import ExcelFile
import sqlite3
import csv

'''-------------------------------------------------------------------------------
               converting json to csv file
-------------------------------------------------------------------------------'''
url = "http://api.worldbank.org/v2/datacatalog?format=json"
request = urllib.request.Request(url)
response = urllib.request.urlopen(request)
data_content = response.read()
print(data_content)

# inputfile = open("E:/Powerg/datacatalog.json","r")
outputfile = open("E:/Powerg/catalog.csv", "w")


# js = json.load(inputfile)

js = json.loads(data_content)

catalog = js["datacatalog"]

for i in catalog:
    print(i['id'])
    for n in i["metatype"]:
        row = i['id']+","+str(n['id']) + "," + str(n['value'])+"\n"
        outputfile.write(row)


book = xlrd.open_workbook("E:\Powerg\Cleanedup_Data.xlsx")
sheet = book.sheet_by_name("Sheet1")

conn = pyodbc.connect('Trusted_Connection= yes', DRIVER='{SQL Server}', SERVER='---YOUR SERVER NAME---',DATABASE='PowerGen')
cursor = conn.cursor()

con = sqlite3.connect('E:\Powerg\Powergen.db')
table_name = 'Powerg_file'

'''-------------------------------------------------------------------------------
               Storing the excel file to sql server
-------------------------------------------------------------------------------'''
query = """create table Powerg_file(
            name varchar (1000),
            acronym varchar (1000),
            description varchar (1000),
            url varchar (1000),
            type varchar (1000),
            languagesupported varchar (1000),
            periodicity varchar (1000),
            economycoverage varchar (1000),
            granularity varchar (1000),
            numberofeconomies varchar (1000),
            topics varchar (1000),
            updatefrequency varchar (1000),
            updateschedule varchar (1000),
            lastrevisiondate varchar (1000) ,
            contactdetails varchar (1000),
            accessoption varchar (1000),
            bulkdownload varchar (1000),
            cite varchar (1000),
            detailpageurl varchar (1000),
            popularity varchar (1000),
            coverage varchar (1000),
            api varchar (1000),
            apiaccessurl varchar (1000),
            apisourceid varchar (1000))"""


insert = """INSERT INTO Powerg_file(name,
                    acronym,
                    description,
                    url,
                    type,
                    languagesupported,
                    periodicity,
                    economycoverage,
                    granularity,
                    numberofeconomies,
                    topics,
                    updatefrequency,
                    updateschedule,
                    lastrevisiondate,
                    contactdetails,
                    accessoption,
                    bulkdownload,
                    cite,
                    detailpageurl,
                    popularity,
                    coverage,
                    api,
                    apiaccessurl,
                    apisourceid)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
for r in range(1,sheet.nrows):
    name = sheet.cell(r,0).value
    acronym = sheet.cell(r, 1).value
    description = sheet.cell(r, 2).value
    url = sheet.cell(r, 3).value
    type = sheet.cell(r, 4).value
    languagesupported = sheet.cell(r, 5).value
    periodicity = sheet.cell(r, 6).value
    economycoverage = sheet.cell(r, 7).value
    granularity = sheet.cell(r, 8).value
    numberofeconomies = sheet.cell(r, 9).value
    topics = sheet.cell(r, 10).value
    updatefrequency = sheet.cell(r, 11).value
    updateschedule = sheet.cell(r, 12).value
    lastrevisiondate = sheet.cell(r, 13).value
    contactdetails = sheet.cell(r, 14).value
    accessoption = sheet.cell(r, 15).value
    bulkdownload = sheet.cell(r, 16).value
    cite = sheet.cell(r, 17).value
    detailpageurl = sheet.cell(r,18).value
    popularity = sheet.cell(r, 19).value
    coverage = sheet.cell(r, 20).value
    api = sheet.cell(r, 21).value
    apiaccessurl = sheet.cell(r, 22).value
    apisourceid = sheet.cell(r, 23).value

    values =(name,acronym,description,url,type,languagesupported,periodicity,economycoverage,granularity,numberofeconomies,topics,updatefrequency,updateschedule,lastrevisiondate,contactdetails,accessoption,bulkdownload,cite,detailpageurl,popularity,coverage,api,apiaccessurl,apisourceid)
    cursor.execute(insert,values)

    # # cursor.close()
conn.commit()


'''-------------------------------------------------------------------------------
               creating sqlite database
-------------------------------------------------------------------------------'''
sql = '''SELECT [name]
      ,[type]
      ,[languagesupported]
      ,[granularity]
      ,[numberofeconomies]
      ,[lastrevisiondate]
      ,[popularity]
      ,[coverage]
  FROM [PowerGen].[dbo].[Powerg_file]'''

df = read_sql(sql, conn)
print(df.head())
df.to_sql(table_name, con, if_exists='replace', index=False)

conn.commit()
conn.close()
