import csv
import pathlib
from pymongo import MongoClient
import os
import dateparser
import time

NOT_AVAILABLE = 'NOT_AVAILABLE'
SOURCE = 'SOURCE'
TARGET = 'TARGET'
EQUITY_TABLE = 'EQUITY_TABLE'
FUTURES_TABLE = 'FUTURES_TABLE'
EQUITY_CSV_HEADER = ['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL',
                     'TIMESTAMP', 'TOTALTRADES', 'ISIN']
FUTURE_CSV_HEADER = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'EXPIRY_DT_FINAL', 'STRIKE_PR', 'OPTION_TYP', 'OPEN', 'HIGH',
                     'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']


def connectToMongoDB():
    mongoClient = MongoClient('localhost', 27017)
    return mongoClient


def databaseName():
    return connectToMongoDB().stocks


def dbCollection(table):
    if table == EQUITY_TABLE:
        return databaseName().equity
    elif table == FUTURES_TABLE:
        return databaseName().futures
    else:
        return NOT_AVAILABLE


def folderLocation(folder):
    if folder == SOURCE:
        return '/home/leo/PycharmProjects/stocksdossier/feed_files/'
    elif folder == TARGET:
        return '/home/leo/PycharmProjects/stocksdossier/feed_files_backup/'
    else:
        return NOT_AVAILABLE


def dateParser(date):
    return dateparser.parse(date)


# def dayFormatter(date):
#     return datetime.strftime(date, '%d-%b-%Y')

def validateFuturesHeader(csv_file):
    for row in csv_file:
        for index, r in enumerate(row):
            if r != FUTURE_CSV_HEADER[index]:
                return False
        return True


def validateEquityHeader(csv_file):
    for row in csv_file:
        for index, r in enumerate(row):
            if r != EQUITY_CSV_HEADER[index]:
                return False
        return True


def equityColumnParser(row):
    return {
        'symbol': row[0], 'open': float(row[2]), 'high': float(row[3]),
        'low': float(row[4]), 'close': float(row[5]), 'total_traded_oty': int(row[8]),
        'timestamp': dateParser(row[10]), 'total_trades': int(row[11])}


def futuresColumnParser(row):
    return {
        'instrument': row[0],
        'symbol': row[1], 'expiry_date': dateParser(row[2]),
        'open': float(row[6]),
        'high': float(row[7]),
        'low': float(row[8]), 'close': float(row[9]),
        'contracts': float(row[11]),
        'open_interest': float(row[13]),
        'change_in_oi': float(row[14]),
        'timestamp': dateParser(row[15])}


def equity_data_loader(equityCollection, equityFileName):
    try:
        with open(equityFileName, mode='r') as file:
            # timestamp_final = dayFormatter(dateParser(timestampString))
            csvFile = csv.reader(file, delimiter=',')
            successRecordCounter = 0
            successInsertCounter = 0
            if validateEquityHeader(csvFile):
                for lines in csvFile:
                    if lines[0] != 'SYMBOL':
                        if lines[0] != '' and lines[2] != '' and lines[3] != '' and lines[4] != '' and lines[
                            5] != '' and \
                                lines[8] != '' and lines[10] != '' and lines[11] != '':
                            equityDocument = equityColumnParser(lines)
                            try:
                                res = equityCollection.insert_one(equityDocument)

                                if res.inserted_id:
                                    successRecordCounter += 1
                                    successInsertCounter += 1
                            except:
                                print("failed - ", equityDocument)
            else:
                print('Header mismatch - equity !!')
    except:
        print('error in file ' + equityFileName)
    finally:
        file.close()
        print('total equity records in files: ', successRecordCounter)
        print('total equity inserted records in db: ', successInsertCounter)
    if successInsertCounter == successRecordCounter:
        return True
    return False


def futures_data_loader(futuresCollection, futureFileName):
    try:
        with open(futureFileName, mode='r') as file:
            csvFile = csv.reader(file, delimiter=',')
            successRecordCounter = 0
            successInsertCounter = 0
            if validateFuturesHeader(csvFile):
                for lines in csvFile:
                    if lines[0] == 'FUTSTK':
                        if lines[0] != '' and lines[1] != '' and lines[2] != '' and lines[6] != '' and lines[
                            7] != '' and \
                                lines[
                                    8] != '' and lines[9] != '' and lines[11] != '' and \
                                lines[13] != '' and lines[14] != '' and lines[15] != '':
                            futuresDocument = futuresColumnParser(lines)
                            try:
                                res = futuresCollection.insert_one(futuresDocument)
                                if res.inserted_id:
                                    successRecordCounter += 1
                                    successInsertCounter += 1
                            except:
                                print("failed - ", futuresDocument)
            else:
                print('Header mismatch - futures !!')
    except:
        print('error in file ' + futureFileName)
    finally:
        file.close()
        print('total futures records in files: ', successRecordCounter)
        print('total futures inserted records in db: ', successInsertCounter)

    if successInsertCounter == successRecordCounter:
        return True
    return False


if __name__ == '__main__':
    try:
        targetCSVFiles = [f for f in os.listdir(folderLocation(TARGET)) if '.csv' in f.lower()]
        sourceCSVFiles = [f for f in os.listdir(folderLocation(SOURCE)) if '.csv' in f.lower()]
        if len(sourceCSVFiles) > 0:
            for file in sourceCSVFiles:
                if file not in targetCSVFiles:
                    start_time = time.time()
                    print("Started processing for file - " + file)
                    if file.endswith("_NSE.csv"):
                        sourceFileLocation = folderLocation(SOURCE) + file
                        targetFileLocation = folderLocation(TARGET) + file
                        if equity_data_loader(dbCollection(EQUITY_TABLE), sourceFileLocation):
                            pathlib.Path(sourceFileLocation).rename(targetFileLocation)
                    elif file.endswith("_NSEFO.csv"):
                        sourceFileLocation = folderLocation(SOURCE) + file
                        targetFileLocation = folderLocation(TARGET) + file
                        if futures_data_loader(dbCollection(FUTURES_TABLE), sourceFileLocation):
                            pathlib.Path(sourceFileLocation).rename(targetFileLocation)
                    print("End processing for file - " + file)
                    print("completed in = %s seconds ---" % (time.time() - start_time))
    except:
        print('ERROR IN READING FOLDER - ', folderLocation(SOURCE))
