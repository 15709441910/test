#!/usr/bin/python3
# -*- coding: utf-8 -*-
import time
import MySQLdb
# import mysqlclient 
import os
import sys
import logging

# reload(sys)
# sys.setdefaultencoding('utf-8')

rootPath = os.path.abspath(os.path.dirname(__file__)) +'/../../util'
sys.path.append(rootPath)
sys.path.append('.')

from utils import Connect_Utils
# from common_notify import DingDingAPI

# dingding = DingDingAPI()

def insertPvReportToDB(action_type,last_time_block,file_name_array,db,logger):

    if action_type == "hour":
        timeFormat = '%Y%m%d_%H'
        tableName = 'ssp_report_hourly'
        dupTableName = 'ssp_duplicate_report_hourly'
        errTable = 'ssp_error_hourly'
    else:
        timeFormat = '%Y%m%d'
        tableName = 'ssp_report_daily'
        dupTableName = 'ssp_duplicate_report_daily'
        errTable = 'ssp_error_daily'

    time_tuple = time.strptime(last_time_block,timeFormat)
    lastTimeBlockStamp = int(time.mktime(time_tuple))
    sspMapMediaAndAdspot = Connect_Utils().get_ssp_map_media_and_adspot()

    pvDataList = {}
    pvDauDataList = {}
    seprate = (':')

    cursor = db.cursor()
    for file_name in file_name_array:
        file = open(file_name)
        for line in file:
            items = line.strip('\n').split('\t')
            if  'ERRORLINE' == items[0]:
                (recordType, timeStamp, adspotID, action, counts, auctionIdStr) = items ##### here???
                ##current exception process
                if int(timeStamp) != lastTimeBlockStamp:
                    continue

                data = (int(timeStamp), adspotID, action, int(counts), auctionIdStr)

                if (int(counts) > 1000):
                    error_message = '[ERROR][REPORT] adspotID %s at %s error lines for action %s count aboved 1000' % (adspotID, last_time_block, action)
                    logger.error(error_message)
                    print('error_message',error_message)
                    # dingding.robot_notify(error_message)

                errReportSql = "insert into " + errTable + " (timestamp,adspot_id,action,counts,auctions) values (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE counts =values(counts),auctions =values(auctions)"
                n = cursor.execute(errReportSql, data)
                db.commit()
                message  = "Successfully insert " + errTable + " records count:%d for timeStamp %s" % (n,last_time_block)
                print (message)
                logger.info(message)

            elif 'deeplink' == items[0]:
                # vendor = sspID 
                # vendorId->vendor_id; mediaId->appid; adspotId->adspotid; supplierId->supplier_id
                (type, timeStamp, sspID, mediaID, adspotID, supplierId, pvs,deviceNum)  = items
                # check mediaID, adspotID,exchangeID
                mapKey = seprate.join((str(adspotID), str(mediaID), str(sspID)))
                if not (str(mediaID).startswith('10') and str(adspotID).startswith('100') and str(sspID).startswith('70')) or mapKey not in sspMapMediaAndAdspot:
                    continue
                if int(timeStamp) != lastTimeBlockStamp:
                    continue
                keyString = seprate.join((timeStamp,sspID,mediaID,adspotID,supplierId)) # 增加 supplierId

                if (pvDataList.get(keyString, None) == None):
                    pvDataList[keyString] = (timeStamp,sspID, mediaID, adspotID, supplierId, pvs,deviceNum) # 增加 supplierId
                else:
                    (timeStampOld, sspIDOld, mediaIDOld, adspotIDOld, supplierIdOld, pvs_old,deviceNum_old) = pvDataList[keyString]
                    pvDataList[keyString] = (timeStamp,sspID,mediaID,adspotID, supplierId, int(pvs)+int(pvs_old),int(deviceNum)+int(deviceNum_old))

            elif 'deeplink_DAU' == items[0]:
                (type, timeStamp, mediaID, deviceNum) = items
                if int(timeStamp) != lastTimeBlockStamp:
                    continue
                keyString = seprate.join((timeStamp, mediaID))

                if (pvDauDataList.get(keyString, None) == None):
                    pvDauDataList[keyString] = (timeStamp, mediaID, deviceNum)
                else:
                    (timeStampOld, mediaIDOld, deviceNumOld) = pvDauDataList[keyString]
                    pvDauDataList[keyString] = (timeStamp, mediaID, int(deviceNum) + int(deviceNumOld))

        file.close()

    count = 0
    countDau = 0
    dupCount = 0
    dupCountDau = 0
    hasError = False
    dauhasError = False

    for items in pvDataList.values():
        (timeStamp,sspID,mediaID,adspotID,supplierId, pvs, deviceNum)  = items
        result1 = createUpdateDBRecordsForPV(tableName, timeStamp, sspID, mediaID, adspotID, supplierId, pvs, deviceNum, cursor, last_time_block, logger)
        count += 1
        result2 = createUpdateDBRecordsForPV(dupTableName, timeStamp, sspID, mediaID, adspotID,supplierId, pvs, deviceNum, cursor, last_time_block, logger)
        dupCount += 1
        if (not result1 or not result2):
            hasError = True
            break

    for items in pvDauDataList.values():
        (timeStamp, mediaID, deviceNum) = items
        result1 = createUpdateDBRecordsForPVDau(tableName, timeStamp, mediaID, deviceNum, cursor, last_time_block, logger)
        countDau += 1
        result2 = createUpdateDBRecordsForPVDau(dupTableName, timeStamp, mediaID, deviceNum, cursor, last_time_block, logger)
        dupCountDau += 1
        if (not result1 or not result2):
            dauhasError = True
            break

    cursor.close()
    
    if(not hasError):
        #do db Commit
        db.commit()
        message  = "Successfully update "+tableName+" records count:%d for timeStamp %s" % (count,last_time_block)
        message2 = "Successfully update "+dupTableName+" records count:%d for timeStamp %s" % (dupCount,last_time_block)
    else:
        message  = "Failed to update "+tableName+" records count:%d for timeStamp %s" % (count,last_time_block)
        message2 = "Failed to update "+dupTableName+" records count:%d for timeStamp %s" % (dupCount,last_time_block)

    if (not dauhasError):
        # do db Commit
        db.commit()
        message3 = "Successfully update dau " + tableName + " records count:%d for timeStamp %s" % (countDau, last_time_block)
        message4 = "Successfully update dau " + dupTableName + " records count:%d for timeStamp %s" % (
        dupCountDau, last_time_block)
    else:
        message3 = "Failed to update dau " + tableName + " records count:%d for timeStamp %s" % (countDau, last_time_block)
        message4 = "Failed to update dau " + dupTableName + " records count:%d for timeStamp %s" % (
        dupCountDau, last_time_block)

    logger.info(message)
    logger.info(message2)
    logger.info(message3)
    logger.info(message4)
    print (message)
    print (message2)
    print (message3)
    print (message4)

def createUpdateDBRecordsForPVDau(tableName, timeStamp, mediaID, deviceNum, cursor, last_time_block, logger):
    try:
        checkExistsSql = "select timestamp,pvs from " + tableName + " where timestamp=%s and media_id='%s' and (channel_id!='165' and channel_id!='166') " % (int(timeStamp), mediaID)
        cursor.execute(checkExistsSql)
        checkExistsResult = cursor.fetchall()

        if (len(checkExistsResult) > 0):
            reportSql = "update " + tableName + " SET dau=%s where timestamp=%s and media_id='%s' and channel_id not in ('165','166') " % (
            int(deviceNum), int(timeStamp), mediaID)
            cursor.execute(reportSql)
        else:
            reportSql = "insert into " + tableName + " (timestamp,ssp_id,media_id,adspot_id,mock_adspot_id,adx,channel_id,owner_id,strategy_id,company_id,customer_id,advertisement_id,creative_id,dau) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) " \
                        % (int(timeStamp), '-1', mediaID, '-1', '-1', '0', '1', '-1', '-1', '-1', '-1', '-1', '-1',
                           int(deviceNum))
            cursor.execute(reportSql)
        return True
    except MySQLdb.Error as e:
        error_message = '[Raddus-Report][DB] save ssp report pv_dau table %s at %s throw mysql error %d: %s' % (
        tableName, last_time_block, e.args[0], e.args[1])
        logger.error(error_message)
        print('error_message',error_message)
        # dingding.robot_notify(error_message)
        return False

def createUpdateDBRecordsForPV(tableName,timeStamp,sspID,mediaID,adspotID,supplierId,pvs,deviceNum,cursor,last_time_block,logger):
    try:
        checkExistsSql = "select timestamp,pvs from "+tableName+" where timestamp=%s and ssp_id='%s' and media_id='%s' and adspot_id='%s' and (channel_id!='165' and channel_id!='166') " % (int(timeStamp),sspID,mediaID,adspotID,supplierId)
        cursor.execute(checkExistsSql)
        checkExistsResult = cursor.fetchall()

        if(len(checkExistsResult)>0):
            reportSql = "update "+tableName+" SET pvs=%s,pv_device=%s where timestamp=%s and ssp_id='%s' and media_id='%s' and adspot_id='%s' and channel_id not in ('165','166') " % (int(pvs),int(deviceNum),int(timeStamp),sspID,mediaID,adspotID,supplierId)
            cursor.execute(reportSql)
        else:
            reportSql = "insert into "+tableName+" (timestamp,ssp_id,media_id,adspot_id,mock_adspot_id,adx,channel_id,owner_id,strategy_id,company_id,customer_id,advertisement_id,creative_id,pvs) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) " \
                                                 % (int(timeStamp),sspID,mediaID,adspotID,supplierId,'0','1','-1','-1','-1','-1','-1','-1',int(pvs)) # 修改
                                                #  % (int(timeStamp),sspID,mediaID,adspotID,adspotID,'0','1','-1','-1','-1','-1','-1','-1',int(pvs))
            cursor.execute(reportSql)
        return True
    except MySQLdb.Error as e:
        error_message ='[Raddus-Report][DB] save ssp report pv table %s at %s throw mysql error %d: %s'% (tableName,last_time_block,e.args[0], e.args[1])
        logger.error(error_message)
        print('error_message',error_message)
        # dingding.robot_notify(error_message)
        return False

def generateMediaChannal(exchangeID, channelID, mediaID, ownerId):
    if (ownerId == '1'):
        #品牌
        if mediaID == '100299' or mediaID == '100300':
            media_channel = '19999'
        elif mediaID == '100323' and channelID != '81' and channelID != '102' and channelID != '54' and channelID != '26' and channelID != '43':
            media_channel = '19999'
        #人人-汇量
        elif exchangeID == '70222' and channelID == '103' :
            media_channel = '20000'
        # 人人-品友
        elif exchangeID == '70222' and channelID == '210':
            media_channel = '16666'
        else:
            media_channel = '18888'
    elif (ownerId == '0' and channelID == '1'):
        media_channel = '0'
    elif (ownerId == '0' and channelID != '1'):
        media_channel = channelID
    else:
        #default to channel ID
        media_channel = channelID
    return media_channel

def insertReportAndBillingDailyLogToDb(action_type,file_name_array,last_time_block,logger):
    db = Connect_Utils().get_report_conn()

    insertPvReportToDB(action_type,last_time_block,file_name_array,db,logger)
    db.close()

def localOutputDataInsertDB(action_type,last_time_block,logger):
    if action_type == "hour":
        folderStr = 'hour'
        retry_minutes = 60
    else:
        folderStr = 'day' #12*60=720? 360?
        retry_minutes = 360

    scriptHomeDir = os.path.abspath(os.path.dirname(__file__)) + '/..'
    localPvOutputSuccessFile = scriptHomeDir + '/output/' + folderStr + '/' + last_time_block + '/' + last_time_block + '.pv_success'

    insertDBFinished = False
    while retry_minutes > 0:
        if not os.path.exists(localPvOutputSuccessFile) :
            logger.info('waiting for MR job finished with output path: %s , %s, %s, %s' % (localPvOutputSuccessFile))
            retry_minutes -= 1
            time.sleep(60)
        else:
            # localPvFile = scriptHomeDir + '/output/' + folderStr + '/' + last_time_block + '/' + last_time_block + '.pv' 
            localPvFile = '/Users/zzz/Downloads/Interstellar-master/raddus/output/hour/20210617_15/20210617_15.pv'
            print ('localPvFile',localPvFile)
            insertReportAndBillingDailyLogToDb(action_type,[localPvFile],last_time_block,logger)
            insertDBFinished = True
            break

    if not insertDBFinished:
        error_message = '[Raddus-Report][LOCAL]local pv output file not exists for time block  ' + last_time_block + ':' + localPvOutputSuccessFile
        logger.error(error_message)
        print('error_message',error_message)
        # dingding.robot_notify(error_message)

if __name__ == '__main__':
    print('start')
    if len(sys.argv) != 3:
        print("Usage: {} hour|day time_block")
        sys.exit(-1)

    action_type = sys.argv[1]
    last_time_block = sys.argv[2]
    logger = logging.getLogger('insert data from bos to mysql db')
    if action_type == "hour":
        # fh = logging.FileHandler('../../log/raddus/pv_db_hour_report_log.'+str(last_time_block))
        fh = logging.FileHandler('./raddus/pv_db_hour_report_log.'+str(last_time_block))
    elif action_type == "day":
        # fh = logging.FileHandler('../../log/raddus/pv_db_day_report_log.'+str(last_time_block))
        fh = logging.FileHandler('./raddus/pv_db_day_report_log.'+str(last_time_block))
    else:
        print("Error action type: use hour or day")

    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    localOutputDataInsertDB(action_type, last_time_block, logger)
