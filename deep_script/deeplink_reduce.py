#!/usr/bin/python
# -*- coding: utf-8 -*-

from operator import itemgetter
import sys
from operator import itemgetter
from itertools import groupby

def read_input(file):
    for line in file:
        yield line.rstrip().split('\t')

def calLogfileReducer():
    data = read_input(sys.stdin)
    for key, valuePairList in groupby(data, itemgetter(0)):
        # print('key',key)
        try:
            # print('keys',keys)
            keys=key.split(":")
            type = keys[0]
            if type == 'ERRORLINE':
                (lineType,timeStamp,adspotId,action) = keys
                timeStamp = int(timeStamp)
                counts = 0
                auctionIds = []
                for valuePair in valuePairList:
                    auctionIds.append(valuePair[1])
                    counts += 1
                auctionIdStr = ','.join(auctionIds)
                print('{}\t{}\t{}\t{}\t{}\t{}'.format('ERRORLINE',timeStamp,adspotId,action,counts,auctionIdStr))

            # elif type == 'PV':
            #     (type, timeStamp, vendor, mediaID, adspotID) = keys
            #     (pvs,deviceNum) = calculateValue(valuePairList)
            #     print('{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(type, int(timeStamp), vendor, mediaID, adspotID, pvs,deviceNum))
            elif type == 'deeplink':
                # print('here1',keys)
                (type, timeStamp, vendor, mediaID, adspotID, supplierId) = keys
                # print('key',type, timeStamp, vendor, mediaID, adspotID, supplierId)
                # if adspotID == '10000971' and supplierId == '199':
                #     flag = 1
                # else:
                #     flag = 0
                # (deeplink_s,deviceNum) = calculateValue(valuePairList,flag)
                (deeplink_s,deviceNum) = calculateValue(valuePairList)
                # print('values',deeplink_s,deviceNum)
                print('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(type, int(timeStamp), vendor, mediaID, adspotID, supplierId, deeplink_s, deviceNum))

            elif type == 'deeplink_DAU':
                (type, timeStamp, mediaID) = keys
                (deviceNum) = calculateDauValue(valuePairList)
                print('{}\t{}\t{}\t{}'.format(type, int(timeStamp), mediaID, deviceNum))

        except:
            continue


# def calculateValue(valuePairList,flag):
def calculateValue(valuePairList):
    counts = 0
    deviceNum = 0
    DeviceIdKeyDict = {}
    for valuePair in valuePairList:
        # print('valuePair[1]',valuePair[1])
        valueArray = valuePair[1].split(':')
        deviceId = valueArray[1]
        # if flag:
        #     print('deviceId',deviceId)
        counts += 1
        if (DeviceIdKeyDict.get(deviceId, None) == None):
            DeviceIdKeyDict[deviceId] = 1
            deviceNum += 1
    return counts,deviceNum


def calculateDauValue(valuePairList):
    deviceNum = 0
    DeviceIdKeyDict = {}
    for valuePair in valuePairList:
        # print('valuePair',valuePair)
        deviceId = valuePair[1]
        if (DeviceIdKeyDict.get(deviceId, None) == None):
            DeviceIdKeyDict[deviceId] = 1
            deviceNum += 1
    return deviceNum

if __name__ == "__main__":
    calLogfileReducer()
