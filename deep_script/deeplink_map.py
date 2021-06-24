# -*- coding: utf-8 -*-
import sys
import os
import time
import json
import pyhdfs
from pyhdfs import HdfsClient
from concurrent.futures import ProcessPoolExecutor

'''
若使用hdfs形式 修改地方
2021.06.22 batch_execute() do_map（）
'''
g_count_execute = 0
hour_max_workers = 16
day_max_workers = 32

def business(report_type, out_file, file_out , data, operation_type):
    flag =0
    for line in data:
        if operation_type == "file":
            jsonString = line.rstrip()
            # if (len(jsonString) > 0) and (jsonString[0] != '{'):
            if (len(jsonString) > 0):
                linePos = jsonString.find('{')
                if linePos >= 0:
                    jsonString = jsonString[linePos:]
                else:

                    continue
            try:
                jsonObj = json.loads(jsonString)
            except:
                continue
        else:
            try:
                jsonObj = json.loads(line.decode())
            except:
                continue
        try:
        # if 1:
            flag = flag+1 # 文件行数统计

            # print('star')
            datetime = jsonObj['_time'] # 时间戳
            # 时间的处理
            if report_type == 'hour':
                lineNum = 13
                formatStr = "%Y-%m-%d %H"
            else:
                lineNum = 10
                formatStr = "%Y-%m-%d"
            timeStamp = convert_to_time_stamp(datetime[0:lineNum], formatStr)

            # 读取 vendor_id
            vendorId = jsonObj.get('vendor_id', '-1')
            if vendorId=='-1':
                print('error this vendorId is :',vendorId)
                break
            mediaId = jsonObj.get('appid', '-') ####
            if mediaId=='-':
                print('error this mediaId is :',mediaId)
                break
            adspotId = jsonObj.get('adspotid', '-1') ####
            if adspotId=='-1':
                print('error this adspotId is :',adspotId)
                break
            supplierId = jsonObj.get('real_supplier_id', jsonObj.get('supplier_id', '-1')) ####
            if supplierId=='-1':
                print('error this supplierId is :',supplierId)
                break
            
            # deviceId 处理
            deviceId = '-'
            deviceOs = jsonObj.get('os', '-1')
            if deviceOs == '1' and 'idfa' in jsonObj:
                deviceId = jsonObj.get('idfa', '-')
            elif deviceOs == '2':
                if 'oaid' in jsonObj:
                    deviceId = jsonObj.get('oaid', '-')
                    if type(deviceId)==bool:
                        print('deviceId',deviceId,flag)
                    if len(deviceId) == 0 and 'imei' in jsonObj:
                        deviceId = jsonObj.get('imei', '-')
                elif 'imei' in jsonObj:
                    deviceId = jsonObj.get('imei', '-')
                elif 'androidid' in jsonObj:
                    deviceId = jsonObj.get('androidid', '-')
                elif 'mac' in jsonObj:
                    deviceId = jsonObj.get('mac', '-')

            auctionId = jsonObj.get('sid', '-')

            # renren map old mediaID #######################      要不要转换？？？？
            '''
            if mediaID == '100395':
                mediaID = '100556'
            '''
            # key集合 vendorId->vendor_id; mediaId->appid; adspotId->adspotid; supplierId->supplier_id

            keySeq = ('deeplink', str(timeStamp), vendorId, mediaId, adspotId, supplierId)
            keyDauSeq = ('deeplink_DAU', str(timeStamp), mediaId)
            seprate = (':')
            key = seprate.join(keySeq)
            keyDau = seprate.join(keyDauSeq)
            valueDau = (deviceId) 
            valueKey = (auctionId, deviceId)
            value = seprate.join(valueKey)
            map_string = '{}\t{}\n'.format(key, value)
            out_file.write(map_string)
            # print('here???')
            if (deviceId == '-' or deviceId == '' or deviceId == None):
                continue
            map_Dau_string = '{}\t{}\n'.format(keyDau, valueDau)##valueDau deviceId
            out_file.write(map_Dau_string)
        # except:
        except Exception as e:
            print(e) #打印所有异常到屏幕
            if isinstance(adspotId, list):
                adspotStr = ''.join(adspotId)
            else:
                adspotStr = str(adspotId)
            keySeq = ('ERRORLINE', str(timeStamp), adspotStr, '98')
            seprate = (':')
            key = seprate.join(keySeq)
            value = auctionId
            print('there is a error data, please checking!!!')
            map_string = '{}\t{}\n'.format(key, value)
            out_file.write(map_string)
            continue

    print('flag',flag) # 文件行数统计
    out_file.close()
    command = "touch " + file_out + "_success"
    os.system(command)

def do_map(report_type ,file_in, file_out,hdfs_hosts):
    if isinstance(hdfs_hosts, str) and str(hdfs_hosts) == "file":
        with open(file_in, 'r') as data:
            out_file = open(file_out, 'w')
            business(report_type, out_file, file_out, data, "file")
    else:
        # client = HdfsClient(hosts=hdfs_hosts, randomize_hosts=False, timeout=30, max_tries=3, retry_delay=5)
        # with client.open(file_in) as data: 
        with open(file_in, 'r') as data: ########### 修改
            out_file = open(file_out, 'w')
            # business(report_type, out_file, file_out, data, "hdfs")
            business(report_type, out_file, file_out, data, "file") ################  修改
            

def convert_to_time_stamp(dateString, formatStr):
    time_tuple = time.strptime(dateString, formatStr)
    timeStamp = time.mktime(time_tuple)
    return int(timeStamp)


def run_map(**kwargs):
    report_type = kwargs["report_type"]
    file_in = kwargs["file_in"]
    file_out = kwargs["file_out"]
    hdfs_hosts = kwargs["hdfs_hosts"]
    print("start to handle ", file_in)
    time.sleep(1)
    do_map(report_type, file_in, file_out,hdfs_hosts)

def file_map(**kwargs):
    report_type = kwargs["report_type"]
    file_in = kwargs["file_in"]
    file_out = kwargs["file_out"]
    print("start to handle ", file_in)
    time.sleep(1)

    do_map(report_type, file_in, file_out, "file")

def batch_execute(report_type, path_in, path_out,hdfs_hosts):
    print('batch_execute()')
    if isinstance(hdfs_hosts, str) and str(hdfs_hosts) == "file":
        if report_type == "hour":
            file_prefix = "deeplink_segment_"
            max_workers = hour_max_workers
        else:
            file_prefix = "deeplink." + path_out[-10:]
            max_workers = day_max_workers

        pool = ProcessPoolExecutor(max_workers=max_workers)

        results = []
        for file in os.listdir(path_in):
            if file.startswith(file_prefix):
                file_in = os.path.join(path_in, file)
                if report_type == "hour":
                    file_out = os.path.join(path_out, "deeplink_map_" + file[-2:])
                else:
                    hour_str = file.split('.')[-2]
                    file_out = os.path.join(path_out, "deeplink_map_" + hour_str)
                result = pool.submit(file_map, report_type=report_type, file_in=file_in, file_out=file_out)
                result.add_done_callback(batch_one_done)
                results.append(result)
        pool.shutdown()
    else:
        if report_type == "hour":
            file_prefix = "deeplink."
            max_workers = hour_max_workers
        else: # report_type = day
            file_prefix = "deeplink."
            max_workers = day_max_workers

        pool = ProcessPoolExecutor(max_workers=max_workers)
        client = HdfsClient(hosts=hdfs_hosts,randomize_hosts=False, timeout=30,max_tries=3,retry_delay=5)
        results = []
        if report_type == "hour":
            # fileslist = client.listdir(path_in) #raddus/* 每一个
            path_in = '.' + path_in  ############################################  修改
            fileslist = os.listdir(path_in) # /raddus/20210617_15
            # print('fileslist',fileslist)
            for file in fileslist: # 修改？请确认
                if file.startswith(file_prefix):
                    file_in = os.path.join(path_in, file)
                    key_str = file.split('.')[-2]
                    file_out = os.path.join(path_out, "deeplink_map_" + key_str)
                    result = pool.submit(run_map, report_type=report_type, file_in=file_in, file_out=file_out,hdfs_hosts=hdfs_hosts)
                    result.add_done_callback(batch_one_done)
                    results.append(result)
        else:
            day_str = path_in.split('/')[-1]
            path_in = '/raddus/'
            fileslist = client.listdir(path_in)
            for file_list in fileslist:
                if file_list.startswith(day_str):
                    log_path = path_in + file_list + '/'
                    log_list = client.listdir(log_path)
                    for file in log_list:
                        if file.startswith(file_prefix):
                            file_in = os.path.join(log_path, file)
                            key_str = file.split('.')[-2]
                            file_out = os.path.join(path_out, "deeplink_map_" + key_str)
                            result = pool.submit(run_map, report_type=report_type, file_in=file_in, file_out=file_out,hdfs_hosts=hdfs_hosts)
                            result.add_done_callback(batch_one_done)
                            results.append(result)
        pool.shutdown()


def batch_one_done(result):
    global g_count_execute
    g_count_execute += 1
    print("process {} is done!".format(g_count_execute))


if __name__ == '__main__':
    hdfs_hosts = []
    if len(sys.argv) == 5 or len(sys.argv) == 6:
        report_type = sys.argv[1]
        if (report_type != "hour") and (report_type != "day"):
            print("Error report type: use hour or day")
        operation_mode = sys.argv[2]
        path_in = sys.argv[3]
        path_out = sys.argv[4]
        hosts_list = sys.argv[5]
        print('operation_mode,path_in,path_out,hosts_list\n',operation_mode,'\n',path_in,'\n',path_out,'\n',hosts_list)
        #  file # /raddus/ # /Users/zzz/Downloads/Interstellar-master/raddus/.map_reduce/map/hour/ # 192.168.3.1,192.168.3.2
        if (operation_mode == "hdfs"):
            hosts_list = sys.argv[5]
            hdfs_hosts.append(hosts_list.split(',')[0])
            hdfs_hosts.append(hosts_list.split(',')[1])
            batch_execute(report_type, path_in, path_out, hdfs_hosts)
        else:
            batch_execute(report_type, path_in, path_out, "file")
    else:
        print("Usage: {} hour|day path_in path_out".format(sys.argv[0]))
        print("Usage: {} day file_in file_out retry".format(sys.argv[2]))
        sys.exit(-1)
