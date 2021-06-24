#!/bin/bash

# @brief:
# 1. map
# 2. reduce
# 3. update mysql

if [ $# = 0 ];then # 在执行命令 ./deeplink_hour_report.sh 时，若后面不跟参数，即脚本所带参数为0时，if为真
    # pre_hour=$(date -d "-1 hour" +%Y%m%d_%H) # hdfs linux使用
    pre_hour="20210622_14" # 暂时指定
    operation_mode="hdfs"
    # echo pre_hour,${pre_hour}
    # operation_mode="file"
    # echo operation_mode,${operation_mode}
else
    pre_hour=$1
    operation_mode=$2
fi

# configure
dir_path=$(cd $(dirname $0); cd ./..; pwd)
source ${dir_path}/conf/env.sh ### 修改

map_bin=$(dirname $0)/deeplink_map.py ###
reduce_bin=$(dirname $0)/deeplink_reduce.py ###
sql_bin=$(dirname $0)/deeplink_update_db.py ###

# HDFS segment tmp path
hdfs_tmp_path=${hdfs_path}/${pre_hour}
# echo pre_hour,${pre_hour}
echo hdfs_tmp_path,${hdfs_tmp_path}

# tmp mapper file path
# deeplink_tmp_path=${dir_path}/.map_reduce/map/hour/${pre_hour}
deeplink_tmp_path=${dir_path}/map_reduce/map/hour/${pre_hour} ####### 修改
if [ ! -d ${deeplink_tmp_path} ];then
    mkdir -p ${deeplink_tmp_path}
fi

# finally output file path
output_path=${dir_path}/output/hour/${pre_hour}
if [ ! -d ${output_path} ];then
    mkdir -p ${output_path}
fi

#update db result file path
update_result_path=${dir_path}/output/result/hour/${pre_hour}
if [ ! -d ${update_result_path} ];then
    mkdir -p ${update_result_path}
fi
update_result_file=${update_result_path}/${pre_hour}.deeplink
# deal_update_result=${update_result_path}/${pre_hour}.deal_success
# req_update_result=${update_result_path}/${pre_hour}.req_success
# bid_update_result=${update_result_path}/${pre_hour}.bid_success

if [ $operation_mode != "hdfs" ];then
    # file segment tmp path
    split_tmp_path=${dir_path}/.map_reduce/split/hour/${pre_hour} 
    # split_tmp_path=${dir_path}/map_reduce/split/hour/${pre_hour}
    if [ ! -d ${split_tmp_path} ];then
        mkdir -p ${split_tmp_path}
    fi

    # step 1. split file
    echo "[INFO] [Raddus] [hour] [deeplink] [split file] [`date +%Y-%m-%d:%H:%M:%S`]"
    echo pre_hour,${pre_hour}
    deeplink_origin_file=${deeplink_log_path}/deeplink.${pre_hour}.log
    echo deeplink_origin_file,${deeplink_origin_file}
    if [ ! -f ${deeplink_origin_file} ]; then
        echo "no exist " ${deeplink_origin_file}
        exit -1
    else
        file_count=`wc -l ${deeplink_origin_file} | awk -F ' ' '{print $1}'`
        per_count=`expr ${file_count} / $split_num + 1`
        echo $file_count
        echo $per_count
        split -l ${per_count} -d ${deeplink_origin_file}  deeplink_segment_
        mv deeplink_segment_* ${split_tmp_path}
    fi

    # step 2. map
    echo "[INFO] [Raddus] [hour] [deeplink] [map] [`date +%Y-%m-%d:%H:%M:%S`]"
    python3 ${map_bin} hour ${operation_mode} ${split_tmp_path} ${deeplink_tmp_path}
    if [[ $? -ne 0 ]];then
        INFO="[Error] [Raddus] [hour] [deeplink] [map error] [`date +%Y-%m-%d:%H:%M:%S`]"
        # python3 $dingding_bin ${INFO}
        echo $INFO
        touch ${update_result_file}_success
        exit -1
    fi
fi


# step 1. map
echo "[INFO] [Raddus] [hour] [deeplink] [map] [`date +%Y-%m-%d:%H:%M:%S`]"
python3 ${map_bin} hour ${operation_mode} ${hdfs_tmp_path} ${deeplink_tmp_path} ${hdfs_hosts}
if [[ $? -ne 0 ]];then
    INFO="[Error] [Raddus] [hour] [deeplink] [map error] [`date +%Y-%m-%d:%H:%M:%S`]"
    # python3 $dingding_bin ${INFO}
    echo $INFO
    touch ${update_result_file}_success
    exit -1
fi

# step 2.reduce

echo "[INFO] [Raddus] [hour] [deeplink] [reduce] [`date +%Y-%m-%d:%H:%M:%S`]"
out_file=${output_path}/${pre_hour}.deeplink
#cat $pv_tmp_path/pv_map_*[^success] | sort -T /home/work/.tmp | python3 ${reduce_bin} > ${out_file}
# for map_file in `ls $pv_tmp_path/pv_map_* | grep -v success`
for map_file in `ls $deeplink_tmp_path/deeplink_map_* | grep -v success` #修改
do
    # sort $map_file -T /home/work/.tmp/hour/pv  > ${map_file}_pvsort 
    sort $map_file -T ${deeplink_tmp_path}/zzz > ${map_file}_deeplinksort #修改
done
wait
# sort -m $pv_tmp_path/pv_map_*_pvsort -T /home/work/.tmp/hour/pv | python3 ${reduce_bin} > ${out_file}
sort -m $deeplink_tmp_path/deeplink_map_*_deeplinksort -T ${deeplink_tmp_path}/zzz | python3 ${reduce_bin} > ${out_file} #修改

if [ $? -eq 0 ];then
    touch ${out_file}_success
    # rm $pv_tmp_path/pv_map_*_pvsort
    rm $deeplink_tmp_path/deeplink_map_*_deeplinksort #修改
else
    INFO="[Error] [Raddus] [hour] [deeplink] [reduce error] [`date +%Y-%m-%d:%H:%M:%S`]"
    # python3 $dingding_bin ${INFO}
    echo $INFO
    touch ${update_result_file}_success
    exit -1
fi


# # # step 3.update mysql
# delay=90
# pv_update_result=${out_file}_success
# while [ ${delay} -gt 0 ]
# do
#     echo "[INFO] [Raddus] [hour] [pv] [update mysql] [`date +%Y-%m-%d:%H:%M:%S`]"
#     echo pv_update_result,${pv_update_result}
#     if [ -f ${pv_update_result} ];then
#         delay=0
#         python $sql_bin hour $pre_hour
#         if [[ $? -ne 0 ]];then
#             INFO="[Error] [Raddus] [hour] [pv] [update db error] [`date +%Y-%m-%d:%H:%M:%S`]"
#             python3 $dingding_bin ${INFO}
#             echo $INFO
#             touch ${update_result_file}_success
#             exit -1
#         else
#             touch ${update_result_file}_success
#         fi
#     else
#         sleep 60
#         let delay--
#     fi
# done 


echo "[INFO] [Raddus] [hour] [deeplink] [report success] [`date +%Y-%m-%d:%H:%M:%S`]"
exit 0
