#!/bin/bash

# @brief:
# 1. map
# 2. reduce
# 3. update mysql

pre_day=$(date -d "-1 day" +%Y%m%d)
pre_day=20210622
if [[ $# == 1 ]]; then
    pre_day=$1
fi


# configure
dir_path=$(cd $(dirname $0); cd ./..; pwd)
source ${dir_path}/conf/env.sh

# map_bin=$(dirname $0)/pv_map.py # 没使用
reduce_bin=$(dirname $0)/deeplink_reduce.py
sql_bin=$(dirname $0)/deeplink_update_db.py
# marge_bin=$(dirname $0)/pv_marge.py # 没使用


# HDFS segment tmp path
hdfs_tmp_path=${hdfs_path}/${pre_day}

# tmp mapper file path
pv_tmp_path=${dir_path}/.map_reduce/map/day/${pre_day} 
# deeplink_tmp_path=${dir_path}/map_reduce/map/day/${pre_day} #
if [ ! -d ${deeplink_tmp_path} ];then
    mkdir -p ${deeplink_tmp_path}
fi

# finally output file path
output_path=${dir_path}/output/day/${pre_day}
if [ ! -d ${output_path} ];then
    mkdir -p ${output_path}
fi

#update db result file path
update_result_path=${dir_path}/output/result/day/${pre_day}
if [ ! -d ${update_result_path} ];then
    mkdir -p ${update_result_path}
fi
# update_result_file=${update_result_path}/${pre_day}.pv
# deal_update_result=${update_result_path}/${pre_day}.deal_success
# bid_update_result=${update_result_path}/${pre_day}.bid_success
# req_update_result=${update_result_path}/${pre_day}.req_success

# step 1. cat hour map file
# step 2 reduce
echo "[INFO] [Raddus] [day] [deeplink] [reduce] [`date +%Y-%m-%d:%H:%M:%S`]"
out_file=${output_path}/${pre_day}.deeplink
time_seconds=$(date -d $pre_day +%s)

# cat ${hour_tmp_path}/${pre_day}_*/deeplink_map_* | grep -v success | awk -F ':' -v OFS=':'  '{$2="'$time_seconds'";print $0}' > $deeplink_tmp_path/deeplink_awk_done 
# 上一句指令为啥每一行的首个字符串多了冒号“:”，如 deeplink::7022
cat ${hour_tmp_path}/${pre_day}_*/deeplink_map_* | grep -v success > $deeplink_tmp_path/deeplink_awk_done
# sort $deeplink_tmp_path/deeplink_awk_done -T /home/work/.tmp/day/pv | python3 ${reduce_bin} > ${out_file}
echo finish process
sort $deeplink_tmp_path/deeplink_awk_done -T ${hour_tmp_path}/day/deeplink | python3 ${reduce_bin} > ${out_file} # 修改
# rm ${hour_tmp_path}/${pre_day}_*/deeplink_map_*  # 修改
# rm $deeplink_tmp_path/deeplink_awk_done # 修改

if [ $? -eq 0 ];then
    touch ${out_file}_success
else
    INFO="[Error] [Raddus] [day] [pv] [reduce error] [`date +%Y-%m-%d:%H:%M:%S`]"
    python3 $dingding_bin ${INFO}
    echo $INFO
    touch ${update_result_file}_success
    exit -1
fi

# step 3.update mysql
# delay=240
# while [ ${delay} -gt 0 ]
# do
#     echo "[INFO] [Raddus] [day] [pv] [update mysql] [`date +%Y-%m-%d:%H:%M:%S`]"
#     if [ -f ${deal_update_result} -a -f ${req_update_result} -a -f ${bid_update_result} ];then
#         delay=0
#         python2 $sql_bin day $pre_day
#         if [[ $? -ne 0 ]];then
#             INFO="[Error] [Raddus] [day] [pv] [update db error] [`date +%Y-%m-%d:%H:%M:%S`]"
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

# rm $pv_tmp_path/pv_map_*[^success]

echo "[INFO] [Raddus] [day] [pv] [report success] [`date +%Y-%m-%d:%H:%M:%S`]"
exit 0
