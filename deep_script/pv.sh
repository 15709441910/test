#!/bin/bash

# @brief:
# 1. map
# 2. reduce
# 3. update mysql
# dir_path=$(cd $(dirname $0); cd ./..; pwd)
# source ${dir_path}/conf/env.sh
sql_bin=$(dirname $0)/test.py
python2 $sql_bin
