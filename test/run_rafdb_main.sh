#!/bin/bash
mkdir -p data_1 data_2 data_3
mkdir -p log
rm -rf data_1/* data_2/* data_3/*
HOME_DIR=/home/xiaoliang.zhou/spider_service/storage/rafdb/test
./rafdb_main --v=6 --db_dir=$HOME_DIR/data_1/  --pidfile=$HOME_DIR/pid/dbpid1.log --check_interval=7200 --thread_num=23 --rafdb_list=192.168.14.146:10020:1,192.168.14.146:10021:2,192.168.14.146:10022:3 --rafdb_self=192.168.14.146:10020:1 --logfile=$HOME_DIR/log/db1.log 1>$HOME_DIR/log/db1.log 2>&1 &
./rafdb_main --v=6 --db_dir=$HOME_DIR/data_2/  --pidfile=$HOME_DIR/pid/dbpid2.log --check_interval=7200 --thread_num=23 --rafdb_list=192.168.14.146:10020:1,192.168.14.146:10021:2,192.168.14.146:10022:3 --rafdb_self=192.168.14.146:10021:2 --logfile=$HOME_DIR/log/db2.log 1>$HOME_DIR/log/db2.log 2>&1 &
./rafdb_main --v=6 --db_dir=$HOME_DIR/data_3/  --check_interval=7200 --thread_num=23 --rafdb_list=192.168.14.146:10020:1,192.168.14.146:10021:2,192.168.14.146:10022:3 --rafdb_self=192.168.14.146:10022:3 --logfile=$HOME_DIR/log/db3.log 1>$HOME_DIR/log/db3.log 2>&1 &
