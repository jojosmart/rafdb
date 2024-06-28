#!/bin/bash

./rafdb_main --db_dir=/home/leveldb/data/ --logfile=/home/leveldb/log/db.log --pidfile=/home/leveldb/pid/dbpid.log --check_interval=7200 --thread_num=23 --image_cache_refresh_interval=60
