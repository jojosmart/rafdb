a=`ps -ef | grep rafdb_main| grep -v grep | awk '{print $2}'`
kill -9 $a
echo $a
