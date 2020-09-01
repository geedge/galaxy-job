#!/bin/bash
#是否已经部署
DEPLOY=no
#脚本所在目录
BASE_DIR=base_directory

#clickhouse要查看的username
TRAFFIC_USERNAME=traffic_username
#clickhouse要查看的password
TRAFFIC_PASSWORD=traffic_password
#clickhouse ip
TRAFFIC_SERVER=traffic_server

date=`date -d "1 day ago" +"%Y%m%d"`
function now(){
curr=`echo "SELECT SUM(bytes_on_disk) FROM parts WHERE database = 'tsg_galaxy_v3' AND partition = '$date'" | curl 'http://'$TRAFFIC_USERNAME':'$TRAFFIC_PASSWORD'@'$TRAFFIC_SERVER'/?database=system' --data-binary @-`
if [ $curr -ge "0" ];then
        echo $curr > $BASE_DIR/old/clickhouse/aggregate_size
        clickhouse_aggregate="success"
        aggregate_ck=$curr
else
        clickhouse_aggregate="fail"
        before=`cat $BASE_DIR/old/clickhouse/aggregate_size`
        if [ $before ];then
                aggregate_ck=$before
        else
                echo 0 > $BASE_DIR/old/clickhouse/aggregate_size
                aggregate_ck=0
        fi
fi

}

now
#转换当前系统时间(小时)为秒级时间戳
function timeStamp(){
current=`date "+%Y-%m-%d %H"`
timeStamp=`date -d "$current" +%s`
#将current转换为时间戳，精确到秒
currentTimeStamp=$(((timeStamp*1000+10#`date "+%N"`/1000000)/1000))
yesterday=`expr $currentTimeStamp - 38800`
}
timeStamp

echo '{"status":{"traffic_aggregate":"'$clickhouse_aggregate'"},"data":[{"time":'$yesterday',"log_type":"Traffic Logs","used_size":0,"max_size":0,"aggregate_size":'$aggregate_ck',"stored_days":0,"first_storage":0}]}'
