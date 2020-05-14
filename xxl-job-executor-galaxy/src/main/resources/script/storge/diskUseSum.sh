#!/bin/bash
#是否已经部署
DEPLOY=no
#脚本所在目录
BASE_DIR=base_directory
#读取配置文件
PROP_FILE="$BASE_DIR/storge.conf"


#-------------------------system-util--------------------------------

#clickhouse要查看的username
TRAFFIC_USERNAME=traffic_username
#clickhouse要查看的password
TRAFFIC_PASSWORD=traffic_password
#clickhouse ip
TRAFFIC_SERVER=traffic_server
#druid 查询节点IP和port
ANALYTIC_SERVER=analytic_server
#minio ip
FILES_SERVER=files_server


#转换当前系统时间(小时)为秒级时间戳
function timeStamp(){
current=`date "+%Y-%m-%d %H"`
timeStamp=`date -d "$current" +%s`
#将current转换为时间戳，精确到秒
currentTimeStamp=$(((timeStamp*1000+10#`date "+%N"`/1000000)/1000))
}
timeStamp

#当前时间戳
function timestamp(){
stamp=`date +%s`
}
timestamp

#计算当前实际存储与上一次存储的差值
function difference(){
dir=$1
before=`cat $dir`
now=$2
if [ $before ];then
    if [ $now -ne "0" ];then
        diff=`expr $now - $before`
        if [ $diff -eq "0" ];then
            echo 0
            else
            if [ $diff -lt "0" ];then
                echo $now > $dir
                echo 0
            else
                echo $now > $dir
                echo $diff
            fi
        fi
    else
        echo $now > $dir
        echo 0
    fi
else
    echo $now > $dir
    echo 0
fi
}
#-------------------------system-util--------------------------------


#-------------------------------clickhouse---------------------------
#获取当前存储大小
function ck_current(){
curr=`echo "SELECT SUM(bytes_on_disk) FROM parts WHERE database = 'tsg_galaxy_v3'" | curl 'http://'$TRAFFIC_USERNAME':'$TRAFFIC_PASSWORD'@'$TRAFFIC_SERVER'/?database=system' --data-binary @-`
if [ $curr -ge "0" ];then
        echo $curr > $BASE_DIR/old/clickhouse/current
        clickhouse_current="success"
        curr_ck=$curr
else
        clickhouse_current="fail"
        before=`cat $BASE_DIR/old/clickhouse/current`
        if [ $before ];then
                curr_ck=$before
        else
                echo 0 > $BASE_DIR/old/clickhouse/current
                curr_ck=0
        fi
fi
}

#获取最大存储大小
function ck_max(){
max=`echo "SELECT SUM(total_space) FROM disks" | curl 'http://'$TRAFFIC_USERNAME':'$TRAFFIC_PASSWORD'@'$TRAFFIC_SERVER'/?database=system' --data-binary @-`
if [ $max -gt "0" ];then
        echo $max > $BASE_DIR/old/clickhouse/max
        clickhouse_max="success"
        max_ck=$max
else
        clickhouse_max="fail"
        before=`cat $BASE_DIR/old/clickhouse/max`
        if [ $before ];then
                max_ck=$before
        else
                echo 0 > $BASE_DIR/old/clickhouse/max
                max_ck=0
        fi
fi
}

#获取存储天数
function ck_days(){
days=`echo "SELECT COUNT(DISTINCT(partition)) FROM parts WHERE database = 'tsg_galaxy_v3' AND partition > '20000101'" | curl 'http://'$TRAFFIC_USERNAME':'$TRAFFIC_PASSWORD'@'$TRAFFIC_SERVER'/?database=system' --data-binary @-`
if [ $days -gt "0" ];then
        echo $days > $BASE_DIR/old/clickhouse/days
        clickhouse_days="success"
        days_ck=$days
else
        clickhouse_days="fail"
        before=`cat $BASE_DIR/old/clickhouse/days`
        if [ $before ];then
                days_ck=$before
        else
                echo 0 > $BASE_DIR/old/clickhouse/days
                days_ck=0
        fi
fi
}

#获取第一天存储时间
function ck_firstday(){
firstday=`echo "SELECT DISTINCT(partition) FROM parts WHERE database = 'tsg_galaxy_v3' AND partition > '20000101' ORDER BY partition limit 1" | curl 'http://'$TRAFFIC_USERNAME':'$TRAFFIC_PASSWORD'@'$TRAFFIC_SERVER'/?database=system' --data-binary @-`
ck_stamp=`date -d $firstday +%s`
is=$?
if [ $is -eq "0" ];then
        echo $stamp > $BASE_DIR/old/clickhouse/firstday
        clickhouse_firstday="success"
        firstday_ck=$ck_stamp
else
        clickhouse_firstday="fail"
        before=`cat $BASE_DIR/old/clickhouse/firstday`
        if [ $before ];then
                firstday_ck=$before
        else
                echo $stamp > $BASE_DIR/old/clickhouse/firstday
                firstday_ck=$stamp
        fi
fi
}

ck_current
ck_max
ck_days
ck_firstday

#-------------------------------clickhouse---------------------------


#---------------------------------druid------------------------------
function dr_current(){
curr=`curl -s -XPOST -H'Content-Type: application/json' http://$ANALYTIC_SERVER/druid/v2/sql/ -d @$BASE_DIR/old/druid/sql/current.json | $BASE_DIR/old/druid/sql/jq '.[].curr_size'`
if [ $curr -ge "0" ];then
        echo $curr > $BASE_DIR/old/druid/current
        druid_current="success"
        curr_druid=$curr
else
        druid_current="fail"
        before=`cat $BASE_DIR/old/druid/current`
        if [ $before ];then
                curr_druid=$before
        else
                echo 0 > $BASE_DIR/old/druid/current
                curr_druid=0
        fi
fi
}

function dr_max(){
max=`curl -s -XPOST -H'Content-Type: application/json' http://$ANALYTIC_SERVER/druid/v2/sql/ -d @$BASE_DIR/old/druid/sql/max.json | $BASE_DIR/old/druid/sql/jq '.[].max_size'`
if [ $max -gt "0" ];then
        echo $max > $BASE_DIR/old/druid/max
        druid_max="success"
        max_druid=$max
else
        druid_max="fail"
        before=`cat $BASE_DIR/old/druid/max`
        if [ $before ];then
                max_druid=$before
        else
                echo 0 > $BASE_DIR/old/druid/max
                max_druid=0
        fi
fi
}

function dr_days(){
day=`curl -s -XPOST -H'Content-Type: application/json' http://$ANALYTIC_SERVER/druid/v2/sql/ -d @$BASE_DIR/old/druid/sql/query.json | $BASE_DIR/old/druid/sql/jq '.[].days'`
if [ $day -ge "0" ];then
    echo $day > $BASE_DIR/old/druid/days
    druid_days="success"
    days_druid=$day
else
    druid_days="fail"
    before=`cat $BASE_DIR/old/druid/days`
    if [ $before ];then
        days_druid=$before
    else
        echo 0 > $BASE_DIR/old/druid/days
        days_druid=0
    fi
fi
}

function dr_firstday(){
time=`curl -s -XPOST -H'Content-Type: application/json' http://$ANALYTIC_SERVER/druid/v2/sql/ -d @$BASE_DIR/old/druid/sql/firstday.json | $BASE_DIR/old/druid/sql/jq '.[].start'`
if [ $time ];then
    array=(${time//T/ })
    time=${array[0]}
    echo `date -d ${time:1:10} +%s` > $BASE_DIR/old/druid/first
    druid_firstday="success"
    firstday_druid=`date -d ${time:1:10} +%s`
else
    druid_firstday="fail"
    before=`cat $BASE_DIR/old/druid/first`
        if [ $before ];then
            firstday_druid=$before
        else
            now=`date +%s`
            echo $now > $BASE_DIR/old/druid/first
            firstday_druid=$now
        fi
fi
}

dr_current
dr_max
dr_days
dr_firstday

diff_druid=$(difference $BASE_DIR/old/druid/before $curr_druid)

#---------------------------------druid------------------------------

#---------------------------------minio------------------------------
function mi_current(){
curr_minio=0
curr_size=`curl -s -XPOST -H'Content-Type: application/json' http://$FILES_SERVER/minio/prometheus/metrics | grep -v \# | grep minio_disk_storage_used_bytes`
array=(${curr_size// / })
if [ ${array[1]} ];then
  if [[ ${array[1]} =~ "e" ]];then
  minio_current="success"
  temp_size=$(echo ${array[1]} | awk '{printf("%d",$0)}')
  curr_minio=`echo "$curr_minio + $temp_size"|bc`
  echo $curr_minio > $BASE_DIR/old/minio/current
  else
  minio_current="success"
  curr_minio=`echo "$curr_minio + ${array[1]}"|bc`
  echo $curr_minio > $BASE_DIR/old/minio/current
  fi
else
  minio_current="fail"
  before=`cat $BASE_DIR/old/minio/current`
        if [ $before ];then
            curr_minio=$before
        else
            echo $now > $BASE_DIR/old/minio/current
            curr_minio=0
        fi
fi
}

function mi_max(){
max_minio=0
max_size=`curl -s -XPOST -H'Content-Type: application/json' http://$FILES_SERVER/minio/prometheus/metrics | grep -v \# | grep minio_disk_storage_available_bytes`
array=(${max_size// / })
if [ ${array[1]} ];then
  if [[ ${array[1]} =~ "e" ]];then
  minio_max="success"
  temp_size=$(echo ${array[1]} | awk '{printf("%d",$0)}')
  max_minio=`echo "$max_minio + $temp_size"|bc`
  echo $max_minio > $BASE_DIR/old/minio/max
  else
  minio_max="success"
  max_minio=`echo "$max_minio + ${array[1]}"|bc`
  echo $max_minio > $BASE_DIR/old/minio/max
  fi
else
  minio_max="fail"
  before=`cat $BASE_DIR/old/minio/max`
        if [ $before ];then
            max_minio=$before
        else
            echo $now > $BASE_DIR/old/minio/max
            max_minio=0
        fi
fi

}

function mi_firstday(){
stat_time=`curl -s -XPOST -H'Content-Type: application/json' http://$FILES_SERVER/minio/prometheus/metrics | grep -v \# | grep process_start_time_seconds`
array=(${stat_time// / })
firstday_minio=$(echo ${array[1]} | awk '{printf("%d",$0)}')

is=$?
if [ $is -eq "0" ];then
minio_firstday="success"
else
minio_firstday="fail"
firstday_minio=$stamp
fi
}

mi_current
mi_max
mi_firstday

#Minio 存储天数
function miniodays(){
if [ $firstday_minio -ne "0" ];then
  diff_minio=`expr $currentTimeStamp - $firstday_minio`
  days_minio=`expr $diff_minio / 86400`
else
  days_minio=0
fi
minio_days="success"
}
miniodays


diff_minio=$(difference $BASE_DIR/old/minio/before $curr_minio)
#---------------------------------minio------------------------------


#{"time":$stamp,"log_type":"Traffic Logs","used_size":$curr_ck,"max_size":$max_ck,"aggregate_size":0,"stored_days":$days_ck,"first_storage":$firstday_ck}
#{"time":$stamp,"log_type":"Report and Metrics","used_size":$curr_druid,"max_size":$max_druid,"aggregate_size":$diff_druid,"stored_days":$days_druid,"first_storage":$firstday_druid}
#{"time":$stamp,"log_type":"Files","used_size":$curr_minio,"max_size":$max_minio,"aggregate_size":$diff_minio,"stored_days":$days_minio,"first_storage":$firstday_minio}


#echo '{"clickhouse_current":"'$clickhouse_current'","clickhouse_max":"'$clickhouse_max'","clickhouse_days":"'$clickhouse_days'","clickhouse_firstday":"'$clickhouse_firstday'","druid_current":"'$druid_current'","druid_max":"'$druid_max'","druid_days":"'$druid_days'","druid_firstday":"'$druid_firstday'","minio_current":"'$minio_current'","minio_max":"'$minio_max'","minio_days":"'$minio_days'","minio_firstday":"'$minio_firstday'"}'


echo '{"status": {"traffic_current":"'$clickhouse_current'","traffic_max":"'$clickhouse_max'","traffic_days":"'$clickhouse_days'","traffic_firstday":"'$clickhouse_firstday'","analytic_current":"'$druid_current'","analytic_max":"'$druid_max'","analytic_days":"'$druid_days'","analytic_firstday":"'$druid_firstday'","analytic_aggregate":"success","files_current":"'$minio_current'","files_max":"'$minio_max'","files_days":"'$minio_days'","files_firstday":"'$minio_firstday'","files_aggregate":"success"},"data":[{"time":'$stamp',"log_type":"Traffic Logs","used_size":'$curr_ck',"max_size":'$max_ck',"aggregate_size":0,"stored_days":'$days_ck',"first_storage":'$firstday_ck'},{"time":'$stamp',"log_type":"Report and Metrics","used_size":'$curr_druid',"max_size":'$max_druid',"aggregate_size":'$diff_druid',"stored_days":'$days_druid',"first_storage":'$firstday_druid'},{"time":'$stamp',"log_type":"Files","used_size":'$curr_minio',"max_size":'$max_minio',"aggregate_size":'$diff_minio',"stored_days":'$days_minio',"first_storage":'$firstday_minio'}]}'
