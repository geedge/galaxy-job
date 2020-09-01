#!/bin/bash
#
#sql文件放在当前脚本所在目录下，配置好数据库连接信息，执行脚本
##数据库信息
HOSTNAME="127.0.0.1"
PORT="3306"
USERNAME="root"
PASSWORD="111111"
##数据库名称
DBNAME="xxl_job"

SQL_FILE=`pwd`/*.sql
SQLS=`cat $SQL_FILE`

echo "开始执行sql内容 ： $SQLS"
result=`mysql -h${HOSTNAME} -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${SQLS}";`
echo "执行完成 $result"