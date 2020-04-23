#
# XXL-JOB v2.2.0
# Copyright (c) 2015-present, xuxueli.

CREATE database if NOT EXISTS `xxl_job` default character set utf8mb4 collate utf8mb4_unicode_ci;
use `xxl_job`;

SET NAMES utf8mb4;

-- ----------------------------
-- Table structure for `xxl_job_group`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_group`;
CREATE TABLE `xxl_job_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_name` varchar(64) NOT NULL COMMENT '执行器AppName',
  `title` varchar(12) NOT NULL COMMENT '执行器名称',
  `address_type` tinyint(4) NOT NULL DEFAULT 0 COMMENT '执行器地址类型：0=自动注册、1=手动录入',
  `address_list` varchar(512) DEFAULT NULL COMMENT '执行器地址列表，多地址逗号分隔',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_group
-- ----------------------------
INSERT INTO `xxl_job_group` VALUES ('3', 'galaxy-executor', '数据平台业务执行器', '1', '192.168.40.224:8886');

-- ----------------------------
-- Table structure for `xxl_job_info`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_info`;
CREATE TABLE `xxl_job_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_group` int(11) NOT NULL COMMENT '执行器主键ID',
  `job_cron` varchar(128) NOT NULL COMMENT '任务执行CRON',
  `job_desc` varchar(255) NOT NULL,
  `add_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `author` varchar(64) DEFAULT NULL COMMENT '作者',
  `alarm_email` varchar(255) DEFAULT NULL COMMENT '报警邮件',
  `executor_route_strategy` varchar(50) DEFAULT NULL COMMENT '执行器路由策略',
  `executor_handler` varchar(255) DEFAULT NULL COMMENT '执行器任务handler',
  `executor_param` varchar(4000) DEFAULT '' COMMENT '执行器任务参数',
  `executor_block_strategy` varchar(50) DEFAULT NULL COMMENT '阻塞处理策略',
  `executor_timeout` int(11) NOT NULL DEFAULT 0 COMMENT '任务执行超时时间，单位秒',
  `executor_fail_retry_count` int(11) NOT NULL DEFAULT 0 COMMENT '失败重试次数',
  `glue_type` varchar(50) NOT NULL COMMENT 'GLUE类型',
  `glue_source` mediumtext DEFAULT NULL COMMENT 'GLUE源代码',
  `glue_remark` varchar(128) DEFAULT NULL COMMENT 'GLUE备注',
  `glue_updatetime` datetime DEFAULT NULL COMMENT 'GLUE更新时间',
  `child_jobid` varchar(255) DEFAULT NULL COMMENT '子任务ID，多个逗号分隔',
  `trigger_status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '调度状态：0-停止，1-运行',
  `trigger_last_time` bigint(13) NOT NULL DEFAULT 0 COMMENT '上次调度时间',
  `trigger_next_time` bigint(13) NOT NULL DEFAULT 0 COMMENT '下次调度时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_info
-- ----------------------------
INSERT INTO `xxl_job_info` VALUES ('11', '3', '0 0/5 * * * ?', 'Traffic-Top-Internal-Host-by-Sessions', '2020-03-03 09:48:02', '2020-04-13 04:36:01', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_client_ip AS source, sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num, sum( c2s_pkt_num ) AS c2s_pkt_num, sum( s2c_pkt_num ) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time, \'sessions\' as order_by FROM internal_host_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_client_ip ORDER BY session_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-INTERNAL-HOST-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-03 09:48:02', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('13', '3', '0 0/5 * * * ?', 'Traffic-Top-Internal-Host-by-Bytes', '2020-03-04 09:04:42', '2020-04-13 04:35:50', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_client_ip AS source, sum( session_num ) AS session_num, sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num, sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'bytes\' as order_by FROM internal_host_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_client_ip ORDER BY c2s_byte_num+s2c_byte_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-INTERNAL-HOST-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-04 09:04:42', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('14', '3', '0 0/5 * * * ?', 'Traffic-Top-Internal-Host-by-Packets', '2020-03-04 09:06:33', '2020-04-13 04:35:38', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_client_ip AS source, sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num, sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'packets\' as order_by FROM internal_host_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_client_ip ORDER BY c2s_pkt_num+s2c_pkt_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-INTERNAL-HOST-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-04 09:06:33', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('15', '3', '0 0/5 * * * ?', 'Traffic-Top-External-Host-by-Sessions	', '2020-03-04 09:09:16', '2020-04-13 04:35:26', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_server_ip AS destination, sum( session_num ) AS session_num, sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num, sum( s2c_pkt_num ) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'sessions\' as order_by FROM external_host_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_server_ip ORDER BY session_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-EXTERNAL-HOST-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-04 09:09:16', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('16', '3', '0 0/5 * * * ?', 'Traffic-Top-External-Host-by-Bytes', '2020-03-04 09:11:06', '2020-04-13 04:35:08', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_server_ip AS destination, sum( session_num ) AS session_num, sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'bytes\' as order_by FROM external_host_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_server_ip ORDER BY c2s_byte_num+s2c_byte_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-EXTERNAL-HOST-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-04 09:11:06', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('17', '3', '0 0/5 * * * ?', 'Traffic-Top-External-Host-by-Packets', '2020-03-04 09:12:53', '2020-04-13 04:34:53', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\r\n\"requestBody\":{\"query\":\"SELECT common_server_ip AS destination, sum( session_num ) AS session_num, sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'packets\' as order_by FROM external_host_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_server_ip ORDER BY c2s_pkt_num+s2c_pkt_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-EXTERNAL-HOST-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-04 09:12:53', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('18', '3', '0 0/5 * * * ?', 'Traffic-Top-User-by-Sessions', '2020-03-05 10:03:07', '2020-04-13 04:34:34', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_subscriber_id as subscriber_id,sum( session_num ) AS session_num, sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num, sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'sessions\' as order_by FROM user_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_subscriber_id ORDER BY session_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-USER-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-05 10:03:07', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('19', '3', '0 0/5 * * * ?', 'Traffic-Top-User-by-Bytes', '2020-03-05 10:05:36', '2020-04-13 04:35:56', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_subscriber_id as subscriber_id,sum( session_num ) AS session_num, sum( c2s_byte_num ) AS c2s_byte_num, sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num, sum( s2c_pkt_num ) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as \\\"time\\\",\'bytes\' as order_by FROM user_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_subscriber_id ORDER BY c2s_byte_num+s2c_byte_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-USER-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-05 10:05:36', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('20', '3', '0 0/5 * * * ?', 'Traffic-Top-User-by-Packets', '2020-03-05 10:07:36', '2020-04-13 04:45:12', 'Galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT common_subscriber_id as subscriber_id, sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num, sum( s2c_pkt_num ) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'packets\' as order_by FROM user_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY common_subscriber_id ORDER BY c2s_pkt_num+s2c_pkt_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-USER-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-05 10:07:36', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('21', '3', '0 0/5 * * * ?', 'Traffic-Top-Website-Domain-by-Sessions', '2020-03-05 10:59:20', '2020-04-13 04:53:14', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT domain,sum(session_num) AS session_num, sum(c2s_byte_num) AS c2s_byte_num, sum(s2c_byte_num) AS s2c_byte_num, sum(c2s_pkt_num) AS c2s_pkt_num,sum(s2c_pkt_num) AS s2c_pkt_num, TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'sessions\' as order_by FROM website_domain_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY domain ORDER BY session_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-WEBSITE-DOMAIN-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-05 10:59:20', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('22', '3', '0 0/5 * * * ?', 'Traffic-Top-Website-Domain-by-Bytes', '2020-03-05 11:00:22', '2020-04-13 04:55:21', 'galaxy', '', 'FAILOVER', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT domain,sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'bytes\' as order_by FROM website_domain_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY domain ORDER BY c2s_byte_num+s2c_byte_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-WEBSITE-DOMAIN-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-05 11:00:22', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('23', '3', '0 0/5 * * * ?', 'Traffic-Top-Website-Domain-by-Packets', '2020-03-05 11:01:38', '2020-04-13 04:54:40', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT domain,sum( session_num ) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum( c2s_pkt_num ) AS c2s_pkt_num,sum( s2c_pkt_num ) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,\'packets\' as order_by FROM website_domain_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY domain ORDER BY c2s_pkt_num+s2c_pkt_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-WEBSITE-DOMAIN-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-05 11:01:38', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('24', '3', '0 0 0 1/1 * ? ', '设置原始日志存储配额（ClickHouse保留天）', '2020-03-23 10:34:04', '2020-04-14 07:07:24', 'zhq', '', 'FIRST', 'deleteTrafficDataJobHandler', '{\"maxdays\":365}', 'DISCARD_LATER', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-23 10:34:04', '', '1', '1587312000000', '1587398400000');
INSERT INTO `xxl_job_info` VALUES ('25', '3', '0 0 0 1/1 * ? ', '设置统计数据存储配额( Druid 保留天)', '2020-03-23 10:35:24', '2020-04-14 07:07:24', 'zhq', '', 'FIRST', 'deleteReportAndMetricsDataJobHandler', '{\"maxdays\":365}', 'DISCARD_LATER', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-23 10:35:24', '', '1', '1587312000000', '1587398400000');
INSERT INTO `xxl_job_info` VALUES ('26', '3', '0 0 0 1 * ?', '删除所有原始日志(ClickHouse)', '2020-03-23 10:36:56', '2020-04-13 05:25:53', 'zhq', '', 'FIRST', 'deleteAllTrafficDataJobHandler', '{\"maxdays\":30}', 'DISCARD_LATER', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-23 10:36:56', '', '0', '0', '0');
INSERT INTO `xxl_job_info` VALUES ('27', '3', '0 0 0 1 * ?', '删除所有统计数据(Druid)', '2020-03-23 10:38:28', '2020-04-13 05:23:05', 'zhq', '', 'FIRST', 'deleteAllReportAndMetricsDataJobHandler', '{\"maxdays\":365}', 'DISCARD_LATER', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-23 10:38:28', '', '0', '0', '0');
INSERT INTO `xxl_job_info` VALUES ('29', '3', '0 0 4 * * ?', '获取CK存储配额-sh(实时压缩负值问题）', '2020-03-26 10:58:09', '2020-04-15 04:31:51', 'zhq', '', 'FIRST', 'getStorageQuotaJobHandler', '{\"scriptFile\":\"/script/storge/ckDayDisk.sh\",\"topic\":\"SYS-STORAGE-LOG\",\"updateScript\":\"false\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-26 10:58:09', '', '1', '1587326400000', '1587412800000');
INSERT INTO `xxl_job_info` VALUES ('30', '3', '0 0/5 * * * ?', 'country-ip-num', '2020-03-31 08:32:17', '2020-04-08 04:14:20', 'ch', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.222:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"select country,APPROX_COUNT_DISTINCT_DS_HLL(ip_object) as ip_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as \\\"time\\\" from source_country_ip_num_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') group by country \",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"COUNTRY-IP-NUM-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-31 08:32:17', '', '0', '0', '0');
INSERT INTO `xxl_job_info` VALUES ('31', '3', '0 0/5 * * * ?', 'top_website_urls_daily_log', '2020-03-31 10:39:09', '2020-04-13 05:16:57', 'ch', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"select url,APPROX_COUNT_DISTINCT_DS_HLL(ip_object) as ip_num,session_num,max(content_length) as content_length,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time,APPROX_COUNT_DISTINCT_DS_HLL(ip_object)*session_num as order_num from website_urls_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1*12*24) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') group by url,session_num order by order_num desc limit 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-WEBSITE-URLS-DAILY-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-03-31 10:39:09', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('32', '3', '0 0/5 * * * ?', 'policy_id_ip_num_log', '2020-04-02 12:17:30', '2020-04-16 02:24:31', 'ch', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"select policy_id,APPROX_COUNT_DISTINCT_DS_HLL(ip_object) as ip_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time from proxy_event_hits_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1*12*1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') group by policy_id\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"POLICY-ID-IP-NUM-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-04-02 12:17:30', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('34', '3', '0 0/5 * * * ?', 'Proxy-Event-Top-URLS-By-Hits', '2020-04-07 21:14:31', '2020-04-13 05:01:51', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT url ,sum( session_num ) AS session_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time FROM proxy_event_urls_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY url ORDER BY session_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-URLS-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-04-07 21:14:31', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('35', '3', '0 0/5 * * * ?', 'Security-Event-Top-URLS-By-Hits', '2020-04-07 21:16:26', '2020-04-13 05:02:34', 'galaxy', '', 'FIRST', 'httpToKafkaJobHandler', '{\"url\":\"http://192.168.40.224:8082/druid/v2/sql\",\"method\":\"post\",\"requestBody\":{\"query\":\"SELECT url ,sum( session_num ) AS session_num,TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') as stat_time FROM security_event_urls_hot_log WHERE __time >=  TIME_SHIFT(TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\'),\'PT5M\',-1) AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,\'PT5M\') GROUP BY url ORDER BY session_num DESC LIMIT 1000\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"},\"resultKey\":\"\",\"topic\":\"TOP-URLS-LOG\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-04-07 21:16:26', '', '1', '1587366000000', '1587366300000');
INSERT INTO `xxl_job_info` VALUES ('36', '3', '0 0/5 * * * ?', '获取所有存储配额-sh', '2020-04-07 21:41:20', '2020-04-15 04:31:28', 'zhq', '', 'FIRST', 'getStorageQuotaJobHandler', '{\"scriptFile\":\"/script/storge/diskUseSum.sh\",\"topic\":\"SYS-STORAGE-LOG\",\"updateScript\":\"false\"}', 'SERIAL_EXECUTION', '0', '0', 'BEAN', '', 'GLUE代码初始化', '2020-04-07 21:41:20', '', '1', '1587366000000', '1587366300000');

-- ----------------------------
-- Table structure for `xxl_job_lock`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_lock`;
CREATE TABLE `xxl_job_lock` (
  `lock_name` varchar(50) NOT NULL COMMENT '锁名称',
  PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_lock
-- ----------------------------
INSERT INTO `xxl_job_lock` VALUES ('schedule_lock');

-- ----------------------------
-- Table structure for `xxl_job_log`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_log`;
CREATE TABLE `xxl_job_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `job_group` int(11) NOT NULL COMMENT '执行器主键ID',
  `job_id` int(11) NOT NULL COMMENT '任务，主键ID',
  `executor_address` varchar(255) DEFAULT NULL COMMENT '执行器地址，本次执行的地址',
  `executor_handler` varchar(255) DEFAULT NULL COMMENT '执行器任务handler',
  `executor_param` varchar(4000) DEFAULT '' COMMENT '执行器任务参数',
  `executor_sharding_param` varchar(20) DEFAULT NULL COMMENT '执行器任务分片参数，格式如 1/2',
  `executor_fail_retry_count` int(11) NOT NULL DEFAULT 0 COMMENT '失败重试次数',
  `trigger_time` datetime DEFAULT NULL COMMENT '调度-时间',
  `trigger_code` int(11) NOT NULL COMMENT '调度-结果',
  `trigger_msg` text DEFAULT NULL COMMENT '调度-日志',
  `handle_time` datetime DEFAULT NULL COMMENT '执行-时间',
  `handle_code` int(11) NOT NULL COMMENT '执行-状态',
  `handle_msg` text DEFAULT NULL COMMENT '执行-日志',
  `alarm_status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '告警状态：0-默认、1-无需告警、2-告警成功、3-告警失败',
  PRIMARY KEY (`id`),
  KEY `I_trigger_time` (`trigger_time`),
  KEY `I_handle_code` (`handle_code`)
) ENGINE=InnoDB AUTO_INCREMENT=420871 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_log
-- ----------------------------

-- ----------------------------
-- Table structure for `xxl_job_logglue`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_logglue`;
CREATE TABLE `xxl_job_logglue` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `job_id` int(11) NOT NULL COMMENT '任务，主键ID',
  `glue_type` varchar(50) DEFAULT NULL COMMENT 'GLUE类型',
  `glue_source` mediumtext DEFAULT NULL COMMENT 'GLUE源代码',
  `glue_remark` varchar(128) NOT NULL COMMENT 'GLUE备注',
  `add_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_logglue
-- ----------------------------

-- ----------------------------
-- Table structure for `xxl_job_log_report`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_log_report`;
CREATE TABLE `xxl_job_log_report` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `trigger_day` datetime DEFAULT NULL COMMENT '调度-时间',
  `running_count` int(11) NOT NULL DEFAULT 0 COMMENT '运行中-日志数量',
  `suc_count` int(11) NOT NULL DEFAULT 0 COMMENT '执行成功-日志数量',
  `fail_count` int(11) NOT NULL DEFAULT 0 COMMENT '执行失败-日志数量',
  PRIMARY KEY (`id`),
  UNIQUE KEY `i_trigger_day` (`trigger_day`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=85 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_log_report
-- ----------------------------


-- ----------------------------
-- Table structure for `xxl_job_registry`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_registry`;
CREATE TABLE `xxl_job_registry` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `registry_group` varchar(50) NOT NULL,
  `registry_key` varchar(255) NOT NULL,
  `registry_value` varchar(255) NOT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `i_g_k_v` (`registry_group`,`registry_key`,`registry_value`)
) ENGINE=InnoDB AUTO_INCREMENT=190 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_registry
-- ----------------------------

-- ----------------------------
-- Table structure for `xxl_job_user`
-- ----------------------------
DROP TABLE IF EXISTS `xxl_job_user`;
CREATE TABLE `xxl_job_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL COMMENT '账号',
  `password` varchar(50) NOT NULL COMMENT '密码',
  `role` tinyint(4) NOT NULL COMMENT '角色：0-普通用户、1-管理员',
  `permission` varchar(255) DEFAULT NULL COMMENT '权限：执行器ID列表，多个逗号分割',
  PRIMARY KEY (`id`),
  UNIQUE KEY `i_username` (`username`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Records of xxl_job_user
-- ----------------------------
INSERT INTO `xxl_job_user` VALUES ('1', 'admin', 'e10adc3949ba59abbe56e057f20f883e', '1', null);
