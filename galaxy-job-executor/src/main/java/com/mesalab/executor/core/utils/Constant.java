package com.mesalab.executor.core.utils;

public class Constant {

    // 存储配额中区分数据类型的字段
    public static final String FILES = "Files";
    public static final String REPORT_AND_METRICS = "Report and Metrics";
    public static final String TRAFFIC_LOGS = "Traffic Logs";
    public static final String METRICS = "Metrics";

    // zookeeper /path+node
    public static final String ZK_TRAFFIC_LOGS = "Traffic-Logs";
    public static final String ZK_REPORT_AND_METRICS = "Report-and-Metrics";
    public static final String ZOOKEEPER_STORAGE_PATH = "/storage/worker/";

    public static final String TOKEN = "Token";
    public static final String TEXT_XML = "text/xml";
    public static final String UTF_8 = "UTF-8";

    public static final Integer DEFAULT_VSYS_ID_VALUE = 0;

    public static final String VSYS_ID = "vsys_id";

    public static final String MARIADB = "mariadb";
    public static final String MYSQL = "mysql";
    public static final String FILE = "file";
    public static final String SPIDER = "spider";
    public static final String HTTP = "http";
    public static final String HOS = "hos";
    public static final String WORK_PATH = "/opt/galaxy-job-executor/data/";
    public static final String ZK_KNOWLEDGE_PATH = "/knowledgeBase/metadata/ids/";
    public static String KNOWLEDGE_WORK_DIR = "/opt/schedule-service/mmdb";
    public static final Integer DB_BATCH = 200000;

    public static String ENCRYPT_KEY = "86cf0e2ffba3f541a6c6761313e5cc7e";


}
