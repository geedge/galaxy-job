package com.mesalab.executor.core.utils;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.hutool.core.date.DateUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.core.config.HbaseConfig;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HbaseUtil {

    private Log logger = Log.get();

    @Autowired
    private HbaseConfig hbaseConfig;

    @Bean(name = "hbaseConfiguration")
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseConfig.getZookeeper_quorum());
        conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZookeeper_property_clientPort());
        conf.set("zookeeper.znode.parent", hbaseConfig.getZookeeper_znode_parent());
        conf.setInt("hbase.client.retries.number", hbaseConfig.getClient_retries_number());
        conf.setInt("hbase.rpc.timeout", hbaseConfig.getRpc_timeout());
        conf.set("hbase.client.keyvalue.maxsize", hbaseConfig.getClient_keyvalue_maxsize());
        conf.setInt("hbase.client.ipc.pool.size", hbaseConfig.getConnect_pool());
        conf.setInt("hbase.client.scanner.timeout.period", hbaseConfig.getScanner_timeout());
        conf.setInt("hbase.client.scanner.max.result.size", hbaseConfig.getScanner_max_result_size());
//        conf.setInt("hbase.client.scanner.caching", hbaseConfig.getScanner_caching());
        return conf;
    }

    @Bean(name = "hbaseConnection")
    public Connection getConnection(@Qualifier("hbaseConfiguration") org.apache.hadoop.conf.Configuration conf) {
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.error("create hbase connection error.", e);
        }
        return conn;
    }

    @Bean(name = "threadPool")
    public ExecutorService getThreadPool() {
        return Executors.newFixedThreadPool(hbaseConfig.getDelete_file_thread());
    }


}
