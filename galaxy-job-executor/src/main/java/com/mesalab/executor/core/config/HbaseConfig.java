package com.mesalab.executor.core.config;

import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.annotation.NacosConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@NacosConfigurationProperties(prefix = "hbase", dataId = "${nacos.config.data-id}", groupId = "${nacos.config.group}", type = ConfigType.PROPERTIES, autoRefreshed = true)
public class HbaseConfig {
    private String zookeeper_quorum;

    private String zookeeper_property_clientPort;

    private String zookeeper_znode_parent;

    private String client_pause;

    private Integer client_retries_number;

    private Integer rpc_timeout;

    private Integer connect_pool;

    private Integer scanner_timeout;

    private Integer scanner_max_result_size;

    private Integer scanner_caching;

    private String time_index_table_prefix;

    private String filename_index_table_prefix;

    private String partfile_index_table_prefix;

    private String system_bucket_meta;

    private String client_keyvalue_maxsize;

    private List<String> filename_head;

    private List<String> part_head;

    private Integer get_batch;

    private Integer delete_batch;

    private Integer scan_batch;

    private Long executeTime;

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public Integer getScan_batch() {
        return scan_batch;
    }

    public void setScan_batch(Integer scan_batch) {
        this.scan_batch = scan_batch;
    }

    public Integer getDelete_batch() {
        return delete_batch;
    }

    public void setDelete_batch(Integer delete_batch) {
        this.delete_batch = delete_batch;
    }

    public String getZookeeper_quorum() {
        return zookeeper_quorum;
    }

    public void setZookeeper_quorum(String zookeeper_quorum) {
        this.zookeeper_quorum = zookeeper_quorum;
    }

    public String getZookeeper_property_clientPort() {
        return zookeeper_property_clientPort;
    }

    public void setZookeeper_property_clientPort(String zookeeper_property_clientPort) {
        this.zookeeper_property_clientPort = zookeeper_property_clientPort;
    }

    public String getZookeeper_znode_parent() {
        return zookeeper_znode_parent;
    }

    public void setZookeeper_znode_parent(String zookeeper_znode_parent) {
        this.zookeeper_znode_parent = zookeeper_znode_parent;
    }

    public String getClient_pause() {
        return client_pause;
    }

    public void setClient_pause(String client_pause) {
        this.client_pause = client_pause;
    }

    public Integer getClient_retries_number() {
        return client_retries_number;
    }

    public void setClient_retries_number(Integer client_retries_number) {
        this.client_retries_number = client_retries_number;
    }

    public Integer getRpc_timeout() {
        return rpc_timeout;
    }

    public void setRpc_timeout(Integer rpc_timeout) {
        this.rpc_timeout = rpc_timeout;
    }

    public Integer getConnect_pool() {
        return connect_pool;
    }

    public void setConnect_pool(Integer connect_pool) {
        this.connect_pool = connect_pool;
    }

    public Integer getScanner_timeout() {
        return scanner_timeout;
    }

    public void setScanner_timeout(Integer scanner_timeout) {
        this.scanner_timeout = scanner_timeout;
    }

    public Integer getScanner_max_result_size() {
        return scanner_max_result_size;
    }

    public void setScanner_max_result_size(Integer scanner_max_result_size) {
        this.scanner_max_result_size = scanner_max_result_size;
    }

    public Integer getScanner_caching() {
        return scanner_caching;
    }

    public void setScanner_caching(Integer scanner_caching) {
        this.scanner_caching = scanner_caching;
    }

    public String getTime_index_table_prefix() {
        return time_index_table_prefix;
    }

    public void setTime_index_table_prefix(String time_index_table_prefix) {
        this.time_index_table_prefix = time_index_table_prefix;
    }

    public String getFilename_index_table_prefix() {
        return filename_index_table_prefix;
    }

    public void setFilename_index_table_prefix(String filename_index_table_prefix) {
        this.filename_index_table_prefix = filename_index_table_prefix;
    }

    public String getPartfile_index_table_prefix() {
        return partfile_index_table_prefix;
    }

    public void setPartfile_index_table_prefix(String partfile_index_table_prefix) {
        this.partfile_index_table_prefix = partfile_index_table_prefix;
    }

    public String getSystem_bucket_meta() {
        return system_bucket_meta;
    }

    public void setSystem_bucket_meta(String system_bucket_meta) {
        this.system_bucket_meta = system_bucket_meta;
    }

    public String getClient_keyvalue_maxsize() {
        return client_keyvalue_maxsize;
    }

    public void setClient_keyvalue_maxsize(String client_keyvalue_maxsize) {
        this.client_keyvalue_maxsize = client_keyvalue_maxsize;
    }

    public List<String> getFilename_head() {
        return filename_head;
    }

    public void setFilename_head(List<String> filename_head) {
        this.filename_head = filename_head;
    }

    public List<String> getPart_head() {
        return part_head;
    }

    public void setPart_head(List<String> part_head) {
        this.part_head = part_head;
    }

    public Integer getGet_batch() {
        return get_batch;
    }

    public void setGet_batch(Integer get_batch) {
        this.get_batch = get_batch;
    }
}
