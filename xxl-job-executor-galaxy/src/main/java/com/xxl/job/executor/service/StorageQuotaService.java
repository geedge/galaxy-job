package com.xxl.job.executor.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.config.StorgeConfig;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.ZookeeperUtils;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

import lombok.Data;

/**
 * 存储配额获取指标类 组合 原始日志结果对象,统计日志结果对象,文件系统结果对象
 *
 * @author qidaijie
 */
@SuppressWarnings("unchecked")
@Service
public class StorageQuotaService {

	private static Logger logger = LoggerFactory.getLogger(StorageQuotaService.class);
	/**
	 * 调用Linux命令返回日志编码
	 */
	private static final String CHART_SET_UTF8 = "UTF-8";
	/**
	 * HBase 单机数据目录在linux上的位置
	 */
	private static String hbaselinuxDir;
	/**
	 * HBase 在HDFS上的路径
	 */
	private static String hbaseHdfsDir;
	/**
	 * Druid rest地址
	 */
	private static String queryDruidUrl;
	/**
	 * ClickHouse rest地址
	 */
	private static String queryClickHouseUrl;

	private static String database;
	/**
	 * HBase 所在HDFS集群地址
	 */
	private static String queryHBaseUrl;

	private static String trafficUserName;
	private static String trafficPassword;

	private static String systemParts;
	private static String systemDisks;
	private static boolean isCluster;
	private  String zookeeperStoragePath = "/storage/worker/";
	
	@Value("${zookeeper.server}")
	private String zookeeperServer;

	@Autowired
	public StorageQuotaService(StorgeConfig config) {
		database = config.getTrafficDatasource();
		trafficUserName = config.getTrafficUsername();
		trafficPassword = config.getTrafficPassword();
		queryClickHouseUrl = Joiner.on("").join("http://", config.getTrafficServer());
		systemParts = config.getSystemParts();
		systemDisks = config.getSystemDisks();

		queryDruidUrl = Joiner.on("").join("http://", config.getAnalyticServerSelect(), "/druid/v2/sql");

		queryHBaseUrl = Joiner.on("").join("http://", config.getFilesServer());
		hbaselinuxDir = config.getHbaseLinuxDir();
		hbaseHdfsDir = config.getHbaseHdfsDir();
		isCluster = config.getIsClusder();
	}

	/**
	 * 用于获取 ClickHouse 当前存储大小 若获取失败查询druid内最新的值替补当前值
	 */
	private StorageQuota getClickHouseCurr() {
		Map<String, Object> deleteParamMap = getDeleteSource();
		StorageQuota storageQuota = new StorageQuota();
		String currSql = "SELECT SUM(`bytes_on_disk`) FROM " + systemParts + " WHERE database = '" + database + "';";
		try {
			deleteParamMap.put("query", currSql);
			String currResult = HttpClientUtils
					.httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
			Long result = Long.valueOf(currResult.trim());

			storageQuota.setData(ImmutableMap.of("used_size", result));
			storageQuota.setStatus(ImmutableMap.of("traffic_current", "success"));
			logger.info("查询 clickhouse used_size 成功, {}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			Long trafficLogs = getCurrBefore("Traffic Logs");
			storageQuota.setData(ImmutableMap.of("used_size", trafficLogs));
			storageQuota.setStatus(ImmutableMap.of("traffic_current", "fail "+e.getMessage()));
			logger.error("查询 clickhouse used_size 失败, {}", e.getMessage());
			return storageQuota;
		}

	}

	/**
	 * 用于获取ClickHouse最大存储大小 若获取失败查询druid内最新的值替补当前值
	 */
	private StorageQuota getClickHouseMax() {
		Map<String, Object> deleteParamMap = getDeleteSource();
		StorageQuota storageQuota = new StorageQuota();
		String maxSql = "SELECT SUM(`total_space`) FROM " + systemDisks + ";";
		try {
			deleteParamMap.put("query", maxSql);
			String maxResult = HttpClientUtils
					.httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
			long result = Long.parseLong(maxResult.trim());
			storageQuota.setData(ImmutableMap.of("max_size", result));
			storageQuota.setStatus(ImmutableMap.of("traffic_max", "success"));
			logger.info("查询 clickhouse max_size 成功 ,{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			Long trafficLogs = getMaxBefore("Traffic Logs");
			storageQuota.setData(ImmutableMap.of("max_size", trafficLogs));
			storageQuota.setStatus(ImmutableMap.of("traffic_max", "fail "+e.getMessage()));
			logger.error("查询 clickhouse max_size 失败,{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 用于获取ClickHouse 差值 若获取失败直接写入0
	 */
	private StorageQuota getClickHouseDiff() {
		String date = DateUtils.getDateOfYesterday("yyyyMMdd");
		Map<String, Object> deleteParamMap = getDeleteSource();
		StorageQuota storageQuota = new StorageQuota();
		String diffSql = "SELECT SUM(bytes_on_disk) FROM " + systemParts + " WHERE database = '" + database
				+ "' AND partition = '" + date + "';";
		try {
			deleteParamMap.put("query", diffSql);
			String diffResult = HttpClientUtils
					.httpGet(HttpClientUtils.getUrlWithParams(queryClickHouseUrl, deleteParamMap));
			long result = Long.parseLong(diffResult.trim());
			storageQuota.setData(ImmutableMap.of("aggregate_size", result));
			storageQuota.setStatus(ImmutableMap.of("traffic_aggregate", "success"));
			logger.info("查询 clickhouse traffic_aggregate 成功,{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
			storageQuota.setStatus(ImmutableMap.of("traffic_aggregate", "fail "+e.getMessage()));
			logger.error("查询clickhouse traffic_aggregate 失败,{}", e.getMessage());
			return storageQuota;
		}
	}

//========================Druid==============================

	/**
	 * 获取Druid当前存储大小 若获取失败直接补0，Druid本身无法提供服务无需再查询上次值
	 */
	private StorageQuota getDruidCurr() {
		String currSql = "{\"query\":\"SELECT SUM(curr_size) AS curr_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
		StorageQuota storageQuota = new StorageQuota();
		try {
			String currResult = HttpClientUtils.httpPost(queryDruidUrl, currSql);
			List<Map<String, Object >> list = (List) JsonMapper.fromJsonString(currResult, List.class);
			Long currSize = Long.valueOf(String.valueOf(list.get(0).get("curr_size")));

			storageQuota.setData(ImmutableMap.of("used_size", currSize));
			storageQuota.setStatus(ImmutableMap.of("analytic_current", "success"));
			logger.info("查询 druid used_size 成功,{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			storageQuota.setData(ImmutableMap.of("used_size", 0L));
			storageQuota.setStatus(ImmutableMap.of("analytic_current", "fail "+e.getMessage()));
			logger.error("查询 druid used_size 失败,{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 获取Druid过去30秒增量 若获取失败直接补0，Druid本身无法提供服务无需再查询上次值
	 */
	private StorageQuota getDruidDiff(Long currSize) {
		StorageQuota storageQuota = new StorageQuota();
		try {
			Long before = getCurrBefore("Report and Metrics");
			long diff = getDiffNum(currSize, before);
			storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
			storageQuota.setStatus(ImmutableMap.of("analytic_aggregate", "success"));
			logger.info("查询 druid aggregate_size 成功,{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
			storageQuota.setStatus(ImmutableMap.of("analytic_aggregate", "fail "+e.getMessage()));
			logger.error("查询druid aggregate_size 失败,{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 获取Druid最大存储大小 若获取失败直接补0，Druid本身无法提供服务无需再查询上次值
	 */
	private StorageQuota getDruidMax() {
		String maxSql = "{\"query\":\"SELECT SUM(max_size) AS max_size FROM sys.servers WHERE server_type = 'historical'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
		StorageQuota storageQuota = new StorageQuota();
		try {
			String maxResult = HttpClientUtils.httpPost(queryDruidUrl, maxSql);
			List<Map<String, Long>> list = (List) JsonMapper.fromJsonString(maxResult, List.class);
			Long maxSize = list.get(0).get("max_size");
			storageQuota.setData(ImmutableMap.of("max_size", maxSize));
			storageQuota.setStatus(ImmutableMap.of("analytic_max", "success"));
			logger.info("查询 druid max_size 成功，{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			storageQuota.setData(ImmutableMap.of("max_size", 0L));
			storageQuota.setStatus(ImmutableMap.of("analytic_max", "fail "+e.getMessage()));
			logger.error("查询 druid max_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

//======================================================

	/**
	 * 获取HBase 单机当前存储大小 若获取失败查询druid内最新的值替补当前值
	 */
	private StorageQuota getHBaseAloneCurr() {
		StorageQuota storageQuota = new StorageQuota();
		try {
			Process catCommands = Runtime.getRuntime()
					.exec(new String[] { "/bin/sh", "-c", "du -s " + hbaselinuxDir + " | awk '{print $1}'" });
			String message = getProcessMessage(catCommands.getInputStream(), CHART_SET_UTF8);
			logger.info("查询 hbase alone used_size 命令行结果，{}", message);
			catCommands.waitFor();
			
			Long result = Long.parseLong(message.trim())*1024L;
			storageQuota.setData(ImmutableMap.of("used_size", result));
			storageQuota.setStatus(ImmutableMap.of("files_current", "success"));
			logger.info("查询 hbase alone used_size 成功，{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			Long trafficLogs = getCurrBefore("Files");
			storageQuota.setData(ImmutableMap.of("used_size", trafficLogs));
			storageQuota.setStatus(ImmutableMap.of("files_current", "fail "+e.getMessage()));
			logger.error("查询 hbase alone used_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 获取HBase 单机最大存储大小 若获取失败查询druid内最新的值替补当前值
	 */
	private StorageQuota getHBaseAloneMax() {
		StorageQuota storageQuota = new StorageQuota();
		try {
			Process catCommands = Runtime.getRuntime().exec(
					new String[] { "/bin/sh", "-c", "df " + hbaselinuxDir + " | sed -n '2,2p' | awk '{print $2}'" });
			String message = getProcessMessage(catCommands.getInputStream(), CHART_SET_UTF8);
			logger.info("查询 hbase alone max_size 命令行结果，{}", message);
			catCommands.waitFor();
			
			Long result = Long.parseLong(message.trim()) * 1024L;
			storageQuota.setData(ImmutableMap.of("max_size", result));
			storageQuota.setStatus(ImmutableMap.of("files_max", "success"));
			logger.info("查询 hbase max_size alone  max_size 成功，{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			Long trafficLogs = getMaxBefore("Files");
			storageQuota.setData(ImmutableMap.of("max_size", trafficLogs));
			storageQuota.setStatus(ImmutableMap.of("files_max", "fail "+e.getMessage()));
			logger.error("查询 hbase alone max_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 获取HBase 单机存储差值 若获取失败直接补0
	 */
	private StorageQuota getHBaseAloneDiff() {
		StorageQuota storageQuota = new StorageQuota();
		try {
			Process catCommands = Runtime.getRuntime()
					.exec(new String[] { "/bin/sh", "-c", "du -s " + hbaselinuxDir + " | awk '{print $1}'" });
			String message = getProcessMessage(catCommands.getInputStream(), CHART_SET_UTF8);
			logger.info("查询 hbase alone aggregate_size 命令行结果，{}", message);
			catCommands.waitFor();
			
			long result = Long.parseLong(message.trim())*1024L;
			Long trafficLogs = getCurrBefore("Files");
			long diff = getDiffNum(result, trafficLogs);
			storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
			storageQuota.setStatus(ImmutableMap.of("files_aggregate", "success"));
			logger.info("查询 hbase alone aggregate_size 成功，{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
			storageQuota.setStatus(ImmutableMap.of("files_aggregate", "fail "+e.getMessage()));
			logger.error("查询 hbase alone aggregate_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

//=========================HBase集群模式=============================

	/**
	 * 获取HBase 集群当前存储大小 若获取失败查询druid内最新的值替补当前值
	 */
	private StorageQuota getHBaseClusterCurr() {
		StorageQuota storageQuota = new StorageQuota();
		try {
			String currDir = queryHBaseUrl + "/webhdfs/v1" + hbaseHdfsDir + "?op=GETCONTENTSUMMARY";
			String diffResult = HttpClientUtils.httpGet(currDir);
			Map map = (Map) JsonMapper.fromJsonString(diffResult, Map.class);
			Map contentSummary = (Map) map.get("ContentSummary");
			Long length = Long.valueOf(String.valueOf(contentSummary.get("length")));

			storageQuota.setData(ImmutableMap.of("used_size", length));
			storageQuota.setStatus(ImmutableMap.of("files_current", "success"));
			logger.info("查询 hbase cluster used_size 成功，{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			Long trafficLogs = getCurrBefore("Files");
			storageQuota.setData(ImmutableMap.of("used_size", trafficLogs));
			storageQuota.setStatus(ImmutableMap.of("files_current", "fail "+e.getMessage()));
			logger.error("查询 hbase cluster used_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 获取HBase 集群最大存储大小 若获取失败查询druid内最新的值替补当前值
	 */
	private StorageQuota getHBaseClusterMax() {
		StorageQuota storageQuota = new StorageQuota();
		try {
			String currDir = queryHBaseUrl + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";
			String diffResult = HttpClientUtils.httpGet(currDir);
			Map map = (Map) JsonMapper.fromJsonString(diffResult, Map.class);
			List<Map> beans = (List<Map>) map.get("beans");
			Long total = Long.valueOf(String.valueOf(beans.get(0).get("Total")));

			storageQuota.setData(ImmutableMap.of("max_size", total));
			storageQuota.setStatus(ImmutableMap.of("files_max", "success"));
			logger.info("查询 hbase cluster max_size 成功,{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			Long trafficLogs = getMaxBefore("Files");

			storageQuota.setData(ImmutableMap.of("max_size", trafficLogs));
			storageQuota.setStatus(ImmutableMap.of("files_max", "fail "+e.getMessage()));
			logger.error("查询 hbase cluster max_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

	/**
	 * 获取HBase 集群当前存储大小 若获取失败直接补0
	 */
	private StorageQuota getHBaseClusterDiff() {
		StorageQuota storageQuota = new StorageQuota();
		try {
			String currDir = queryHBaseUrl + "/webhdfs/v1" + hbaseHdfsDir + "?op=GETCONTENTSUMMARY ";
			String diffResult = HttpClientUtils.httpGet(currDir);
			Map map = (Map) JsonMapper.fromJsonString(diffResult, Map.class);
			Map contentSummary = (Map) map.get("ContentSummary");
			Long length = Long.valueOf(String.valueOf(contentSummary.get("length")));

			Long trafficLogs = getCurrBefore("Files");
			long diff = getDiffNum(length, trafficLogs);
			storageQuota.setData(ImmutableMap.of("aggregate_size", diff));
			storageQuota.setStatus(ImmutableMap.of("files_aggregate", "success"));
			logger.info("查询 HBase cluster aggregate_size 成功,{}", storageQuota);
			return storageQuota;
		} catch (Exception e) {
			storageQuota.setData(ImmutableMap.of("aggregate_size", 0L));
			storageQuota.setStatus(ImmutableMap.of("files_aggregate", "fail "+e.getMessage()));
			logger.error("查询 hbase cluster aggregate_size 失败，{}", e.getMessage());
			return storageQuota;
		}
	}

//=======================工具类方法===============================

	/**
	 * 用于通过druid获取上次对应类型的Curr值
	 *
	 * @param logType 统计类型
	 * @return 上次的值
	 */
	private Long getCurrBefore(String logType) {
		try {
			String currSql = "{\"query\":\"SELECT used_size FROM sys_storage_log WHERE log_type = '" + logType
					+ "' ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
			String currResult = HttpClientUtils.httpPost(queryDruidUrl, currSql);
			List<Map> list = (List) JsonMapper.fromJsonString(currResult, List.class);
			Long currSize = Long.valueOf(String.valueOf(list.get(0).get("used_size")));
			logger.info("查询 druid 上次 used_size 成功，{}", currSize);
			return currSize;
		} catch (Exception e) {
			logger.error("查询druid上次 used_size 失败，{}", e.getMessage());
			return 0L;
		}
	}

	/**
	 * 用于通过druid获取上次对应类型的Max值
	 *
	 * @param logType 统计类型
	 * @return 上次的值
	 */
	private Long getMaxBefore(String logType) {
		try {
			String maxSql = "{\"query\":\"SELECT max_size FROM sys_storage_log WHERE log_type = '" + logType
					+ "' ORDER BY __time DESC LIMIT 1\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
			String maxResult = HttpClientUtils.httpPost(queryDruidUrl, maxSql);
			List<Map> list = (List<Map>) JsonMapper.fromJsonString(maxResult, List.class);
			Long currSize = Long.valueOf(String.valueOf(list.get(0).get("max_size")));
			logger.info("查询 druid max_size 上次值成功,{}", currSize);
			return currSize;
		} catch (Exception e) {
			logger.error("查询 druid max_size 上次值失败，{}", e.getMessage());
			return 0L;
		}
	}

	/**
	 * @param node
	 * @return 自定义的标准时间 last_storage,
	 * 存在zookeeper 节点/storage/worker/+node
	 */
	public Long getLastStorage(String node) {
		try {
			ZookeeperUtils zk = new ZookeeperUtils();
	    	//查询
			String nodeData = zk.getNodeData(zookeeperStoragePath + node, zookeeperServer);
			//不存在创建一个
			if(ObjectUtils.isEmpty(nodeData)) {
				Long lastTime = System.currentTimeMillis()/1000;
				zk.modifyNode(zookeeperStoragePath + node, String.valueOf(lastTime), zookeeperServer);
				return lastTime;
			}
			Long lastStorage = Long.valueOf(nodeData);
			logger.info("查询标准时间 last_storage成功,{}", lastStorage);
			
			return Long.valueOf(nodeData);
		} catch (Exception e) {
			logger.error("查询标准时间 last_storage 失败,{}",e.getMessage());
			throw new BusinessException("查询标准时间 last_storage 失败 ,"+e.getMessage());
		}

	}
	/**
     * 修改zookeeper节点信息
     *
     * @param path 节点路径/storage/worker/ +node
     */
    public void modifyLastStorage(String node,String data) {
    	ZookeeperUtils zk = new ZookeeperUtils();
    	//是否存在 不存在创建
    	zk.modifyNode(zookeeperStoragePath + node, data, zookeeperServer);
    }
	/**
	 * 数据库连接参数
	 *
	 * @return map {database,password：,user:,}
	 */
	private Map<String, Object> getDeleteSource() {
		// sql参数拼接
		Map<String, Object> deleteParamMap = Maps.newHashMap();
		deleteParamMap.put("password", trafficPassword);
		deleteParamMap.put("user", trafficUserName);
		return deleteParamMap;
	}

	/**
	 * 解析脚本返回结果
	 *
	 * @param input   流
	 * @param charset 编码格式
	 */
	private String getProcessMessage(InputStream in, String charset) {
		if (in != null) {
			BufferedReader br = null;
			StringBuffer buffer = new StringBuffer();
			try {
				br = new BufferedReader(
						new InputStreamReader(in, StringUtil.isBlank(charset) ? CHART_SET_UTF8 : charset));
				String line = null;
				while ((line = br.readLine()) != null) {
					buffer.append(line);
				}
			} catch (IOException e) {
				logger.error("解析返回结果IO错误，{}",e.getMessage());
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						logger.error("解析返回结果IO错误，{}",e.getMessage());
						e.printStackTrace();
					}
				}
			}
			return buffer.toString();
		} else {
			return "";
		}
	}

	/**
	 * 用于组合正常维度的JSON串
	 *
	 * @return 结果json
	 */
	public Map getDiskJson() {
		StorageQuota clickHouseCurr = getClickHouseCurr();
		StorageQuota clickHouseMax = getClickHouseMax();
		Map status = new HashMap<>();
		status.putAll(clickHouseCurr.getStatus());
		status.putAll(clickHouseMax.getStatus());
		Long now = System.currentTimeMillis()/1000;
		
		Map traffic = new HashMap<>();
		traffic.put("log_type", "Traffic Logs");
		traffic.put("time", now);
		traffic.put("last_storage",getLastStorage("Traffic-Logs"));
		traffic.putAll(clickHouseCurr.getData());
		traffic.putAll(clickHouseMax.getData());


		StorageQuota druidCurr = getDruidCurr();
		StorageQuota druidDiff = getDruidDiff(Long.valueOf(String.valueOf(druidCurr.getData().get("used_size"))));
		StorageQuota druidMax = getDruidMax();
		status.putAll(druidCurr.getStatus());
		status.putAll(druidDiff.getStatus());
		status.putAll(druidMax.getStatus());
		
		Map metrics = new HashMap<>();
		metrics.put("log_type", "Report and Metrics");
		metrics.put("time", now);
		metrics.put("last_storage",getLastStorage("Report-and-Metrics"));
		metrics.putAll(druidCurr.getData());
		metrics.putAll(druidDiff.getData());
		metrics.putAll(druidMax.getData());

		

		StorageQuota hBaseCurr = new StorageQuota();
		StorageQuota hBaseMax = new StorageQuota();
		StorageQuota hBaseDiff = new StorageQuota();

		if (isCluster) {
			hBaseCurr = getHBaseClusterCurr();
			hBaseDiff = getHBaseClusterDiff();
			hBaseMax = getHBaseClusterMax();
		} else {
			hBaseCurr = getHBaseAloneCurr();
			hBaseDiff = getHBaseAloneDiff();
			hBaseMax = getHBaseAloneMax();
		}
		status.putAll(hBaseCurr.getStatus());
		status.putAll(hBaseMax.getStatus());
		status.putAll(hBaseDiff.getStatus());

		Map files = new HashMap<>();
		files.put("log_type", "Files");
		files.put("time", now);
		files.put("last_storage",getLastStorage("Files"));
		files.putAll(hBaseCurr.getData());
		files.putAll(hBaseMax.getData());
		files.putAll(hBaseDiff.getData());
		

		Map all = new HashMap<>();
		List data = new ArrayList<>();

		data.add(traffic);
		data.add(metrics);
		data.add(files);

		all.put("status", status);
		all.put("data", data);

		return all;
	}

	/**
	 * 用于组合ClickHouse一天执行一次的JSON串 特殊处理
	 *
	 * @return 结果json
	 */
	public Map getDayJson() {
		StorageQuota clickHouseCurr = getClickHouseCurr();
		StorageQuota clickHouseMax = getClickHouseMax();
		StorageQuota clickHouseDiff = getClickHouseDiff();
		Map status = new HashMap<>();
		status.putAll(clickHouseCurr.getStatus());
		status.putAll(clickHouseMax.getStatus());
		status.putAll(clickHouseDiff.getStatus());

		Map traffic = new HashMap<>();
		traffic.put("log_type", "Traffic Logs");
		traffic.put("time", System.currentTimeMillis()/1000);
		traffic.put("last_storage",getLastStorage("Traffic-Logs"));
		traffic.putAll(clickHouseCurr.getData());
		traffic.putAll(clickHouseMax.getData());
		traffic.putAll(clickHouseDiff.getData());

		Map all = new HashMap<>();
		List data = new ArrayList<>();
		data.add(traffic);

		all.put("status", status);
		all.put("data", data);
		return all;
	}

	/**
	 * 获取差值计算，若为负数则填写0
	 * 
	 * @param now    这一次的值
	 * @param before 上一次的值
	 * @return 差值
	 */
	private Long getDiffNum(Long now, Long before) {
		long diff = now - before;
		if (diff >= 0) {
			return diff;
		} else {
			return 0L;
		}
	}
	 
	@Data
	class StorageQuota {

		private Map<String, Object> data;

		private Map<String, String> status;
	}
}
