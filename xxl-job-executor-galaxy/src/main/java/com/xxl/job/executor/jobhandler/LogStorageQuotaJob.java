package com.xxl.job.executor.jobhandler;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.config.StorgeConfig;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.core.utils.XmlUtil;
import com.xxl.job.executor.pojo.LifecycleConfiguration;
import com.xxl.job.executor.pojo.LifecycleConfiguration.Expiration;
import com.xxl.job.executor.pojo.ListAllMyBucketsResult;
import com.xxl.job.executor.pojo.ListAllMyBucketsResult.Buckets;
import com.xxl.job.executor.pojo.ListAllMyBucketsResult.Buckets.Bucket;
import com.xxl.job.executor.service.StorageQuotaService;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.TimeConstants;

@SuppressWarnings({ "unchecked", "rawtypes" })
@Component
public class LogStorageQuotaJob {
	
	private static Logger logger = LoggerFactory.getLogger(LogStorageQuotaJob.class);
	
	private static String trafficServer;
	private static String trafficDataSource;
	private static String trafficUserName;
	private static String trafficPassword;
	
	private static String trafficClusterName;//clickhouse集群名称
	private static String queryClickhouseUrl;
	
	private static String analyticServer;
	private static String selectAnalyticServer;
	private static List<String> trafficTableList;
	private static List<String> analyticTableList;
	private static String queryDruidUrl;
	
	private static String systemParts;
	private static String systemTables;
	
	private static String filesServer;
	private static String filesToken;
	
	// zookeeper /path+node
	private static final String TRAFFIC_LOGS = "Traffic-Logs";
	private static final String REPORT_AND_METRICS = "Report-and-Metrics";
	private static final String FILES = "Files";
	
	@Autowired
	StorgeConfig storgeConfig;
	
	@Autowired
	StorageQuotaService storageQuotaService;

	
	@Autowired
	public LogStorageQuotaJob(StorgeConfig deletionConfig) {
		trafficServer = deletionConfig.getTrafficServer();
		trafficDataSource = deletionConfig.getTrafficDatasource();
		trafficUserName = deletionConfig.getTrafficUsername();
		trafficPassword = deletionConfig.getTrafficPassword();
		trafficClusterName = deletionConfig.getTrafficClusterName();
		systemParts = deletionConfig.getSystemParts();
		systemTables= deletionConfig.getSystemTables();

		analyticServer = deletionConfig.getAnalyticServerDelete();//用于Druid删除
		selectAnalyticServer = deletionConfig.getAnalyticServerSelect();//用于Druid查询
		
		queryDruidUrl = Joiner.on("").join("http://", selectAnalyticServer, "/druid/v2/sql");
		queryClickhouseUrl = Joiner.on("").join("http://", trafficServer);
		
		filesServer = deletionConfig.getFilesServer();
		filesToken = deletionConfig.getFilesToken();
	}
	
	//*********************************************设置存储策略**********************************************************

	
	/**
	 *  设置流量日志存储策略储
	 *  clickhouse 数据库
	 * @param params {"maxdays":30}
	 */
	@XxlJob("deleteTrafficDataJobHandler")
	public ReturnT<String> deleteTrafficData(String params) {
		try {
			Map<String, Object> paramsMap = validParams(params);
			
			Map<String, Object> deleteParamMap = getDeleteSource();
			
			String prefixDeleteSql = Joiner.on("").join("ALTER TABLE ", trafficDataSource,".");
			String suffixDeleteSql = Joiner.on("").join(" ON CLUSTER ",trafficClusterName," DROP PARTITION ");
			Integer maxdays = Integer.valueOf(String.valueOf(paramsMap.get("maxdays")));
			String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -maxdays);
			// 设置一个获取sql的方法，把表List 
			getTrafficTables().forEach(table->{
				
				String selectSql = Joiner.on("").join("SELECT `partition` from "+systemParts+" WHERE table = '", table, "' AND `partition` < '",deleteMaxDate,"' FORMAT JSON;");
				//查询所有小于设定时间的分区
				deleteParamMap.put("query", selectSql);
				String httpGetResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(queryClickhouseUrl, deleteParamMap));
				Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
				List<Map> partitionList = (List) httpGetResultMap.get("data");
				
				partitionList.forEach(partition->{
						String deleteSql = Joiner.on("").join(prefixDeleteSql, table, suffixDeleteSql, partition.get("partition"));
						deleteParamMap.put("query", deleteSql);
						XxlJobLogger.log("reset storage days clickhouse sql:{}", deleteSql);
						
						String deleteMessage = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(queryClickhouseUrl, deleteParamMap), null);
						XxlJobLogger.log("reset storage days clickhouse message:{}", deleteMessage);
						XxlJobLogger.log("reset storage days table {} success", table);
				});
			});
		      modifyLastStorage(TRAFFIC_LOGS, maxdays);
		} catch (BusinessException be) {
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		
		return ReturnT.SUCCESS;
	}
	
	/**
	 * Report and Metrics 日志存储策略
	 * druid 数据库
	 * @param params{"maxdays":365}
	 */
	@XxlJob("deleteReportAndMetricsDataJobHandler")
	public ReturnT<String> deleteReportAndMetricsData(String params) {
		
		try {
			Map<String, Object> paramsMap = validParams(params);
			
			//修改规则
			String changeRulesUrl = Joiner.on("").join("http://", analyticServer, "/druid/coordinator/v1/rules/");
			String changeRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", paramsMap.get("maxdays"), "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");
			//物理删除除
			String deleteUrl = Joiner.on("").join("http://", analyticServer, "/druid/indexer/v1/task");
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			//起始时间/当前时间-maxDay
			String druidDataStartTime = getDruidDataStartTime();
			String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
			String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"", druidDataStartTime,"/", deleteMaxDate, "\"}");
			getAnalyticTables().forEach(table->{
				
				XxlJobLogger.log("reset storage days first command table :{}, sql : {}",table,changeRulesParam);
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				if(druidDataStartTime.compareTo(deleteMaxDate)<0) {
					XxlJobLogger.log("reset storage days second command table :{}, sql : {}",table,prefixDeleteParam+table+suffixDeleteParam);
					String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+table+suffixDeleteParam);
					XxlJobLogger.log("reset storage days result message :{}",deleteMessage);
				}
			});
			modifyLastStorage(REPORT_AND_METRICS, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
		} catch (BusinessException be) {
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		
		return ReturnT.SUCCESS;
	}
	/**
	 * 设置文件存储策略
	 * @param params{"maxdays":30}
	 */
	@XxlJob("deleteFilesJobHandler")
	public ReturnT<String> deleteFiles(String params){
		try {
			Map<String, Object> paramsMap = validParams(params);
			Header header1 = new BasicHeader("Token", filesToken);
			Header header2 = new BasicHeader(HttpHeaders.CONTENT_TYPE, "text/xml");
			Header[] headers = {header1,header2};

			String httpGetRes = HttpClientUtils.httpGet(Joiner.on("").join(filesServer,"/hos/"), header1);
			ListAllMyBucketsResult list = XmlUtil.converXmlToBean(ListAllMyBucketsResult.class, httpGetRes);
			if(ObjectUtils.allNotNull(list,list.getBuckets(),list.getBuckets().getBucket())) {
			Buckets buckets = list.getBuckets();
			List<Bucket> bucketList = buckets.getBucket();
			XxlJobLogger.log("bucket list {} ",bucketList);
			LifecycleConfiguration lc = new LifecycleConfiguration();
			lc.getRule().setExpiration(new Expiration(String.valueOf(paramsMap.get("maxdays"))));
			int failCount = 0;
				for (Bucket bucket : bucketList) {
					try {
						HttpClientUtils.httpPut(Joiner.on("").join(filesServer,"/hos/",bucket.getName(),"?lifecycle"),XmlUtil.converToXml(lc), headers);
					} catch (BusinessException be) {
						logger.error("reset storage days files fail ,{}",be.getErrorMessage());
						XxlJobLogger.log("reset storage days files fail ,{}",be.getErrorMessage());
						failCount++;
					}
					XxlJobLogger.log("reset storage days bucket {} ",bucket);
				}
				if(failCount>0) {
					throw new BusinessException("number of total task "+bucketList.size()+", number of failed tasks "+failCount) ;
				}
			}
			modifyLastStorage(FILES, Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
		} catch (BusinessException be) {
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		return ReturnT.SUCCESS;
	}
	
	//*********************************************清库**********************************************************
	
	/**
	 * 清除所有流量数据,click hosue库
	 * @param params{"maxdays":365}
	 */
	@XxlJob("deleteAllTrafficDataJobHandler")
	public ReturnT<String> deleteAllTrafficData(String params){
		
		try {
			Map<String, Object> deleteParamMap = getDeleteSource();
			//清库命令参数
			String prefixDeleteSql = Joiner.on("").join("TRUNCATE TABLE IF EXISTS ", trafficDataSource, ".");
			String suffixDeleteSql = Joiner.on("").join(" ON CLUSTER ", trafficClusterName);
			getTrafficTables().forEach(table->{
				deleteParamMap.put("query", prefixDeleteSql + table + suffixDeleteSql);
				XxlJobLogger.log("delete clickhouse sql:{}",prefixDeleteSql + table + suffixDeleteSql);
				HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(queryClickhouseUrl, deleteParamMap), null);
				XxlJobLogger.log("delete table {} success",table);
			});
			modifyLastStorage(TRAFFIC_LOGS, 0);
		} catch (BusinessException be) {
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		
		return ReturnT.SUCCESS;
	}
	
	/**
	 * 清除所有report metric数据 druid库
	 * @param params{"maxdays":30}
	 */
	@XxlJob("deleteAllReportAndMetricsDataJobHandler")
	public ReturnT<String> deleteAllReportAndMetricsData(String params) {
		try {
			Map<String, Object> paramMap = validParams(params);
			String deleteEndDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD,1);
			
			String deleteStartTime = getDruidDataStartTime();
			
			//1停止加载
			String changeRulesUrl = Joiner.on("").join("http://", analyticServer, "/druid/coordinator/v1/rules/");
			String changeRulesParam = "[{ \"type\":\"dropByInterval\", \"interval\":\""+deleteStartTime+"/"+deleteEndDate+"\" }]";
			
			//2物理删除所有数据
			String deleteUrl = Joiner.on("").join("http://", analyticServer, "/druid/indexer/v1/task");
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"",deleteStartTime+"/",deleteEndDate,"\"}");
			//3恢复加载规则
			String resetRulesUrl = Joiner.on("").join("http://", analyticServer, "/druid/coordinator/v1/rules/");
			String resetRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", paramMap.get("maxdays"), "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");
			List<String> analyticTables = getAnalyticTables();
			int failCount = 0;
			for( String table: analyticTables){
				//每个表限制操作时间五分钟，开始计时
				long startTime = System.currentTimeMillis();
				long endTime = startTime + 1000*60*5;
				XxlJobLogger.log("delete table first command table :{}, sql : {}",table , changeRulesParam);
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				try {
					//查询表数据是否删除 固定key EXPR$0
					String countTableDataSql = Joiner.on("").join("{\"query\":\"SELECT count(*) from ", table, " where __time >= '", deleteStartTime, "' AND __time <= '", getDruidDataEndTime(), "'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}");
					int count = 1;
					while(count > 0) {
						String httpPostResult = HttpClientUtils.httpPost(queryDruidUrl, countTableDataSql);
						List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
						count = Integer.valueOf(String.valueOf(list.get(0).get("EXPR$0")));
						try {
							if(count>0) {
								//每次停留5秒判断是否删除结束
								Thread.sleep(5000L);
							}
							if(System.currentTimeMillis()>endTime) {
								XxlJobLogger.log("delete time out error, table : {} ",table);
								break;
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				} catch (BusinessException e) {
					// 表不存在时，查询发生错误，记录下继续执行
					logger.error(" query error {}",e.getErrorMessage());
					XxlJobLogger.log(" query error {}",e.getErrorMessage());
					failCount++;
				}
				
				XxlJobLogger.log("delete table second command table :{}, sql : {}",table , prefixDeleteParam+ table +suffixDeleteParam);
				String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+ table +suffixDeleteParam);
				XxlJobLogger.log("delete table third command table :{}, sql : {}",table , resetRulesParam);
				HttpClientUtils.httpPost(resetRulesUrl+table, resetRulesParam);
				
				XxlJobLogger.log("delete druid message :{}",deleteMessage);
			}
			if(failCount>0) {
				throw new BusinessException("number of total tasks "+analyticTables.size()+" , number of failed tasks "+failCount) ;
			}
			modifyLastStorage(REPORT_AND_METRICS, 0);
		} catch (BusinessException be) {
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		
		return ReturnT.SUCCESS;
	}
	/**
	 * 清除文件数据hbase库
	 * @param params{"maxdays":365}
	 */
	@XxlJob("deleteAllFilesJobHandler")
	public ReturnT<String> deleteAllFiles(String params){
		
		try {
			Header header = new BasicHeader("Token", filesToken);

			String httpGetRes = HttpClientUtils.httpGet(Joiner.on("").join(filesServer,"/hos/"), header);
			ListAllMyBucketsResult list = XmlUtil.converXmlToBean(ListAllMyBucketsResult.class, httpGetRes);
			if(ObjectUtils.allNotNull(list,list.getBuckets(),list.getBuckets().getBucket())) {
			Buckets buckets = list.getBuckets();
			List<Bucket> bucketList = buckets.getBucket();
				XxlJobLogger.log("bucket list {} ",bucketList);
				int failCount=0;
				for (Bucket bucket : bucketList) {
					try {
						HttpClientUtils.httpDelete(Joiner.on("").join(filesServer,"/hos/",bucket.getName(),"?truncate"), header);
					} catch (BusinessException be) {
						logger.error(be.getErrorMessage());
						XxlJobLogger.log(be.getErrorMessage());
						
						failCount++;
					}
					XxlJobLogger.log("delete bucket {} ",bucket);
				}
				if(failCount>0) {
					throw new BusinessException("number of total tasks "+bucketList.size()+" , number of failed tasks "+failCount) ;
				}
			}
			modifyLastStorage(FILES, 0);
		} catch (BusinessException be) {
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		return ReturnT.SUCCESS;
	}
	

	/**
	 * 直接查询数据库获取参数
	 * @param params 
	 * {"topic":"SYS-STORAGE-LOG","ckDayGrowth":"false"}
	 * ckDayGrowth:表示是否为一天的执行一次的时间粒度
	 */
	@SuppressWarnings("null")
	@XxlJob("getStorageQuotaJobHandler")
	public ReturnT<String> getStorageQuota (String params) {
		
	        try {
	        	Map<String, Object> paramsMap = validParams(params);
				//调用查询方法单机，集群
	        	Map resultMap = null;
	        	
				if ("false".equals(paramsMap.get("ckDayGrowth"))) {
					resultMap = storageQuotaService.getDiskJson();
				} else {
					resultMap = storageQuotaService.getDayJson();
				}
	        	
	            //发送到kafka
	            new KafkaUtils().sendMessage(String.valueOf(paramsMap.get("topic")), (List<Object>) resultMap.get("data"));
	            XxlJobLogger.log("send message to kafka, topic is {}", paramsMap.get("topic"));
	            //查询存在问题会返回fail
	            if(resultMap.toString().contains("fail")) {
	            	return new ReturnT<String>(IJobHandler.FAIL.getCode(), " query error "+resultMap.get("status"));
	        	}
	            
	        } catch (BusinessException be) {
				logger.error(be.getErrorMessage());
				XxlJobLogger.log(be.getErrorMessage());
				return ReturnT.FAIL;
			} catch (Exception e) {
	        	logger.error(e.getMessage());
	            XxlJobLogger.log(e);
	            return ReturnT.FAIL;
	        }

	        return ReturnT.SUCCESS;
	}
	
	/**
	 *  数据库连接参数
	 * @return map
	 * {database：,
	 * 	password：,
	 * 	user:,
	 * }
	 */
	private Map<String, Object> getDeleteSource() {
		//删除sql参数拼接
		Map<String, Object> deleteParamMap = Maps.newHashMap();
		deleteParamMap.put("database", trafficDataSource);
		deleteParamMap.put("password", trafficPassword);
		deleteParamMap.put("user", trafficUserName);
		return deleteParamMap;
	}
	/**
	 * 必填参数进行验证，其它自行配置
	 * @param params
	 */
	private Map<String, Object> validParams(String params) {
		logger.info("params{}",params);

		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is Empty !");
			return null;
		}
		Map<String,Object> paramMap = (Map) JsonMapper.fromJsonString(params, Map.class);
		if(paramMap==null) {
			XxlJobLogger.log("params error !");
			return null;
		}
		return paramMap;
	}
	/**
	 * 根据druid日志表命名规则查询以log结尾的表
	 * @return List<String>
	 */
	private List<String> getAnalyticTables() {
		//查询特定的表名
		if(analyticTableList==null) {
			List<String> tableList = Lists.newArrayList();
			String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks WHERE datasource NOT LIKE '%hot%'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
			String httpPostResult = HttpClientUtils.httpPost(queryDruidUrl, queryTablesSql);
			
			List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
			list.forEach(map->tableList.addAll(map.values()));
			analyticTableList = tableList;
		}
		return analyticTableList;
	}
	/**
	 * 查询clickhouse 需要删除的表
	 * @return List<String>
	 */
	private List<String> getTrafficTables() {
		//查询特定的表名
		List<String> tableList = Lists.newArrayList();
		if(trafficTableList==null) {
			Map<String, Object> deleteParamMap = getDeleteSource();
			
			deleteParamMap.put("query", Joiner.on("").join("SELECT name FROM "+systemTables+" WHERE database = '", trafficDataSource, "' AND engine in  ('MergeTree','ReplicatedMergeTree') FORMAT JSON;"));
			String httpGetResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(queryClickhouseUrl, deleteParamMap));
			
			Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
			List<Map> list = (List) httpGetResultMap.get("data");
			list.forEach(map->tableList.addAll(map.values()));
			trafficTableList = tableList;
		}
		return trafficTableList;
	}
	
	/**
	 * 查询Druid数据的开始时间,为空时返回今天，格式yyyy-MM-dd
	 */
	private String getDruidDataStartTime() {
		//查询druid数据库的数据的开始时间
		String queryAnalyticStartTimeSql = "{\"query\":\"SELECT \\\"start\\\" FROM sys.segments WHERE \\\"start\\\" <= '"+DateUtils.getSomeDate(DateUtils.YYYY_MM_DD, 1)+"' order by \\\"start\\\" limit 1\"}";//durid 清库的起始时间
		String httpPost = HttpClientUtils.httpPost(queryDruidUrl, queryAnalyticStartTimeSql);
		//查询表数据url
		List<Map> startTimeList = (List) JsonMapper.fromJsonString(httpPost, List.class);
		return ObjectUtils.isEmpty(startTimeList)?DateUtils.getCurrentDate():DateUtils.getFormatDate(DateUtils.convertStringToDate(String.valueOf(startTimeList.get(0).get("start")), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"), DateUtils.YYYY_MM_DD);
	}
	/**
	 * 更新标准时间
	 * @param logType
	 * @param maxdays
	 */
	private void modifyLastStorage(String logType, Integer maxdays) {
		// 设置标准时间,如果now - 标准时间 > maxdays,则更新 标准时间 = 当前时间-maxday
		Long lastStorage = storageQuotaService.getLastStorage(logType);
		Long now = System.currentTimeMillis() / 1000;
		Long max = maxdays * 24 * 60 * 60L;// 单位秒
		if (now - lastStorage > max) {
			storageQuotaService.modifyLastStorage(logType, String.valueOf(now - max));
		}
	}
	private String getDruidDataEndTime() {
		//查询druid数据库的数据的结束时间
		String queryAnalyticStartTimeSql = "{\"query\":\"SELECT version FROM sys.segments WHERE version LIKE '2%' ORDER BY version DESC LIMIT 1\"}";//durid 清库的起始时间
		String httpPost = HttpClientUtils.httpPost(queryDruidUrl, queryAnalyticStartTimeSql);
		//查询表数据url
		List<Map> startTimeList = (List) JsonMapper.fromJsonString(httpPost, List.class);
		return ObjectUtils.isEmpty(startTimeList)?DateUtils.getCurrentDate(): String.valueOf(startTimeList.get(0).get("version"));
	}
}
