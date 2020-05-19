package com.xxl.job.executor.service.jobhandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.config.DeletionConfig;
import com.xxl.job.executor.core.config.StorgeConfig;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.zdjizhi.file.FileApplication;
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
	
	private static String clusterName = "ck_cluster";//clickhouse集群名称
	
	private static String analyticServer;
	private static String selectAnalyticServer;
	private static List<String> trafficTableList;
	private static List<String> analyticTableList;
	
	private static final String DELETE_START_TIME = "2000-01-01";//durid 清库的起始时间
	private static final String CHARTSET_UTF8 = "UTF-8";//调用Linux命令返回日志编码
	

	@Autowired
	public LogStorageQuotaJob(DeletionConfig deletionConfig) {
		trafficServer = deletionConfig.trafficServer;
		trafficDataSource = deletionConfig.trafficDataSource;
		trafficUserName = deletionConfig.trafficUserName;
		trafficPassword = deletionConfig.trafficPassword;
		clusterName = deletionConfig.clusterName;

		analyticServer = deletionConfig.analyticServer;
		selectAnalyticServer = deletionConfig.selectAnalyticServer;//用于查询
	}
	/**
	   *    设置流量日志存储策略储
	 *  clickhouse 数据库
	 * @param params {"maxdays":30}
	 */
	@XxlJob("deleteTrafficDataJobHandler")
	public ReturnT<String> deleteTrafficData(String params) {
		try {
			Map<String, Object> paramsMap = validParams(params);
			
			Map<String, Object> deleteParamMap = getDeleteSource();
			
			String url = Joiner.on("").join("http://", trafficServer);
			String prefixDeleteSql = Joiner.on("").join("ALTER TABLE ", trafficDataSource,".");
			String suffixDeleteSql = Joiner.on("").join(" ON CLUSTER ",clusterName," DROP PARTITION ");
			int deleteMaxDate = Integer.valueOf(DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -Integer.valueOf(String.valueOf(paramsMap.get("maxdays")))));
			// 设置一个获取sql的方法，把表List 
			getTrafficTables().forEach(table->{
				
				String selectSql = Joiner.on("").join("SELECT `partition` from `system`.parts WHERE table = '", table, "' AND `partition` < '",deleteMaxDate,"' FORMAT JSON;");
				//查询所有小于设定时间的分区
				deleteParamMap.put("query", selectSql);
				String httpGetResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(url, deleteParamMap));
				Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
				List<Map> partitionList = (List) httpGetResultMap.get("data");
				
				partitionList.forEach(partition->{
						String deleteSql = Joiner.on("").join(prefixDeleteSql, table, suffixDeleteSql, partition.get("partition"));
						deleteParamMap.put("query", deleteSql);
						XxlJobLogger.log("delete clickhouse sql:{}", deleteSql);
						
						String deleteMessage = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), null);
						XxlJobLogger.log("delete clickhouse message:{}", deleteMessage);
						XxlJobLogger.log("delete table {} success", table);
				});
			});
		      
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
			String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -Integer.valueOf(String.valueOf(paramsMap.get("maxdays"))));
			String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"", DELETE_START_TIME,"/", deleteMaxDate, "\"}");
			getAnalyticTables().forEach(table->{
				
				XxlJobLogger.log("delete first command table :{}, sql : {}",table,changeRulesParam);
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				
				XxlJobLogger.log("delete second command table :{}, sql : {}",table,prefixDeleteParam+table+suffixDeleteParam);
				String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+table+suffixDeleteParam);
				XxlJobLogger.log("delete result message :{}",deleteMessage);
			});
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
	 * 清除所有流量数据,click hosue库
	 * @param params{"maxdays":365}
	 */
	@XxlJob("deleteAllTrafficDataJobHandler")
	public ReturnT<String> deleteAllTrafficData(String params){
		
		try {
			Map<String, Object> deleteParamMap = getDeleteSource();
			String url = Joiner.on("").join("http://", trafficServer);
			//清库命令参数
			String prefixDeleteSql = Joiner.on("").join("TRUNCATE TABLE IF EXISTS ", trafficDataSource, ".");
			String suffixDeleteSql = Joiner.on("").join(" ON CLUSTER ", clusterName);
			getTrafficTables().forEach(table->{
				deleteParamMap.put("query", prefixDeleteSql + table + suffixDeleteSql);
				XxlJobLogger.log("delete clickhouse sql:{}",prefixDeleteSql + table + suffixDeleteSql);
				HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), null);
				XxlJobLogger.log("delete table {} success",table);
			});
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
			
			//停止加载
			String changeRulesUrl = Joiner.on("").join("http://", analyticServer, "/druid/coordinator/v1/rules/");
			String changeRulesParam = "[{\"type\":\"dropForever\"}]";
			
			//物理删除所有数据
			String deleteUrl = Joiner.on("").join("http://", analyticServer, "/druid/indexer/v1/task");
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			String suffixDeleteParam = Joiner.on("").join("\",\"interval\":\"",DELETE_START_TIME+"/",DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD,1),"\"}");
			//恢复加载规则
			String resetRulesUrl = Joiner.on("").join("http://", analyticServer, "/druid/coordinator/v1/rules/");
			String resetRulesParam = Joiner.on("").join("[{\"type\":\"loadByPeriod\",\"period\":\"P", paramMap.get("maxdays"), "D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]");
			//查询表数据url
			String countDataUrl = Joiner.on("").join("http://", selectAnalyticServer, "/druid/v2/sql");
			getAnalyticTables().forEach(table->{
				//每个表限制操作时间五分钟，开始计时
				long startTime = System.currentTimeMillis();
				long endTime = startTime + 1000*60*5;
				XxlJobLogger.log("delete table first command table :{}, sql : {}",table , changeRulesParam);
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				try {
					//查询表数据是否删除 固定key EXPR$0
					String countTableDataSql = Joiner.on("").join("{\"query\":\"SELECT count(*) from ", table, " where __time > '", DELETE_START_TIME, "' AND __time < '", DateUtils.getCurrentDate(), "'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}");
					int count = 1;
					while(count > 0) {
						String httpPostResult = HttpClientUtils.httpPost(countDataUrl, countTableDataSql);
						List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
						count = Integer.valueOf(String.valueOf(list.get(0).get("EXPR$0")));
						try {
							if(count>0) {
								//每次停留两秒判断是否删除结束
								Thread.sleep(2000L);
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
					e.printStackTrace();
					logger.error(" query error {}",e.getErrorMessage());
					XxlJobLogger.log(" query error {}",e.getErrorMessage());
				}
				
				XxlJobLogger.log("delete table second command table :{}, sql : {}",table , prefixDeleteParam+ table +suffixDeleteParam);
				String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+ table +suffixDeleteParam);
				XxlJobLogger.log("delete table third command table :{}, sql : {}",table , resetRulesParam);
				HttpClientUtils.httpPost(resetRulesUrl+table, resetRulesParam);
				
				XxlJobLogger.log("delete druid message :{}",deleteMessage);
			});
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
	 * 本地服务器调用脚本实现
	 * @param params 
	 * {"topic":"SYS-STORAGE-LOG",//脚本推送到kafka的topic
	 * "scriptFile":"/scritp/storge/xxx.sh",//调用的脚本
	 * "updateScript":"false"} //更新脚本参数
	 */
	@XxlJob("getStorageQuotaJobHandler")
	public ReturnT<String> getStorageQuota (String params) {
		
			int exitValue = -1;//表示命令执行结果：0 成功，其它值失败
	        try {
	        	Map<String, Object> paramsMap = validParams(params);
				//脚本路径+文件名
				String scriptFile =new File("").getCanonicalPath() + paramsMap.get("scriptFile");
				if(!FileApplication.isExist(scriptFile)) {
					XxlJobLogger.log("script file does not exist !");
					return ReturnT.FAIL;
				}
				//脚本文件目录
				String scriptDirectory = new File(scriptFile).getParent()+File.separator;
				
				Process catcommands = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",StorgeConfig.getDeployScriptCommand(scriptDirectory)});
				String deplopMessage = getProcessMessage(catcommands.getInputStream(), CHARTSET_UTF8);
				XxlJobLogger.log("script deploy {} ",deplopMessage);
				catcommands.waitFor();
				exitValue = catcommands.exitValue();
				if (exitValue != 0) {
		            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "deploy script file command exit value("+exitValue+") is failed");
		        }
				//更新脚本参数，并分配权限
				if("true".equals(String.valueOf(paramsMap.get("updateScript")))||"no".equals(deplopMessage)) {
					//开通权限,修改配置文件
					String[] commands  = new String[]{"/bin/sh","-c",StorgeConfig.getStorgeConfigCommand(scriptDirectory)};
					Process chmodProcess = Runtime.getRuntime().exec(commands);
					XxlJobLogger.log("update parameters and chmod command ! ");
					
					chmodProcess.waitFor();
					exitValue = chmodProcess.exitValue();
					if (exitValue != 0) {
			            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "update script file command exit value("+exitValue+") is failed");
			        }
				}
	        	
	            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",scriptFile});
	            String execResult = getProcessMessage(process.getInputStream(), CHARTSET_UTF8);
	            XxlJobLogger.log(execResult);
	            // command exit
	            process.waitFor();
	            exitValue = process.exitValue();
	            
	            Map resultMap = (Map) JsonMapper.fromJsonString(execResult, Map.class);
	            if(StringUtil.isBlank(execResult) || resultMap==null) {
					XxlJobLogger.log("script run error !");
					return ReturnT.FAIL;
				}
	            //发送到kafka
	            new KafkaUtils().sendMessage(String.valueOf(paramsMap.get("topic")), (List<Object>) resultMap.get("data"));
	            XxlJobLogger.log("send message to kafka, topic is {}", paramsMap.get("topic"));
	            //脚本查询存在问题会返回fail
	            if(execResult.contains("fail")) {
	            	return new ReturnT<String>(IJobHandler.FAIL.getCode(), "script query error "+resultMap.get("status"));
	        	}
	        } catch (BusinessException be) {
				logger.error("script run error：{}", be.getErrorMessage());
				XxlJobLogger.log(be.getErrorMessage());
				return IJobHandler.FAIL;
			} catch (Exception e) {
	        	logger.error("script run error：{}", e.getMessage());
	            XxlJobLogger.log(e);
	            return IJobHandler.FAIL;
	        }

	        if (exitValue == 0) {
	            return IJobHandler.SUCCESS;
	        } else {
	            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "command exit value("+exitValue+") is failed");
	        }
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
	 * 解析脚本返回结果
	 * @param in
	 * @param charset
	 */
	private String getProcessMessage(InputStream in, String charset) {
		if(in!=null) {
			BufferedReader br = null;
			StringBuffer buffer = new StringBuffer();
			try {
				br = new BufferedReader(new InputStreamReader(in, StringUtil.isBlank(charset)? CHARTSET_UTF8:charset));
				String line = null;
				while ((line = br.readLine()) != null) {
					buffer.append(line);
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
	            if (br != null) {
	            	try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
	            }
	        }
			return buffer.toString();
		}else {
			return "";
		}
	}
	/**
	 * 根据druid日志表命名规则查询以log结尾的表
	 * @return List<String>
	 */
	private List<String> getAnalyticTables() {
		//查询特定的表名
		List<String> tableList = Lists.newArrayList();
		if(analyticTableList==null) {
			String  queryTablesUrl = Joiner.on("").join("http://", selectAnalyticServer, "/druid/v2/sql");
			String queryTablesSql = "{\"query\":\"SELECT DISTINCT(datasource) FROM sys.tasks WHERE datasource NOT LIKE '%hot%'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
			String httpPostResult = HttpClientUtils.httpPost(queryTablesUrl, queryTablesSql);
			
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
			String url = Joiner.on("").join("http://", trafficServer);
			
			deleteParamMap.put("query", Joiner.on("").join("SELECT name FROM `system`.tables WHERE database = '", trafficDataSource, "' AND engine in  ('MergeTree','ReplicatedMergeTree') FORMAT JSON;"));
			String httpGetResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(url, deleteParamMap));
			
			Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
			List<Map> list = (List) httpGetResultMap.get("data");
			list.forEach(map->tableList.addAll(map.values()));
			trafficTableList = tableList;
		}
		return trafficTableList;
	}
}
