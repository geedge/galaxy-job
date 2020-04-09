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
import org.springframework.stereotype.Component;

import com.google.common.base.Splitter;
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
import com.xxl.job.executor.core.utils.JobUtil;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.zdjizhi.file.FileApplication;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.TimeConstants;

@Component
public class LogStorageQuotaJob {
	
	private static Logger logger = LoggerFactory.getLogger(LogStorageQuotaJob.class);
	
	private static String trafficServer;
	private static String trafficDataSource;
	private static String trafficUserName;
	private static String trafficPassword;
	private static String trafficTableNames;
	
	private static String clusterName = "ck_cluster";//clickhouse集群名称
	
	private static String analyticServer;
	private static String selectAnalyticServer;
	private static List<String> trafficTableList;
	private static List<String> analyticTableList;

	static {
		 trafficServer = DeletionConfig.trafficServer;
		 trafficDataSource = DeletionConfig.trafficDataSource;
		 trafficTableNames = DeletionConfig.trafficTableNames;
		 trafficUserName = DeletionConfig.trafficUserName;
		 trafficPassword = DeletionConfig.trafficPassword;
		 clusterName = DeletionConfig.clusterName;
		 
		 analyticServer = DeletionConfig.analyticServer;
		 selectAnalyticServer = DeletionConfig.selectAnalyticServer;//用于查询
		 
		 trafficTableList = Splitter.on(",").trimResults().splitToList(trafficTableNames);
	}
	/**
	  * 设置流量日志存储策略储
	 *  clickhouse 数据库
	 * @param params {"maxdays":30}
	 */
	@XxlJob("deleteTrafficDataJobHandler")
	public ReturnT<String> deleteTrafficDataJobHandler(String params) {
		try {
			Map<String, Integer> paramsMap = validParams(params);
			
			Map<String, Object> deleteParamMap = getDeleteSource();
			
			String url = "http://"+trafficServer;
			String prefixDeleteSql = "ALTER TABLE "+ trafficDataSource +".";
			String suffixDeleteSql =" ON CLUSTER "+clusterName+" DROP PARTITION ";
			int deleteMaxDate = Integer.valueOf(DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -paramsMap.get("maxdays")));
			
			getAnalyticTables().forEach(table->{
				//查询所有小于设定时间的分区
				deleteParamMap.put("query", "SELECT `partition` from `system`.parts WHERE table = '" + table + "' AND `partition` < '"+deleteMaxDate+"' FORMAT JSON;");
				String httpGetResult = HttpClientUtils.httpGet(HttpClientUtils.getUrlWithParams(url, deleteParamMap));
				Map httpGetResultMap = (Map) JsonMapper.fromJsonString(httpGetResult, Map.class);
				List<Map> partitionList = (List) httpGetResultMap.get("data");
				
				partitionList.forEach(partition->{
					if(Integer.valueOf(String.valueOf(partition.get("partition"))) < deleteMaxDate) {
						deleteParamMap.put("query", prefixDeleteSql + table + suffixDeleteSql+partition.get("partition"));
						String deleteMessage = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), null);
						logger.info("delete clickhouse message:{}",deleteMessage);
						XxlJobLogger.log("delete clickhouse message:{}",deleteMessage);
					}
					
				});
			});
		      
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			e.printStackTrace();
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
	public ReturnT<String> deleteReportAndMetricsDataJobHandler(String params) {
		
		try {
			Map<String, Integer> paramsMap = validParams(params);
			
			//修改规则
			String changeRulesUrl ="http://"+ analyticServer +"/druid/coordinator/v1/rules/";
			String changeRulesParam ="[{\"type\":\"loadByPeriod\",\"period\":\"P"+paramsMap.get("maxdays")+"D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]";
			//物理删除除
			String deleteUrl = "http://"+analyticServer+"/druid/indexer/v1/task";
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			//1970-01-01/当前时间-maxDay
			String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -paramsMap.get("maxdays"));
			String suffixDeleteParam = "\",\"interval\":\"1970-01-01/"+deleteMaxDate+"\"}";
			
			getAnalyticTables().forEach(table->{
				
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				
				String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+table+suffixDeleteParam);
				logger.info("delete info :{}",deleteMessage);
				XxlJobLogger.log("delete message :{}",deleteMessage);
			});
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			e.printStackTrace();
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
	public ReturnT<String> deleteAllTrafficDataJobHandler(String params){
		
		try {
			Map<String, Object> deleteParamMap = getDeleteSource();
			String url = "http://"+trafficServer;
			//清库命令参数
			String prefixDeleteSql = "TRUNCATE TABLE IF EXISTS "+ trafficDataSource+".";
			
			String suffixDeleteTtl = " ON CLUSTER " + clusterName;
			
			trafficTableList.forEach(table->{
				deleteParamMap.put("query", prefixDeleteSql+ table +suffixDeleteTtl);
				String deleteMessage = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, deleteParamMap), null);
				logger.info("delete clickhouse message:{}",deleteMessage);
				XxlJobLogger.log("delete clickhouse message:{}",deleteMessage);
			});
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			e.printStackTrace();
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
	public ReturnT<String> deleteAllReportAndMetricsDataJobHandler(String params) {
		try {
			Map<String, Integer> paramMap = validParams(params);
			
			//停止加载
			String changeRulesUrl ="http://"+ analyticServer +"/druid/coordinator/v1/rules/";
			String changeRulesParam = "[{\"type\":\"dropForever\"}]";
			
			//物理删除所有数据
			String deleteUrl ="http://"+ analyticServer +"/druid/indexer/v1/task";
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			String suffixDeleteParam = "\",\"interval\":\"1970-01-01/"+DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD,1)+"\"}";
			
			//恢复加载规则
			String resetRulesUrl ="http://"+ analyticServer +"/druid/coordinator/v1/rules/";
			String resetRulesParam ="[{\"type\":\"loadByPeriod\",\"period\":\"P"+ paramMap.get("maxdays") +"D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]";
			//查询表数据url
			String countDataUrl = "http://"+selectAnalyticServer+"/druid/v2/sql";
			//限制操作时间五分钟，开始计时
			long startTime = System.currentTimeMillis();
			long endTime = startTime + 1000*60*5;
			getAnalyticTables().forEach(table->{
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				
				try {
					//查询表数据是否删除 固定key EXPR$0
					String countTableDataSql ="{\"query\":\"SELECT count(*) from "+table+" where __time < '"+DateUtils.getCurrentDate()+"'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
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
				
				String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+ table +suffixDeleteParam);
				XxlJobLogger.log("delete druid message :{}",deleteMessage);
				HttpClientUtils.httpPost(resetRulesUrl+table, resetRulesParam);
			});
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be.getErrorMessage());
			return ReturnT.FAIL;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		
		return ReturnT.SUCCESS;
	}

	/**
	 * 本地服务器调用脚本实现
	 * @param params 
	 * {"scriptName":"diskUseSum.sh",//调用的脚本
	 * "topic":"SYS-STORAGE-LOG",//脚本推送到kafka的topic
	 * "destDirectory":"/usr/local/storge/",//脚本需要部署的服务器目录
	 * "sourceZipFile":"/config/script/storge.zip"}//源脚本放置的位置
	 */
	@XxlJob("getStorageQuotaJobHandler")
	public ReturnT<String> scriptToKafkaJobHandler (String params) {
		logger.info("remote command job param is {}", params);
		if(StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is empty ! ");
			return ReturnT.FAIL;
		}
		Map<String,String> paramsMap = (Map<String, String>) JsonMapper.fromJsonString(params, Map.class);
		if(paramsMap == null) {
			XxlJobLogger.log("params error ! ");
			return ReturnT.FAIL;
		}
		
	        int exitValue = -1;
	        try {
	        	//脚本解压目录
	        	String destDirectory = paramsMap.get("destDirectory");
				//脚本文件带路径
				String scriptFile = destDirectory + paramsMap.get("scriptName");
				
				if(!FileApplication.isExist(scriptFile)) {
					//源压缩文件
					String sourceFile = new File("").getCanonicalPath() + paramsMap.get("sourceZipFile");
					boolean unZipFiles = FileApplication.unZipFiles(sourceFile, new File(destDirectory).getParent());
					XxlJobLogger.log("unzip file success: {}", unZipFiles);
					//开通权限,修改配置文件
					String[] commands  = new String[]{"/bin/sh","-c",StorgeConfig.getStorgeConfigCommand(destDirectory)};
					Process chmodProcess = Runtime.getRuntime().exec(commands);
					XxlJobLogger.log("chmod command result {}", getProcessMessage(chmodProcess.getInputStream(), "UTF-8"));
					
					chmodProcess.waitFor();
					exitValue = chmodProcess.exitValue();
					if (exitValue != 0) {
			            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "upload script file command exit value("+exitValue+") is failed");
			        }
				}
	        	
	            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",scriptFile});
	            String execResult = getProcessMessage(process.getInputStream(), "UTF-8");
	            // command exit
	            process.waitFor();
	            exitValue = process.exitValue();
	            
	            Map resultMap = (Map) JsonMapper.fromJsonString(execResult, Map.class);
	            if(StringUtil.isBlank(execResult) || resultMap==null) {
					XxlJobLogger.log("Script run error !");
					return ReturnT.FAIL;
				}
	            //发送到kafka
	            new KafkaUtils().sendMessage(paramsMap.get("topic"), (List<Object>) resultMap.get("data"));
	            XxlJobLogger.log("Send message to kafka, topic is {}", paramsMap.get("topic"));
	            //脚本查询存在问题会返回fail
	            if("fail".contains(execResult)) {
	            	return new ReturnT<String>(IJobHandler.FAIL.getCode(), "script query error "+resultMap.get("status"));
	        	}
	        } catch (BusinessException be) {
				logger.error("调用脚本发送到kafka发生错误：{}", be.getErrorMessage());
				XxlJobLogger.log(be.getErrorMessage());
				return IJobHandler.FAIL;
			} catch (Exception e) {
	        	logger.error("调用脚本发送到kafka发生错误：{}", e.getMessage());
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
	private Map<String, Integer> validParams(String params) {
		logger.info("params{}",params);

		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is Empty !");
			return null;
		}
		Map<String,Integer> paramMap = (Map) JsonMapper.fromJsonString(params, Map.class);
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
				br = new BufferedReader(new InputStreamReader(in, StringUtil.isBlank(charset)? "UTF-8":charset));
				String line = null;
				while ((line = br.readLine()) != null) {
					buffer.append(JobUtil.formatJobLog(line));
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
			String  queryTablesUrl = "http://"+selectAnalyticServer+"/druid/v2/sql";
			String queryTablesSql = "{\"query\":\"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%log'\",\"context\":{\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
			String httpPostResult = HttpClientUtils.httpPost(queryTablesUrl, queryTablesSql);
			
			List<Map> list = (List) JsonMapper.fromJsonString(httpPostResult, List.class);
			list.forEach(map->tableList.addAll(map.values()));
			analyticTableList = tableList;
		}
		return analyticTableList;
	}
}
