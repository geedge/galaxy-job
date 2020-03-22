package com.xxl.job.executor.service.jobhandler;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.pojo.DeleteDataParam;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.TimeConstants;

@Component
public class DataDeletionJob {
	
	private static Logger logger = LoggerFactory.getLogger(DataDeletionJob.class);

	/**
	  *  流量日志清除任务
	 *  clickhouse 数据库
	 * @param params
	 */
	@XxlJob("deleteTrafficDataJobHandler")
	public ReturnT<String> deleteTrafficDataJobHandler(String params) {
		logger.info("params{}",params);
		
		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is Empty !");
			return IJobHandler.FAIL;
		}
		
		try {
			DeleteDataParam deleteDataParam = (DeleteDataParam) JsonMapper.fromJsonString(params, DeleteDataParam.class);
			if(deleteDataParam==null) {
				XxlJobLogger.log("params error !");
				return IJobHandler.FAIL;
			}
			String server = deleteDataParam.getServer();
			String tableName = deleteDataParam.getTablename();
			Integer maxDays = deleteDataParam.getMaxdays();
			String datasource = deleteDataParam.getDatasource();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			
			//删除sql参数拼接
			Map<String, Object> deleteParamMap = Maps.newHashMap();
			deleteParamMap.put("database", datasource);
			deleteParamMap.put("password", deleteDataParam.getPassword());
			deleteParamMap.put("user", deleteDataParam.getUsername());
			
			String url = "http://"+server;
			String prefixDeleteSql = "ALTER TABLE "+ datasource +".";
			String suffixDeleteSql =" ON CLUSTER ck_cluster DROP PARTITION ";
			int deleteMaxDate = Integer.valueOf(DateUtils.getSomeDate(TimeConstants.YYYYMMDD, -maxDays));
			
			tableList.forEach(table->{
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
	 * Report and Metrics 日志清除任务
	 * druid 数据库
	 * @param params
	 * @return
	 */
	@XxlJob("deleteReportAndMetricsDataJobHandler")
	public ReturnT<String> deleteReportAndMetricsDataJobHandler(String params) {
		logger.info("params{}",params);

		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is Empty !");
			return IJobHandler.FAIL;
		}
		try {
			DeleteDataParam deleteDataParam = (DeleteDataParam) JsonMapper.fromJsonString(params, DeleteDataParam.class);
			if(deleteDataParam==null) {
				XxlJobLogger.log("params error !");
				return IJobHandler.FAIL;
			}
			String server = deleteDataParam.getServer();
			String tableName = deleteDataParam.getTablename();
			Integer maxDays = deleteDataParam.getMaxdays();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			//修改规则
			String changeRulesUrl ="http://"+ server +"/druid/coordinator/v1/rules/";
			String changeRulesParam ="[{\"type\":\"loadByPeriod\",\"period\":\"P"+maxDays+"D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]";
			//物理删除除
			String deleteUrl = "http://"+server+"/druid/indexer/v1/task";
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			//1970-01-01/当前时间-maxDay
			String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -maxDays);
			String suffixDeleteParam = "\",\"interval\":\"1970-01-01/"+deleteMaxDate+"\"}";
			
			tableList.forEach(table->{
				
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
	 * @param params
	 */
	@XxlJob("deleteAllTrafficDataJobHandler")
	public ReturnT<String> deleteAllTrafficDataJobHandler(String params){
		logger.info("params{}",params);

		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is Empty !");
			return IJobHandler.FAIL;
		}
		
		try {
			DeleteDataParam deleteDataParam = (DeleteDataParam) JsonMapper.fromJsonString(params, DeleteDataParam.class);
			if(deleteDataParam==null) {
				XxlJobLogger.log("params error !");
				return IJobHandler.FAIL;
			}
			String server = deleteDataParam.getServer();
			String datasource = deleteDataParam.getDatasource();
			String tableName = deleteDataParam.getTablename();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			
			//连接参数
			Map<String, Object> deleteParamMap = Maps.newHashMap();
			deleteParamMap.put("database", datasource);
			deleteParamMap.put("password", deleteDataParam.getPassword());
			deleteParamMap.put("user", deleteDataParam.getUsername());
			String url = "http://"+server;
			//清库命令参数
			String prefixDeleteSql = "TRUNCATE TABLE IF EXISTS "+ datasource+".";
			//String clusterName = "ck_cluster";
			String suffixDeleteTtl = " ON CLUSTER ck_cluster";
			
			tableList.forEach(table->{
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
	 * @param params
	 */
	@XxlJob("deleteAllReportAndMetricsDataJobHandler")
	public ReturnT<String> deleteAllReportAndMetricsDataJobHandler(String params) {
		logger.info("params{}",params);

		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is Empty !");
			return IJobHandler.FAIL;
		}
		
		try {
			DeleteDataParam deleteDataParam = (DeleteDataParam) JsonMapper.fromJsonString(params, DeleteDataParam.class);
			if(deleteDataParam==null) {
				XxlJobLogger.log("params error !");
				return IJobHandler.FAIL;
			}
			String servers = deleteDataParam.getServer();
			List<String> serverList = Splitter.on(",").trimResults().splitToList(servers);
			String deleteServer = serverList.get(0);//用于删除的节点
			String queryServer = serverList.get(1);//用于查询
			String tableName = deleteDataParam.getTablename();
			Integer maxDays = deleteDataParam.getMaxdays();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			
			//停止加载
			String changeRulesUrl ="http://"+ deleteServer +"/druid/coordinator/v1/rules/";
			String changeRulesParam = "[{\"type\":\"dropForever\"}]";
			
			//物理删除所有数据
			String deleteUrl ="http://"+ deleteServer +"/druid/indexer/v1/task";
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			String suffixDeleteParam = "\",\"interval\":\"1970-01-01/"+DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD,1)+"\"}";
			
			//恢复加载规则
			String resetRulesUrl ="http://"+ deleteServer +"/druid/coordinator/v1/rules/";
			String resetRulesParam ="[{\"type\":\"loadByPeriod\",\"period\":\"P"+ maxDays +"D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]";
			//查询表数据url
			String countDataUrl = "http://"+queryServer+"/druid/v2/sql";
			//限制操作时间五分钟，开始计时
			long startTime = System.currentTimeMillis();
			long endTime = startTime + 1000*60*5;
			tableList.forEach(table->{
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				//查询表数据是否删除 固定key EXPR$0
				String countTableDataSql ="{\"query\":\"SELECT count(*) from "+table+" where __time < '"+DateUtils.getCurrentDate()+"'\",\"context\":{\"sqlTimeZone\":\"Asia/Shanghai\",\"skipEmptyBuckets\":\"false\"},\"resultFormat\":\"object\"}";
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

}
