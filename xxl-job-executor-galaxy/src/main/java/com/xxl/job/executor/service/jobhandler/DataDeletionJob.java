package com.xxl.job.executor.service.jobhandler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.zdjizhi.utils.Encodes;
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
			String tableName = deleteDataParam.getTablename();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			
			//参数拼接命令
			/**
		      * echo "ALTER TABLE tsg_galaxy_v3.connection_record_log_local_ttl2 MODIFY TTL ttl_date + INTERVAL 11 DAY" | 
		      * curl 'http://default:111111@192.168.40.193:8123/' --data-binary @- 
		     */
			String url = "curl 'http://"+deleteDataParam.getUsername()+":"+deleteDataParam.getPassword()+"@"+deleteDataParam.getServer()+"/' --data-binary @-";
		      
			//远程调用
			tableList.forEach(table->{
				commandJob( "echo \"ALTER TABLE "+deleteDataParam.getDatasource()+"."+ table +" MODIFY TTL ttl_date + INTERVAL "+deleteDataParam.getMaxdays()+" DAY\" | " + url);
			});
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be);
			return ReturnT.FAIL;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			XxlJobLogger.log(e);
			return ReturnT.FAIL;
		}
		
		return ReturnT.SUCCESS;
	}
    private ReturnT<String> commandJob(String param) {
        String command = param;
        int exitValue = -1;

        BufferedReader bufferedReader = null;
        try {
            // command process
            Process process = Runtime.getRuntime().exec(command);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(process.getInputStream());
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream));

            // command log
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                XxlJobLogger.log(line);
            }

            // command exit
            process.waitFor();
            exitValue = process.exitValue();
        } catch (Exception e) {
        	logger.error(e.getMessage());
            XxlJobLogger.log(e);
        } finally {
            if (bufferedReader != null) {
                try {
					bufferedReader.close();
				} catch (IOException e) {
					logger.error("释放链接错误: {}", e.getMessage());
                    e.printStackTrace();
				}
            }
        }

        if (exitValue == 0) {
            return IJobHandler.SUCCESS;
        } else {
            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "command exit value("+exitValue+") is failed");
        }
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
			String tableName = deleteDataParam.getTablename();//connection_record_log_local_ttl2 ,tableName,
			Integer maxDays = deleteDataParam.getMaxdays();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			//修改规则
			String changeRulesUrl ="http://"+ server +"/druid/coordinator/v1/rules/";
			String changeRulesParam ="[{\"type\":\"loadByPeriod\",\"period\":\"P"+maxDays+"D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]";
			//物理删除除
			String deleteUrl = "http://"+server+"/druid/indexer/v1/task";
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			//1970-01-01/当前时间-maxDay
			String deleteMaxDate = DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD, -Integer.valueOf(maxDays));
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
			XxlJobLogger.log(be);
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
		// 清空click house
		// 示例语法：
		// TRUNCATE TABLE [IF EXISTS] [db.]name
		//	若为集群：
		//TRUNCATE TABLE [IF EXISTS] [db.]name ON CLUSTER cluster
		
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
			Map<String, Object> ttlDeleteParam = Maps.newHashMap();
			ttlDeleteParam.put("database", datasource);
			ttlDeleteParam.put("password", deleteDataParam.getPassword());
			ttlDeleteParam.put("user", deleteDataParam.getUsername());
			String url = "http://"+server;
			//命令参数
			String prefixDeleteTtl = "TRUNCATE TABLE IF EXISTS "+ datasource+".";
			//String clusterName = "ck_cluster";
			String suffixDeleteTtl = " ON CLUSTER ck_cluster";
			
			tableList.forEach(table->{
				ttlDeleteParam.put("query", Encodes.urlEncode(prefixDeleteTtl+ table +suffixDeleteTtl));
				String deleteMessage = HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(url, ttlDeleteParam), null);
				logger.info("delete message:{}",deleteMessage);
				XxlJobLogger.log("delete message:{}",deleteMessage);
			});
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be);
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
			String server = deleteDataParam.getServer();
			String tableName = deleteDataParam.getTablename();
			Integer maxDays = deleteDataParam.getMaxdays();
			List<String> tableList = Splitter.on(",").trimResults().splitToList(tableName);
			
			//停止加载
			String changeRulesUrl ="http://"+ server +"/druid/coordinator/v1/rules/";
			String changeRulesParam = "[{\"type\":\"dropForever\"}]";
			
			//物理删除所有数据
			String deleteUrl ="http://"+ server +"/druid/indexer/v1/task";
			String prefixDeleteParam = "{\"type\":\"kill\",\"dataSource\":\"";
			String suffixDeleteParam = "\",\"interval\":\"1970-01-01/"+DateUtils.getSomeDate(TimeConstants.YYYY_MM_DD,1)+"\"}";
			
			//恢复加载规则
			String resetRulesUrl ="http://"+ server +"/druid/coordinator/v1/rules/";
			String resetRulesParam ="[{\"type\":\"loadByPeriod\",\"period\":\"P"+ maxDays +"D\",\"tieredReplicants\":{\"_default_tier\":1}},{\"type\":\"dropForever\"}]";
			//停止加载的规则
			tableList.forEach(table->{
				HttpClientUtils.httpPost(changeRulesUrl+table, changeRulesParam);
				String deleteMessage = HttpClientUtils.httpPost(deleteUrl, prefixDeleteParam+ table +suffixDeleteParam);
				XxlJobLogger.log("delete message :{}",deleteMessage);
				HttpClientUtils.httpPost(resetRulesUrl+table, resetRulesParam);
			});
		} catch (BusinessException be) {
			be.printStackTrace();
			logger.error(be.getErrorMessage());
			XxlJobLogger.log(be);
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
