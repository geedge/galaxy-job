package com.xxl.job.executor.service.jobhandler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.core.utils.RemoteShellUtil;
import com.xxl.job.executor.pojo.RequestParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

@Component
public class DataflowJob {
	private static Logger logger = LoggerFactory.getLogger(DataflowJob.class);

//    @XxlJob("httpToKafkaJobHandler")
	/**
	 * params[0]: Http Rest url 
	 * params[1]: Result BODY key ,Parser Json Data toRecords to Kafka 
	 * params[2]: topicName, kafka topic name
	 */
	public ReturnT<String> httpToKafkaJobHandlerTest(String params) {
		if (StringUtil.isBlank(params) || params.split(",").length != 4) {
			XxlJobLogger.log("params is Empty or params must 4 params!");
			return IJobHandler.FAIL;
		}
		String[] paramsArray = params.split(",");

		// TODO

		XxlJobLogger.log(paramsArray[0], paramsArray[1], paramsArray[2]);
		return IJobHandler.SUCCESS;
	}

	/**
	 * http查询 db，推送kafka 
	 * { url:Http Rest url, method:request method,
	 * requestBody:{query:query sql json }, 
	 * resultKey:"data",
	 * topic:topicName,
	 * method:request method }
	 */
	@XxlJob("httpToKafkaJobHandler")
	public ReturnT<String> httpToKafkaJobHandler(String params) {
		
		logger.info("params: {} ", params);
		
		if (validateHttpToKafkaJobParams(params)) {
			logger.error("params error , params is {}",params);
			return IJobHandler.FAIL;
		}
		
		RequestParam requestParam = (RequestParam) JsonMapper.fromJsonString(params, RequestParam.class);
		String resultMessage = "";
		try {
			if ("post".equalsIgnoreCase(requestParam.getMethod())) {
				resultMessage = HttpClientUtils.httpPost(requestParam.getUrl(), JsonMapper.toJsonString(requestParam.getRequestBody()));
			}
			if ("get".equalsIgnoreCase(requestParam.getMethod())) {
				resultMessage = HttpClientUtils.httpGet(requestParam.getUrl());
			}
			
			return sendMessageToKafka(requestParam, resultMessage);
			
		} catch (BusinessException be) {
			logger.error("params:{}, error:{}", params, be.getErrorMessage());
			XxlJobLogger.log("params:{}, error:{}", params, be.getErrorMessage());
			return new ReturnT<String>(be.getErrorCode(), be.getErrorMessage());
		} catch (Exception e) {
			logger.error("params:{}, error:{}", params, e.getMessage());
			XxlJobLogger.log("params:{}, error:{}", params, e.getMessage());
			return new ReturnT<String>(IJobHandler.FAIL.getCode(), e.getMessage());
		}
	}

	/**
	* 例： 
	* {
	*	"url": "http://ip:port/druid/v2/sql",
	*	"method": "post",
	*	"topic": "topicName",
	*	"requestBody": {query sql ：json sql},
	*	"resultKey": "data"
	*	}
	*/
	private Boolean validateHttpToKafkaJobParams(String params) {
		
		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is empty ! ");
			return true;
		}
		
		RequestParam requestParam = (RequestParam) JsonMapper.fromJsonString(params, RequestParam.class);
		if(requestParam==null) {
			XxlJobLogger.log("params is not format json ! ");
			return true;
		}
		if (StringUtil.isBlank(requestParam.getUrl())) {
			XxlJobLogger.log("url is empty ! ");
			return true;
		}
		if(StringUtil.isEmpty(requestParam.getTopic())) {
			XxlJobLogger.log("topic is empty ! ");
			return true;
		}
		
		String method = requestParam.getMethod();
		if (StringUtil.isEmpty(method) || ( !"get".equalsIgnoreCase(method)&&!"post".equalsIgnoreCase(method) )) {
			XxlJobLogger.log("this request method is not supported");
			return true;
		}
		// post 需要url，method，requestBody,topic
		if("post".equalsIgnoreCase(method)&&requestParam.getRequestBody()==null) {
			XxlJobLogger.log("requestBody is empty ! ");
			return true;
		}
		
		return false;
	}
	
	/**
	 * 处理信息并发送kafka
	 * @param requestParam
	 * @param result
	 */
	private ReturnT<String> sendMessageToKafka(RequestParam requestParam, String result) {
		
		String resultKey = requestParam.getResultKey();
		Object resultObj = JsonMapper.fromJsonString(result, Object.class);
		
		//获取指定key的结果集 , 默认全结果集
		if (StringUtil.isNotBlank(resultKey) && resultObj instanceof Map) {
			Map resultMap = (Map) resultObj;
			
			if (resultMap.containsKey(resultKey)) {
				resultObj = resultMap.get(resultKey);
				
			} else {
				XxlJobLogger.log("No fields matching result key error");
				return ReturnT.FAIL;
			}
		}
		
		new KafkaUtils().sendMessage(requestParam.getTopic(), resultObj);
		XxlJobLogger.log("Send message to kafka, topic is {}", requestParam.getTopic());
		
		return ReturnT.SUCCESS;
	}
	
	/**
	 * 调用脚本实现
	 * @param params 脚本文件
	 * {server,user,password,shell}
	 */
	@XxlJob("remoteCommandJobHandler")
	public ReturnT<String> remoteCommandJobHandler (String params) {
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
		try {
			
			RemoteShellUtil remoteShellUtil = new RemoteShellUtil(paramsMap.get("server"), paramsMap.get("username"), paramsMap.get("password"), "UTF-8");
			String execMessage = remoteShellUtil.exec(paramsMap.get("command"));
			System.err.println(execMessage);
			logger.info("remote command exec message {}", execMessage);
			XxlJobLogger.log("remote command exec message {}", execMessage);
			if(StringUtil.isBlank(execMessage)||"fail".contains(execMessage)) {
				XxlJobLogger.log("remote command exec error");
				return ReturnT.FAIL;
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("remote comnand exec erorr,{}",e.getMessage());
			XxlJobLogger.log("remote comnand exec erorr,{}",e);
			return ReturnT.FAIL;
		}
		return ReturnT.SUCCESS;
	}
	
}
