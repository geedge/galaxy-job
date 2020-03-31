package com.xxl.job.executor.service.jobhandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.config.StorgeConfig;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.pojo.RequestParam;
import com.zdjizhi.file.FileApplication;
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
			logger.error(be.getErrorMessage());
			String errorMessage = be.getErrorMessage();
			errorMessage=errorMessage.replace("\"", "\\\"");
			XxlJobLogger.log(errorMessage);
			return new ReturnT<String>(be.getErrorCode(), errorMessage);
		} catch (Exception e) {
			logger.error(e.getMessage());
			XxlJobLogger.log(e.getMessage());
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
	 * @throws Exception 
	 */
	private ReturnT<String> sendMessageToKafka(RequestParam requestParam, String result) throws Exception {
		
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
	 * 本地服务器调用脚本实现
	 * @param params 脚本文件
	 * {"scriptName":"scriptName", "topic":"topic"}
	 */
	@XxlJob("scriptToKafkaJobHandler")
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
	            // command process
	        	//脚本文件默认存放目录
				String scriptDirectory = "/usr/local/storge/";
				//kafka topic
				String topic = StringUtil.isBlank(paramsMap.get("topic"))? "SYS-STORAGE-LOG" : paramsMap.get("topic");
				
				//脚本名称
				String scriptName = paramsMap.get("scriptName");
				//脚本文件带路径
				String scriptFile = scriptDirectory + scriptName;
				
				if(!FileApplication.isExist(scriptFile)) {
		        	//脚本解压目录
		        	String directoryZip = "/usr/local/";
					//脚本压缩包
					String scriptZip = "storge.zip";
					//源压缩文件
					String sourceFile = new File("").getCanonicalPath()+"/config/script/" + scriptZip;
					boolean unZipFiles = FileApplication.unZipFiles(sourceFile, directoryZip);
					XxlJobLogger.log("unzip file success: {}", unZipFiles);
					//开通权限,修改配置文件
					String[] commands  = new String[]{"/bin/sh","-c",StorgeConfig.getStorgeConfigCommand(scriptDirectory)};
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
	            XxlJobLogger.log("script exec message {}", execResult);
	            
	            Map resultMap = (Map) JsonMapper.fromJsonString(execResult, Map.class);
	            if(StringUtil.isBlank(execResult) || resultMap==null) {
					XxlJobLogger.log("script exec error");
					return ReturnT.FAIL;
				}
	            //发送到kafka
	            new KafkaUtils().sendMessage(topic, resultMap.get("data"));
	            XxlJobLogger.log("Send message to kafka, topic is {}", topic);
	            logger.info("Send message to kafka, topic is {}", topic);
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
	 * 解析脚本返回结果
	 * @param InputStream
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
}
