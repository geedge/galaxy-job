package com.xxl.job.executor.service.jobhandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import com.xxl.job.executor.pojo.HttpToKafkaParam;
import io.netty.handler.codec.http.HttpMethod;
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
import com.zdjizhi.file.FileApplication;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

@Component
public class DataflowJob {
	private static Logger logger = LoggerFactory.getLogger(DataflowJob.class);


	/**
	 * 调度模型httpToKafka
	 * source: http rest url
	 * sink: kafka
	 * params 参数定义如下：
	 * {
	 *     url:<Http Rest url>,
	 *     method:[request method], // get|post 可选，默认get
	 *     requestBody:[{query:query sql json }],
	 *     resultKey:[key], //可选，对结果做二次解析
	 *     topic:<topicName>  // 分区名称，依据系统预置的kafka broker
	 * }
	 * @param params Json 结构
	 * @return
	 */
	@XxlJob("httpToKafkaJobHandler")
	public ReturnT<String> httpToKafkaJobHandler(String params) {


		HttpToKafkaParam httpToKafkaParam = parserHttpToKafkaParams(params);

		if (StringUtil.isEmpty(httpToKafkaParam)) {
			logger.error("params parser error , params is {}",params);
			return IJobHandler.FAIL;
		}



		try {
			String resultMessage;

			if (HttpMethod.POST.name().equalsIgnoreCase(httpToKafkaParam.getMethod())) {
				resultMessage = HttpClientUtils.httpPost(httpToKafkaParam.getUrl(), JsonMapper.toJsonString(httpToKafkaParam.getRequestBody()));
			} else if (HttpMethod.GET.name().equalsIgnoreCase(httpToKafkaParam.getMethod())) {
				resultMessage = HttpClientUtils.httpGet(httpToKafkaParam.getUrl());
			} else {
				return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Not support Method:" + httpToKafkaParam.getMethod());
			}

			 if (resultMessage.equals("-1")) {
				 return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Http Client connection Error!");

			 }

			Object resultObj = JsonMapper.fromJsonString(resultMessage, Object.class);

			 if (StringUtil.isNotBlank(httpToKafkaParam.getResultKey())) {
				 resultObj = ((Map)resultObj).get(httpToKafkaParam.getResultKey());

			 }

			 new KafkaUtils().sendMessage(httpToKafkaParam.getTopic(), (List<Object>) resultObj);

			 XxlJobLogger.log("Send message to kafka, topic is {}", httpToKafkaParam.getTopic());

			return ReturnT.SUCCESS;
			
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
	 * 必填参数进行验证，其它自行配置
	 * @param params
	 * @return
	 */
	private HttpToKafkaParam parserHttpToKafkaParams(String params) {

		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is empty!");
			return null;
		}

		HttpToKafkaParam httpToKafkaParam = (HttpToKafkaParam) JsonMapper.fromJsonString(params, HttpToKafkaParam.class);
		
		if(StringUtil.isEmpty(httpToKafkaParam)) {
			XxlJobLogger.log("params is not format json ! ");
			return null;
		}
		if (StringUtil.isBlank(httpToKafkaParam.getUrl())) {
			XxlJobLogger.log("url is empty ! ");
			return null;
		}
		if(StringUtil.isEmpty(httpToKafkaParam.getTopic())) {
			XxlJobLogger.log("kafka topic is empty ! ");
			return null;
		}
		
		if (StringUtil.isBlank(httpToKafkaParam.getMethod())) {
			XxlJobLogger.log("this request method is not supported");
			httpToKafkaParam.setMethod(HttpMethod.GET.name());
		}


		return httpToKafkaParam;
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
	            new KafkaUtils().sendMessage(topic, (List<Object>) resultMap.get("data"));
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
