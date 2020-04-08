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

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.config.StorgeConfig;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.zdjizhi.file.FileApplication;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

@Component
public class StorageJob {
	
	private static Logger logger = LoggerFactory.getLogger(StorageJob.class);
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
	        	//脚本文件默认存放目录
				String defDestDirectory = "/usr/local/storge/";
				//默认topic
				String defTopic = "SYS-STORAGE-LOG";
				
	        	//脚本解压目录
	        	String destDirectory = StringUtil.isBlank(paramsMap.get("destDirectory"))? defDestDirectory:paramsMap.get("destDirectory");
			
				//kafka topic
				String topic = StringUtil.isBlank(paramsMap.get("topic"))? defTopic : paramsMap.get("topic");
				
				//脚本名称
				String scriptName = paramsMap.get("scriptName");
				//脚本文件带路径
				String scriptFile = destDirectory + scriptName;
				
				if(!FileApplication.isExist(scriptFile)) {
					//默认源文件压缩包
					String defSourceZipFile = "/config/script/storge.zip";
		        	//脚本压缩包
		        	String sourceZip = StringUtil.isBlank(paramsMap.get("sourceZipFile"))?defSourceZipFile:paramsMap.get("sourceZipFile");
					//源压缩文件
					String sourceFile = new File("").getCanonicalPath() + sourceZip;
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
