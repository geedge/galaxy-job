package com.xxl.job.executor.jobhandler;

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
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.pojo.HttpToKafkaParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

import io.netty.handler.codec.http.HttpMethod;

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
			XxlJobLogger.log(be.getErrorMessage());
			return new ReturnT<String>(be.getErrorCode(), be.getErrorMessage());
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
	

}
