package com.xxl.job.executor.service.jobhandler;

import com.google.common.collect.Maps;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.pojo.DruidQueryParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

import java.util.List;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
	 * http查询 db，推送kafka { url:Http Rest url, query:query sql json, topic:topicName,
	 * method:request method }
	 */
	@XxlJob("httpToKafkaJobHandler")
	public ReturnT<String> httpToKafkaJobHandler(String params) {
		try {
			if (StringUtil.isBlank(params)) {
				XxlJobLogger.log("params is Empty !");
				return IJobHandler.FAIL;
			}
			Map<String, String> paramsMap = Maps.newHashMap();
			try {
				paramsMap = (Map) JsonMapper.fromJsonString(params, Map.class);
			} catch (Exception e) {
				e.printStackTrace();
				XxlJobLogger.log("params is not json !");
				return IJobHandler.FAIL;
			}
			if (StringUtil.isAnyEmpty(paramsMap.get("url"), paramsMap.get("query"), paramsMap.get("topic"),
					paramsMap.get("method")) && "post".equalsIgnoreCase(paramsMap.get("method"))) {
				XxlJobLogger.log("params must 4 params!,including url, query, topic, method");
				return IJobHandler.FAIL;
			}
			XxlJobLogger.log("params:" + paramsMap);

			if ("post".equalsIgnoreCase(paramsMap.get("method"))) {
				// 构建查询druid参数
				Map<String, String> context = Maps.newHashMap();
				context.put("sqlTimeZone", "Asia/Shanghai");
				context.put("skipEmptyBuckets", "false");
				DruidQueryParam druidQueryParam = DruidQueryParam.builder().query(paramsMap.get("query")) .context(context).resultFormat("object").build();
				Map<String, String> resultMap = HttpClientUtils.httpPost(paramsMap.get("url"), JsonMapper.toJsonString(druidQueryParam));
				if (!String.valueOf(HttpStatus.SC_OK).equals(resultMap.get("status"))) {
					XxlJobLogger.log(String.valueOf(resultMap));
					logger.error("http获取数据错误：{}", resultMap);
					return ReturnT.FAIL;
				} else {
					// 发生错误，数据为空都不发送到kakfa
					List<Object> allResults = (List<Object>) JsonMapper.fromJsonString(resultMap.get("result"), Object.class);
					XxlJobLogger.log("topic:" + paramsMap.get("topic"), "result:" + resultMap);
					if (StringUtil.isNotEmpty(allResults) && allResults.size() > 0) {
						new KafkaUtils().sendMessage(paramsMap.get("topic"), allResults);
					}
					logger.debug("推送kafka消息:{}", resultMap.get("result"));
				}
			} else if ("get".equalsIgnoreCase(paramsMap.get("method"))) {
				String resultMessage = HttpClientUtils.httpGet(paramsMap.get("url"));
				XxlJobLogger.log("topic:" + paramsMap.get("topic"), "result:" + resultMessage);
				if ("-1".equals(resultMessage)) {
					logger.error("http获取数据错误：{}", resultMessage);
					return ReturnT.FAIL;
				} else {
					if (StringUtil.isNotEmpty(resultMessage)) {
						new KafkaUtils().sendMessage(paramsMap.get("topic"), resultMessage);
					}
					logger.debug("推送kafka消息:{}", resultMessage);
				}
			} else {
				return new ReturnT<String>(IJobHandler.FAIL.getCode(), "this method is not supported");
			}

			return ReturnT.SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			XxlJobLogger.log(e);
			return new ReturnT<String>(IJobHandler.FAIL.getCode(), e.getMessage());
		}
	}

}
