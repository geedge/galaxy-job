package com.xxl.job.executor.service.jobhandler;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.core.utils.ToolUtil;
import com.xxl.job.executor.pojo.RequestParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DataflowJob {
	private static Logger logger = LoggerFactory.getLogger(DataflowJob.class);

//    @XxlJob("httpToKafkaJobHandler")
	/**
	 * params[0]: Http Rest url params[1]: Result BODY key ,Parser Json Data
	 * toRecords to Kafka params[2]: topicName, kafka topic name
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
	 * requestBody:{query:query sql json, topic:topicName}, 
	 * resultKey:"data,rows,statistics,time",
	 * method:request method }
	 */
	@XxlJob("httpToKafkaJobHandler")
	@SuppressWarnings("unchecked")
	public ReturnT<String> httpToKafkaJobHandler(String params) {
		if (validataHttpToKafkaJobParams(params)) {
			logger.error("params is error , params is {}",params);
			return IJobHandler.FAIL;
		}
		RequestParam requestParam = (RequestParam) JsonMapper.fromJsonString(params, RequestParam.class);
		String url = requestParam.getUrl();
		String method = requestParam.getMethod();
		Map<String, Object> requestBody = requestParam.getRequestBody();
		String resultKey = requestParam.getResultKey();// 设定返回结果值
		try {
			XxlJobLogger.log("params is " + params);
			StopWatch queryWatch = new StopWatch();//统计http查询时间
			StopWatch kafkaWatch = new StopWatch();//统计推送kafka时间

			if ("post".equalsIgnoreCase(method)) {
				String topic = (String) requestBody.get("topic");
				queryWatch.start();
				Map<String, String> resultMap = HttpClientUtils.httpPost(url, JsonMapper.toJsonString(requestBody));
				queryWatch.stop();
				if ("200".equals(resultMap.get("status"))) {
					// 发生错误，数据为空都不发送到kakfa
					List<Object> allResults = (List<Object>) JsonMapper.fromJsonString(resultMap.get("result"),Object.class);
					if (StringUtil.isNotEmpty(allResults) && allResults.size() > 0) {
						//指定key过滤结果集
						if(ObjectUtils.isNotEmpty(resultKey)) {
							Iterable<String> keys = Splitter.on(",").omitEmptyStrings().split(resultKey);
							List<Object> newArrayList = Lists.newArrayList();
							for (Object map : allResults) {
								Map<String, Object> newHashMap = Maps.newHashMap();
								for (String key : keys) {
									newHashMap.put(key,((Map) map).get(key));
								}
								newArrayList.add(newHashMap);
							}
							allResults = newArrayList;
						}
						kafkaWatch.start();
						new KafkaUtils().sendMessage(topic, allResults);
						kafkaWatch.stop();
					}
					XxlJobLogger.log("query time:{}, push to kafka time:{}ms, topic:{}ms, result:{} rows",queryWatch.getTime(),kafkaWatch.getTime(), topic, allResults.size());
					logger.debug("推送kafka消息:{}", resultMap.get("result"));
					return ReturnT.SUCCESS;
				} else {
					XxlJobLogger.log(String.valueOf(resultMap));
					logger.error("http获取数据错误：topic is :{}, data is {}", topic, resultMap);
					return ReturnT.FAIL;
				}
				
				
			} else if ("get".equalsIgnoreCase(method)) {
				String topic = ToolUtil.getUrlParam(url, "topic");
				url = ToolUtil.removeUrlParam(url, "topic");
				queryWatch.start();
				String resultMessage = HttpClientUtils.httpGet(url);
				queryWatch.stop();
				if ("-1".equals(resultMessage)) {
					logger.error("http获取数据错误：topic is :{}, data is {}", topic, resultMessage);
					return ReturnT.FAIL;
				} else {
					if (StringUtil.isNotEmpty(resultMessage)) {
						//获取指定key的结果集
						if(ObjectUtils.isNotEmpty(resultKey)) {
							Map<String, Object> newResultMap = Maps.newHashMap();
							Map<String,Object> resultMap = (Map<String,Object>) JsonMapper.fromJsonString(resultMessage, Map.class);
							Splitter.on(",").omitEmptyStrings().split(resultKey).forEach(key->newResultMap.put(key, resultMap.get(key)));
							resultMessage = JsonMapper.toJsonString(newResultMap);
						}
						kafkaWatch.start();
						new KafkaUtils().sendMessage(topic, resultMessage);
						kafkaWatch.stop();
					}
					XxlJobLogger.log("query time:{}ms, push to kafka time:{}ms,topic:{}, result:{}",queryWatch.getTime(),kafkaWatch.getTime(), topic, resultMessage);
					logger.debug("推送kafka topic:{},消息:{}",topic, resultMessage);
					return ReturnT.SUCCESS;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			XxlJobLogger.log(e);
			return new ReturnT<String>(IJobHandler.FAIL.getCode(), e.getMessage());
		}
		return ReturnT.FAIL;
	}

	/**
	* 例： {
	*	"url":"http://192.168.40.224:8089/druid/v2/sql",
	*	"method":"post",
	*	"requestBody":{"topic":"TOP-INTERNAL-HOST-LOG",
	*	"query":"SELECT common_client_ip AS source,sum(session_num) AS session_num,sum( c2s_byte_num ) AS c2s_byte_num,sum( s2c_byte_num ) AS s2c_byte_num,sum(c2s_pkt_num) AS c2s_pkt_num,sum(s2c_pkt_num) AS s2c_pkt_num,TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') as \"time\",'sessions' as order_by FROM top_internal_host_log_hot WHERE __time >= TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') AND __time < TIME_FLOOR(CURRENT_TIMESTAMP,'PT5M') GROUP BY common_client_ip limit 5","context":{"sqlTimeZone":"Asia/Shanghai","skipEmptyBuckets":"false"},"resultFormat":"object"},
	*	"resultKey":"data,row"
	*	}
	* 
	* @param params
	* @return Boolean
	*/
	public Boolean validataHttpToKafkaJobParams(String params) {
		if (StringUtil.isBlank(params)) {
			XxlJobLogger.log("params is empty !");
			return true;
		}
		RequestParam requestParam = (RequestParam) JsonMapper.fromJsonString(params, RequestParam.class);
		if(requestParam==null) {
			XxlJobLogger.log("params is not format json ! {}", params);
			return true;
		}
		String url = requestParam.getUrl();
		if (StringUtil.isBlank(url)) {
			XxlJobLogger.log("url is empty !");
			return true;
		}
		// post 需要url，method，requestBody：topic
		String method = requestParam.getMethod();
		if("post".equalsIgnoreCase(method)) {
			Map<String, Object> requestBody = requestParam.getRequestBody();
			if(requestBody==null) {
				XxlJobLogger.log("requestBody is empty ! ");
				return true;
			}
			if(ObjectUtils.isEmpty(requestBody.get("topic"))) {
				XxlJobLogger.log("topic is empty ! ");
				return true;
			}
			
		}else if (StringUtil.isEmpty(method) || "get".equalsIgnoreCase(method)) {
			// get 需要url
			if(ToolUtil.getUrlParam(url, "topic") == null) {
				XxlJobLogger.log("topic is empty ! ");
				return true;
			}
			
		}else {
			XxlJobLogger.log("this request method is not supported");
			return true;
		}

		return false;
	}
	
}
