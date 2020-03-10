package com.xxl.job.executor.core.utils;

import java.util.Map;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

public class ToolUtil {

	/**
	 * 获取url中的指定name参数
	 * 
	 * @param url
	 * @param name
	 * @return
	 */
	public static String getUrlParam(String url, String name) {
		try {
			String params = url.substring(url.indexOf("?") + 1, url.length());
			Map<String, String> split = Splitter.on("&").withKeyValueSeparator("=").split(params);
			return split.get(name);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 移除url中指定key的参数
	 * @param url
	 * @param name
	 * @return String url
	 */
	public static String removeUrlParam(String url, String name) {
		String params = url.substring(url.indexOf("?") + 1, url.length());
		Map<String, String> split = Splitter.on("&").withKeyValueSeparator("=").split(params);
		Map<String, Object> newHashMap = Maps.newHashMap(split);
		newHashMap.remove(name);
		return getUrlWithParams(url.substring(0, url.indexOf("?")), newHashMap);
	}
	/**
          * 拼装url
    * url ,参数map
    */
    public static String getUrlWithParams(String url, Map<String, Object> params) {
        try {
            StringBuffer query = new StringBuffer();
            for (Map.Entry<String, Object> kv : params.entrySet()) {
                query.append(kv.getKey() + "=");
                query.append(kv.getValue() + "&");
            }
            if (query.lastIndexOf("&") > 0) {
                query.deleteCharAt(query.length() - 1);
            }
            return url + "?" + query.toString();
        } catch (Exception ex) {
            throw ex;
        }
    }
	public static void main(String[] args) throws InterruptedException {
//		String param = getUrlParam("http://www.bai.com?", "topic0");
//		String domain = FormatUtils.getDomain("http://www.bai.com");
//		System.err.println(param);
//		System.err.println(domain);
//		String valueOf = String.valueOf();
//		System.err.println(Objects.nonNull(""));
//		boolean empty = ObjectUtils.isEmpty(null);
//		System.err.println(empty);
////		StringUtil.isBlank(valueOf);
//		StopWatch queryWatch = new StopWatch();
//		queryWatch.start();
//		Thread.sleep(3000L);
//		queryWatch.stop();
//		System.err.println(queryWatch.getTime(TimeUnit.SECONDS));
//		System.err.println(queryWatch.getTime());
		String removeUrlParam = removeUrlParam("http://192.168.40.224:8089/druid/v2/sql?topic=TOP-WEBSITE-DOMAIN-LOG&query=select TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') as start_time, sum(hits) as hits from security_event_hits_log where policy_id<10 group by TIME_FORMAT(TIME_FLOOR(__time,'PT5M'),'yyyy-MM-dd HH:mm:ss') limit 5", "topic");
		System.err.println(removeUrlParam);
	}

}
