package com.xxl.job.executor.core.utils;

public class JobUtil {
	
	/**
	 * 对执行器中日志字符串做处理防止特殊符号转义报错
	 * @param message
	 * @return
	 */
	public static String formatJobLog(String message) {
		message = message.replace("\"", "\\\"");
		return message;
	}

}
