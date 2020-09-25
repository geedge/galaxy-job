package com.mesalab.executor.core.utils;

import com.mesalab.executor.exception.BusinessException;

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

	public static String getErrorMsg(Exception e){
		System.out.println(e.getMessage());
		if(e instanceof BusinessException){
			return ((BusinessException) e).getErrorMessage();
		}
		return e.getMessage();
	}
}
