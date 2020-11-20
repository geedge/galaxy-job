package com.xxl.job.core.log;

import com.xxl.job.core.biz.model.LogResult;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * store trigger log in each log-file
 * @author xuxueli 2016-3-12 19:25:12
 */
public class XxlJobFileAppender {
	private static Logger logger = LoggerFactory.getLogger(XxlJobFileAppender.class);
	
	// for JobThread (support log for child thread of job handler)
	//public static ThreadLocal<String> contextHolder = new ThreadLocal<String>();
	public static final InheritableThreadLocal<String> contextHolder = new InheritableThreadLocal<String>();


	/**
	 * log base path
	 *
	 * strut like:
	 * 	---/
	 * 	---/gluesource/
	 * 	---/gluesource/10_1514171108000.js
	 * 	---/gluesource/10_1514171108000.js
	 * 	---/2017-12-25/
	 * 	---/2017-12-25/639.log
	 * 	---/2017-12-25/821.log
	 *
	 */
//	private static String logBasePath = File.separator.concat("data").concat(File.separator).concat("applogs").concat(File.separator).concat("xxl-job").concat(File.separator).concat("jobhandler");
	private static String logBasePath = "";
	private static String glueSrcPath = "";
//	private static String glueSrcPath = logBasePath.concat(File.separator).concat("gluesource");
	public static void initLogPath(String logPath){
		// init
		if (logPath!=null && logPath.trim().length()>0) {
			logBasePath = logPath;
		}
		// mk base dir
		File logPathDir = new File(logBasePath);
		if (!logPathDir.exists()) {
			logPathDir.mkdirs();
		}
		logBasePath = logPathDir.getPath();

		// mk glue dir
		File glueBaseDir = new File(logPathDir, "gluesource");
		if (!glueBaseDir.exists()) {
			glueBaseDir.mkdirs();
		}
		glueSrcPath = glueBaseDir.getPath();
	}
	public static String getLogPath() {
		return logBasePath;
	}
	public static String getGlueSrcPath() {
		return glueSrcPath;
	}

	/**
	 * log filename, like "logPath/yyyy-MM-dd/9999.log"
	 *
	 * @param triggerDate
	 * @param logId
	 * @return
	 */
	public static String makeLogFileName(Date triggerDate, long logId) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// filePath/yyyy-MM-dd
		/*SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");	// avoid concurrent problem, can not be static
		String logp = getLogPath();
		File logFilePath = new File(logp, sdf.format(triggerDate));
		if (!logFilePath.exists()) {
			logFilePath.mkdir();
		}*/

		// filePath/yyyy-MM-dd/9999.log
		/*String logFileName = logFilePath.getPath()
				.concat(File.separator)
				.concat(String.valueOf(logId))
				.concat(".log");*/
		return sdf.format(triggerDate).concat(".log");
	}

	/**
	 * append log
	 *
	 * @param logFileName
	 * @param appendLog
	 */
	public static void appendLog(String logFileName, String appendLog) {

		// log file
		if (logFileName==null || logFileName.trim().length()==0) {
			return;
		}
		File logFile = new File(logFileName);

		if (!logFile.exists()) {
			try {
				logFile.createNewFile();
			} catch (IOException e) {
				logger.error("errorMessage:{}", e);
				return;
			}
		}

		// log
		if (appendLog == null) {
			appendLog = "";
		}
		appendLog += "\r\n";
		
		// append file content
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(logFile, true);
			fos.write(appendLog.getBytes("utf-8"));
			fos.flush();
		} catch (Exception e) {
			logger.error("errorMessage:{}", e);
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					logger.error("errorMessage:{}", e);
				}
			}
		}
		
	}

	/**
	 * support read log-file
	 *
	 * @param logFileName
	 * @return log content
	 */
	public static LogResult readLog(String logFileName, int fromLineNum){

		// valid log file
		if (logFileName==null || logFileName.trim().length()==0) {
            return new LogResult(fromLineNum, 0, "readLog fail, logFile not found", true);
		}
		File logFile = new File(logFileName);

		if (!logFile.exists()) {
            return new LogResult(fromLineNum, 0, "readLog fail, logFile not exists", true);
		}

		// read file
		StringBuffer logContentBuffer = new StringBuffer();
		int toLineNum = 0;
		final int MAX_STR_LEN = 100_000_000;
		try(BoundedInputStream boundedInputStream = new BoundedInputStream(new FileInputStream(logFile), 30000 * 1024);
			LineNumberReader reader = new LineNumberReader(new InputStreamReader(boundedInputStream, "utf-8"),1024*10);) {

			/*String line = null;
			while ((line = reader.readLine())!=null) {
				toLineNum = reader.getLineNumber();		// [from, to], start as 1
				if (toLineNum >= fromLineNum) {
					logContentBuffer.append(line).append("\n");
				}
			}*/

			int intC;
			while ((intC = reader.read()) != -1) {
				char c = (char) intC;
				if (c == '\n') {
					break;
				}
				if (logContentBuffer.length() >= MAX_STR_LEN) {
					throw new RuntimeException("input too long");
				}
				logContentBuffer.append(c);
			}


		} catch (IOException e) {
			logger.error("errorMessage:{}", e);
		}

		// result
		LogResult logResult = new LogResult(fromLineNum, toLineNum, logContentBuffer.toString(), false);
		return logResult;

		/*
        // it will return the number of characters actually skipped
        reader.skip(Long.MAX_VALUE);
        int maxLineNum = reader.getLineNumber();
        maxLineNum++;	// 最大行号
        */
	}

	/**
	 * read log data
	 * @param logFile
	 * @return log line content
	 */
	/*public static String readLines(File logFile){
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"));
			if (reader != null) {
				StringBuilder sb = new StringBuilder();
				String line = null;
				while ((line = reader.readLine()) != null) {
					sb.append(line).append("\n");
				}
				return sb.toString();
			}
		} catch (IOException e) {
			logger.error("errorMessage:{}", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("errorMessage:{}", e);
				}
			}
		}
		return null;
	}*/

}
