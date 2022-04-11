package com.mesalab.executor.core.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONNull;
import cn.hutool.log.Log;
import com.mesalab.executor.exception.BusinessException;
import com.xxl.job.core.log.XxlJobLogger;

import java.io.File;
import java.util.*;

public class JobUtil {
    private static Log logger = Log.get();

    /**
     * 对执行器中日志字符串做处理防止特殊符号转义报错
     *
     * @param message
     * @return
     */
    public static String formatJobLog(String message) {
        message = message.replace("\"", "\\\"");
        return message;
    }

    public static String getErrorMsg(Exception e) {
        if (e instanceof BusinessException) {
            return ((BusinessException) e).getMessage();
        }
        return e.getMessage();
    }

    /**
     * 导出csv
     *
     * @param maps
     * @param filePath
     * @return file
     */
    public static File createCsvFile(List<Map> maps, String filePath) {
        File file = FileUtil.newFile(filePath);
        try (CsvWriter writer = CsvUtil.getWriter(file, CharsetUtil.CHARSET_UTF_8)) {
            List result = new ArrayList<String[]>();
            if (ObjectUtil.isNotEmpty(maps)) {
                Object[] fieldArr = maps.get(0).keySet().toArray();
                result.add(Arrays.asList(fieldArr));
                maps.forEach(m -> result.add(m.values()));
            }
            writer.write(result);
            return file;
        } catch (IORuntimeException e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e.getMessage());
            return null;
        }
    }

    /**
     * 获取map中第一个非空数据值
     *
     * @param <K> Key的类型
     * @param <V> Value的类型
     * @param map 数据源
     * @return 返回的值
     */
    public static <K, V> V getFirstNotNull(Map<K, V> map) {
        V obj = null;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            obj = entry.getValue();
            if (obj != null) {
                break;
            }
        }
        return obj;
    }

    public static Object nullToDef(Map datum, String key, String def) {
        if (datum.get(key) instanceof JSONNull) {
            return def;
        } else if (datum.get(key) instanceof String) {
            return datum.get(key) == null || "null".equals(datum.get(key)) ? def : StrUtil.toString(datum.get(key));
        }
        return datum.get(key);
    }

    public static Object nullToDef(Object dataum, String def) {
        if (dataum instanceof JSONNull) {
            return def;
        } else if (dataum instanceof String) {
            return dataum == null || "null".equals(dataum) ? def : StrUtil.toString(dataum);
        }
        return dataum;
    }
    /**
     * 获取map中的指定字段
     * 1.将null转为""
     */
    public static Map extractMap(Map source, String keys) {
        String[] keysArr = StrUtil.split(keys, ",");
        Map m = new HashMap();
        for (String key : keysArr) {
            m.put(key, JobUtil.nullToDef(source, key, ""));
        }
        return m;
    }
}
