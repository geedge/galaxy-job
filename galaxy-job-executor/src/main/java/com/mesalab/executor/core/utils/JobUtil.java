package com.mesalab.executor.core.utils;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.CryptoException;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.geedgenetworks.crypt.AESUtil;
import com.geedgenetworks.utils.IpLookupV2;
import com.mesalab.executor.exception.BusinessException;
import com.xxl.job.core.log.XxlJobLogger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class JobUtil {
    private static Log logger = Log.get();

    public static final String DAY_TRUNC_REGEX = "\\$\\{now\\(\\)-\\d+DAY\\}";
    public static final String HOUR_TRUNC_REGEX = "\\$\\{now\\(\\)-\\d+HOUR\\}";
    public static final String NOW_REGEX = "\\$\\{now\\(\\)\\}";
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES";

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

    public static File createCsvFile(List<Map> maps, String filePath, boolean append) {
        File file = FileUtil.newFile(filePath);
        try (CsvWriter writer = CsvUtil.getWriter(file, CharsetUtil.CHARSET_UTF_8, append)) {
            List result = new ArrayList<String[]>();
            if (ObjectUtil.isNotEmpty(maps)) {
                Object[] fieldArr = maps.get(0).keySet().toArray();
                result.add(Arrays.asList(fieldArr));
                if (maps.size() > 1) {
                    maps.forEach(m -> result.add(m.values()));
                }
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
        if (ObjectUtil.isNull(datum.get(key))) {
            return def;
        } else if (datum.get(key) instanceof JSONNull) {
            return def;
        } else if (datum.get(key) instanceof String) {
            return datum.get(key) == null || "null".equals(datum.get(key)) ? def : StrUtil.toString(datum.get(key));
        }
        return datum.get(key);
    }

    public static Object nullToDef(Object dataum, String def) {
        if (ObjectUtil.isNull(dataum)) {
            return def;
        } else if (dataum instanceof JSONNull) {
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
        Map m = new LinkedHashMap();
        for (String key : keysArr) {
            m.put(key, JobUtil.nullToDef(source, key, ""));
        }
        return m;
    }

    public static int executeCommand(String command) throws Exception {
        int exitValue = -1;

        BufferedReader bufferedReader = null;
        try {
            JobUtil.infoLog("command: " + command);
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
            BufferedInputStream bufferedInputStream = new BufferedInputStream(process.getInputStream());
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream));

            XxlJobLogger.log("process running...");
            // command log
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                XxlJobLogger.log(line);
                logger.info(line);
            }
            // command exit
            process.waitFor();
            exitValue = process.exitValue();
        } catch (Exception e) {
            JobUtil.errorLog(e);
            throw e;
        } finally {
            IoUtil.close(bufferedReader);
        }
        return exitValue;
    }

    public static String exeCmd(String command) {
        int exitValue = -1;
        BufferedReader bufferedReader = null;
        StringBuffer sb = new StringBuffer();
        try {
            logger.info("execute command: " + command);
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
            BufferedInputStream bufferedInputStream = new BufferedInputStream(process.getInputStream());
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream));

            XxlJobLogger.log("process running...");
            // command log
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
            // command exit
            process.waitFor();
            process.exitValue();
        } catch (Exception e) {
            logger.error(e);
            XxlJobLogger.log(e);
        } finally {
            IoUtil.close(bufferedReader);
        }
        return sb.toString();
    }

    public static <T> T readJsonFile(String path, Class<T> c) throws Exception {
        FileReader fr = new FileReader(path);
        T t = JSONUtil.toBean(fr.readString(), c);
        return t;
    }

    public static void infoLog(String msg, Object... arguments) {
        logger.info(msg, arguments);
        XxlJobLogger.log(msg, arguments);
    }
    public static void warnLog(String msg, Object... arguments) {
        logger.warn(msg, arguments);
        XxlJobLogger.log(msg, arguments);
    }
    public static void errorLog(String msg, Object... arguments) {
        logger.error(msg, arguments);
        XxlJobLogger.log(msg, arguments);
    }

    public static void errorLog(Throwable throwable) {
        logger.error(throwable);
        XxlJobLogger.log(throwable);
    }

    public static String parseParamsTemplate(String paramString) {
        String format = "yyy-MM-dd'T'HH:mm:ss";
        String result = parseDayParams(paramString, format);
        result = parseHourParams(result, format);
        result = parseNowParams(result, format);
        return result;
    }

    private static String parseNowParams(String result, String format) {
        return ReUtil.replaceAll(result, NOW_REGEX, DateUtil.format(DateUtil.date(), format)
        );
    }

    private static String parseHourParams(String paramString, String format) {
        return ReUtil.replaceAll(paramString, HOUR_TRUNC_REGEX, match -> {
            int hour = ReUtil.getFirstNumber(match.group());
            return DateUtil.format(DateUtil.offsetHour(DateUtil.date(), -hour), format);
        });
    }

    private static String parseDayParams(String paramString, String format) {
        return ReUtil.replaceAll(paramString, DAY_TRUNC_REGEX, match -> {
            int days = ReUtil.getFirstNumber(match.group());
            return DateUtil.format(DateUtil.offsetDay(DateUtil.date(), -days), format);
        });
    }
    public static boolean ipKnowledgeNotEmpty(String builtinPath, String userDefinedPath, List<String> ipSample) {
        IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
                .loadDataFile(builtinPath)
                .loadDataFilePrivate(userDefinedPath)
                .build();
        for (String sampleIp : ipSample) {
            if (ObjectUtil.isEmpty(ipLookup.latLngLookup(sampleIp))) {
                JobUtil.errorLog("ip locate library sample validate fail, sampleIp {}, latLng {}", sampleIp, ipLookup.latLngLookup(sampleIp));
                return false;
            }
        }
        return true;
    }

    public static boolean asnKnowledgeNotEmpty(String builtinPath, String userDefinedPath, List<String> ipSample) {
        IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
                .loadAsnDataFile(builtinPath)
                .loadAsnDataFilePrivate(userDefinedPath)
                .build();
        for (String sampleIp : ipSample) {
            if (ObjectUtil.isEmpty(ipLookup.asnLookup(sampleIp))) {
                JobUtil.infoLog("asn library sample validate fail, sample ip {}, asn {}", sampleIp, ipLookup.asnLookup(sampleIp));
                return false;
            }
        }
        return true;
    }

    public static void doCrypto(int cipherMode, String key, File inputFile, File outputFile) {

        try (FileInputStream inputStream = new FileInputStream(inputFile);
             BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
             FileOutputStream outputStream = new FileOutputStream(outputFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream)) {
            Key secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(cipherMode, secretKey);
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                byte[] outputBytes = cipher.update(buffer, 0, bytesRead);
                if (outputBytes != null) {
                    bufferedOutputStream.write(outputBytes);
                }
            }
            byte[] finalBytes = cipher.doFinal();
            if (finalBytes != null) {
                bufferedOutputStream.write(finalBytes);
            }
        } catch (NoSuchPaddingException | NoSuchAlgorithmException
                | InvalidKeyException | BadPaddingException
                | IllegalBlockSizeException | IOException ex) {
            throw new CryptoException("Error encrypting/decrypting file", ex);
        }
    }

    public static File encrypt(String key, File inputFile, File outputFile) throws Exception {
        byte[] bytes = FileUtil.readBytes(inputFile);
        byte[] encrypt = AESUtil.encrypt(bytes, key);
        File file = FileUtil.writeBytes(encrypt, outputFile);
        return file;
    }

    public static File decrypt(String key, File inputFile, File outputFile) throws Exception {
        byte[] bytes = FileUtil.readBytes(inputFile);
        byte[] decrypt = AESUtil.decrypt(bytes, key);
        File file = FileUtil.writeBytes(decrypt, outputFile);
        return file;

    }

}
