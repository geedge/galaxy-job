package com.mesalab.executor.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.StreamProgress;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.DBParam;
import com.mesalab.executor.pojo.HttpParam;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.JsonMapper;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: zhq
 * @create: 2022-03-30`
 **/
@Service
public class DataSourceService {

    private Log logger = Log.get();

    public Map<String, Object> adapt(Map<String, String> sourceParams) throws Exception {
        String type = sourceParams.get("type");
        //1. 文件类型 2. 选择处理工具 3. 转为格式数据
        Map<String, Object> sourceResult = new HashMap<>();
        switch (type) {
            case "file":
                sourceResult = fileSource(sourceParams);
                break;
            case "mysql":
                sourceResult.put("mysql", mysqlSource(sourceParams));
                break;
            case "mariadb":
                sourceResult.put("mariadb", mysqlSource(sourceParams));
                break;
            case "spider":
                sourceResult.put("spider", spider(sourceParams));
                break;
            case "http":
                sourceResult.put("http", http(sourceParams));
                break;
            default:
                break;
        }
        return sourceResult;
    }

    /**
     * 网址数据源
     *
     * @param sourceParams url,method,requestBody,resultKey,tableName
     * @return
     */
    private Map http(Map<String, String> sourceParams) throws Exception {

        String url = String.valueOf(sourceParams.get("url"));
        String method = String.valueOf(sourceParams.get("method"));

        String resultMessage = "";
        if (HttpMethod.POST.name().equalsIgnoreCase(method)) {
            Map requestBody = JSONUtil.toBean(JsonMapper.toJsonString(sourceParams.get("requestBody")), Map.class);
            resultMessage = HttpClientUtils.httpPost(url, JsonMapper.toJsonString(requestBody));
        } else if (HttpMethod.GET.name().equalsIgnoreCase(method)) {
            resultMessage = HttpClientUtils.httpGet(url);
        } else {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Not support Method:" + method);
        }
        if (resultMessage.equals("-1")) {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Http Client connection Error!");
        }
        Map map = (Map) JsonMapper.fromJsonString(resultMessage, Map.class);

        return map;
    }

    private Map spider(Map<String, String> sourceParams) throws Exception {

        logger.info("spider data source starting,param is {}", sourceParams);
        HttpParam httpParam = HttpParam.parserHttpParam(JSONUtil.toJsonStr(sourceParams));

        if (ObjectUtil.isEmpty(httpParam)) {
            throw new BusinessException("params parser error");
        }
        if (HttpMethod.GET.name().equalsIgnoreCase(httpParam.getMethod())) {
            //带进度显示的文件下载
            HttpUtil.downloadFile(httpParam.getUrl(), FileUtil.file(httpParam.getStore()), new StreamProgress() {
                @Override
                public void start() {
                    logger.info("start download . . . ");
                }

                @Override
                public void progress(long progressSize) {
                    logger.info("loading {}", FileUtil.readableFileSize(progressSize));
                }

                @Override
                public void finish() {
                    logger.info("download over !");
                }
            });
        } else {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Not support Method:" + httpParam.getMethod());
        }
        return null;
    }

    public List<Map> mysqlSource(Map<String, String> sourceParams) throws Exception {
        logger.info("mysql data source starting, param is {}", sourceParams);
        DBParam dbParam = BeanUtil.fillBeanWithMap(sourceParams, new DBParam(), false);
        List<Map> collect;
        Connection conn = null;
        DBUtils dbUtils = null;
        try {
            dbUtils = new DBUtils();
            conn = dbUtils.getDBConn(sourceParams);
            String sql = dbParam.getSql();
            List<Entity> entityList = SqlExecutor.query(conn, sql, new EntityListHandler());
            logger.info("query result size {}", entityList.size());
            XxlJobLogger.log("query result size {}", entityList.size());
            collect = entityList.stream().map(x -> BeanUtil.beanToMap(x)).collect(Collectors.toList());
        } finally {
            DbUtil.close(conn);
            dbUtils.close();
        }
        return collect;
    }

    /**
     * v1: 支持 json ,xlsx
     * <p>
     * "type": "file",
     * "path": "/",
     * "fileType": "json,xlsx",
     *
     * @return
     */
    public Map<String, Object> fileSource(Map<String, String> params) throws Exception {
        logger.info("mysql data source starting, param is {}", params);
        //解析参数
        String path = params.get("path");
        String fileTypeParams = params.get("fileType");
        String[] fileTypes = StrUtil.split(fileTypeParams, ",");
        //处理数据
        Map<String, Object> typeResult = new HashMap();
        for (String fileType : fileTypes) {
            if ("json".equals(fileType)) {
                List<File> files = FileUtil.loopFiles(Paths.get(path), 1, FileFilterUtils.suffixFileFilter(fileType));
                List<Map> jsonList = files.stream().map(f -> {
                    FileReader fr = new FileReader(f);
                    Map objMap = JSONUtil.toBean(fr.readString(), Map.class);
                    return objMap;
                }).collect(Collectors.toList());
                typeResult.put(fileType, jsonList);
            } else if ("xlsx".equals(fileType)) {
                List<File> files = FileUtil.loopFiles(Paths.get(path), 1, FileFilterUtils.suffixFileFilter(fileType));
                List<Map<String, List<Map>>> excelList = files.stream().map(f -> {
                    ExcelReader reader = ExcelUtil.getReader(f);
                    List<String> sheetNames = reader.getSheetNames();
                    IoUtil.close(reader);
                    Map<String, List<Map>> sheetMap = new HashMap();
                    for (String sheetName : sheetNames) {
                        ExcelReader rd = ExcelUtil.getReader(f, sheetName);
                        sheetMap.put(sheetName, rd.readAll(Map.class));
                        IoUtil.close(rd);
                    }
                    return sheetMap;
                }).collect(Collectors.toList());
                typeResult.put(fileType, excelList);
            }
        }
        return typeResult;
    }

}
