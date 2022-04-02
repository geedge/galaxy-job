package com.mesalab.executor.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.sql.SqlExecutor;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.pojo.DBParam;
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

    public Map<String, List> adapt(Map<String, String> sourceParams) throws Exception {
        String type = sourceParams.get("type");
        //1. 文件类型 2. 选择处理工具 3. 转为格式数据
        if ("file".equals(type)) {
            return fileSource(sourceParams);
        } else if ("mysql".equals(type) || "mariadb".equals(type)) {
            return MapUtil.of("mysql", mysqlSource(sourceParams));
        }
        return null;
    }

    private List<Map> mysqlSource(Map<String, String> sourceParams) throws Exception {
        DBParam dbParam = BeanUtil.fillBeanWithMap(sourceParams, new DBParam(), false);
        DBUtils dbUtils = new DBUtils();
        Connection conn = dbUtils.getDBConn(sourceParams);
        String sql = dbParam.getSql();
        List<Entity> entityList = SqlExecutor.query(conn, sql, new EntityListHandler());
        logger.info("query result size {}", entityList.size());
        List<Map> collect = entityList.stream().map(x -> BeanUtil.beanToMap(x)).collect(Collectors.toList());
        DbUtil.close(conn);
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
    public Map<String, List> fileSource(Map<String, String> params) throws Exception {
        //解析参数
        String path = params.get("path");
        String fileTypeParams = params.get("fileType");
        String[] fileTypes = StrUtil.split(fileTypeParams, ",");
        //处理数据
        Map<String, List> typeResult = new HashMap();
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
