package com.mesalab.executor.service;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Paths;
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

    public Map<String, List> adapt(Map<String, String> sourceParams) throws Exception{
        String type = sourceParams.get("type");
        //1. 文件类型 2. 选择处理工具 3. 转为格式数据
        if ("file".equals(type)) {
            return fileSource(sourceParams);
        }
        return null;
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
