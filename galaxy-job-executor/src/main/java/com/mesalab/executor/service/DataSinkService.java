package com.mesalab.executor.service;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.pojo.JDBCParam;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.mesalab.executor.core.utils.Constant.*;

/**
 * @description:
 * @author: zhq
 * @create: 2022-03-30`
 **/
@Service
public class DataSinkService {
    private Log logger = Log.get();

    public void adapt(Map<String, Object> transformResult, Map<String, String> sinkParams) throws Exception {
        if (ObjectUtil.isEmpty(sinkParams.get("type"))) {
            return;
        }
        //1. 文件类型 2. 选择处理工具 3. 转为格式数据
        switch (sinkParams.get("type")) {
            case MARIADB:
                mysqlSink(transformResult, sinkParams);
                break;
            case MYSQL:
                mysqlSink(transformResult, sinkParams);
                break;
            case FILE:
                fileSink(JobUtil.getFirstNotNull(transformResult), sinkParams);
                break;
            default:
                break;
        }
    }


    public void fileSink(Object transformResult, Map<String, String> sinkParams) throws Exception {
        if (ObjectUtil.isEmpty(transformResult)) {
            logger.info("file sink data is empty ");
            XxlJobLogger.log("file sink data is empty ");
            return;
        }
        String[] fileTypes = StrUtil.split(sinkParams.get("fileType"), ",");
        String path = sinkParams.get("path");
        String prefix = sinkParams.get("prefix");
        String date = "";
        if (StringUtils.isNotBlank(sinkParams.get("datePattern"))) {
            date = "_".concat(DateUtils.getCurrentDate("yyyyMMdd"));
        }
        String filePath = StrUtil.concat(false, path, prefix, date);
        for (String fileType : fileTypes) {
            if ("json".equals(fileType)) {
                FileWriter fileWriter = new FileWriter(filePath.concat(".json"));
                File file = fileWriter.write(JSON.toJSONString(transformResult, SerializerFeature.PrettyFormat));
                logger.info("download json data to path {}", file.getAbsolutePath());
                XxlJobLogger.log("download json data to path {}", file.getAbsolutePath());
            } else if ("csv".equals(fileType)) {
                List<Map> transform = (List<Map>) JsonMapper.fromJsonString(JsonMapper.toJsonString(transformResult), List.class);
                File file = JobUtil.createCsvFile(transform, filePath.concat(".csv"));
                logger.info("download {} pieces of csv data to path {}", transform.size(), file.getAbsolutePath());
                XxlJobLogger.log("download {} pieces of csv data to path {}", transform.size(), file.getAbsolutePath());
            }
        }

    }

    /**
     * @return
     */
    public void mysqlSink(Object transformResult, Map<String, String> sinkParams) throws Exception {
        logger.info("mysql sink param {} ", sinkParams);
        Map<String, List> transform = (Map<String, List>) JsonMapper.fromJsonString(JsonMapper.toJsonString(transformResult), Map.class);
        List list = JobUtil.getFirstNotNull(transform);
        if (ObjectUtil.isEmpty(list)) {
            logger.info("mysql sink data is empty ");
            XxlJobLogger.log("mysql sink data is empty ");
            return;
        }
        exec(list, sinkParams);
    }

    /**
     * @throws Exception
     */
    public void exec(List<Map> transformResult, Map<String, String> sinkParams) throws Exception {
        if (ObjectUtil.isEmpty(transformResult)) {
            logger.info("sink data is empty ");
            return;
        }
        Connection conn = null;
        DBUtils dbUtils = null;
        PreparedStatement pst = null;
        try {

            JDBCParam jdbcParam = new JDBCParam(sinkParams);
            String tableName = jdbcParam.getTable();
            dbUtils = new DBUtils();
            //创建数据库连接库对象
            conn = dbUtils.getDBConn(jdbcParam);
            String sql = "";
            if ("update".equals(sinkParams.get("option"))) {
                sql = dbUtils.getUpdateSql(transformResult, tableName);
            } else {
                sql = dbUtils.getInsertSql(transformResult, tableName);
            }
            long start = System.currentTimeMillis();
            conn.setAutoCommit(false);
            pst = conn.prepareStatement(sql);
            int count = 0;
            int index = 1;
            for (int i = 1; i <= transformResult.size(); i++) {
                for (Object val : transformResult.get(i - 1).values()) {
                    if (val instanceof Long) {
                        pst.setLong((index++), Long.valueOf(StrUtil.toString(val)));
                    } else if (val instanceof Integer) {
                        pst.setInt((index++), Integer.valueOf(StrUtil.toString(val)));
                    } else if (val instanceof Boolean) {
                        pst.setBoolean((index++), Boolean.valueOf(StrUtil.toString(val)));
                    } else {
                        pst.setString((index++), StrUtil.toString(val));
                    }
                }
                index = 1;
                pst.addBatch();
                //1w提交一次
                if (i % 10000 == 0) {
                    int[] ints = pst.executeBatch();
                    count = count + ints.length;
                    conn.commit();
                    pst.clearBatch();
                }
            }
            int[] ints = pst.executeBatch();
            count = count + ints.length;
            conn.commit();
            logger.info("{} table {} , count {} ,  take {}", sinkParams.get("option"), tableName, count, (System.currentTimeMillis() - start) + "ms");
            XxlJobLogger.log("{} table {} , count {} ,  take {}", sinkParams.get("option"), tableName, count, (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    logger.error(e1);
                }
            }
            logger.error(e);
            throw e;
        } finally {
            IoUtil.close(pst);
            IoUtil.close(conn);
        }
    }

}
