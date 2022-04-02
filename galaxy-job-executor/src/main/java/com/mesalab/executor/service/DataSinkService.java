package com.mesalab.executor.service;

import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.core.utils.DBUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.zdjizhi.utils.DateUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: zhq
 * @create: 2022-03-30`
 **/
@Service
public class DataSinkService {
    private Log logger = Log.get();

    public void adapt(Map<String, List> transformResult, Map<String, String> sinkParams) throws Exception {
        String type = sinkParams.get("type");
        //1. 文件类型 2. 选择处理工具 3. 转为格式数据
        if ("mariadb".equals(type) || "mysql".equals(type)) {
            mysqlSink(JobUtil.getFirstNotNull(transformResult), sinkParams);
        } else if ("file".equals(type)) {
            fileSink(JobUtil.getFirstNotNull(transformResult), sinkParams);
        }
    }

    public void fileSink(List<Map> transformResult, Map<String, String> sinkParams) throws Exception {
        String fileType = sinkParams.get("fileType");
        String path = sinkParams.get("path");
        String prefix = sinkParams.get("prefix");
        String date = DateUtils.getCurrentDate("yyyyMMdd");
        String filePath = StrUtil.concat(false, path, prefix, "_", date);
        if ("json".equals(fileType)) {
            FileWriter fileWriter = new FileWriter(filePath.concat(".json"));
            File file = fileWriter.writeLines(transformResult);
            logger.info("download json file to path {}", file.getAbsolutePath());
        } else if ("csv".equals(fileType)) {
            File file = JobUtil.createCsvFile(transformResult, filePath.concat(".json"));
            logger.info("download csv file to path {}", file.getAbsolutePath());
        }
    }

    /**
     * @return
     */
    public void mysqlSink(List<Map> transformResult, Map<String, String> sinkParams) throws Exception {
        exec(transformResult, sinkParams);
    }

    /**
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public void exec(List<Map> transformResult, Map<String, String> sinkParams) throws Exception {
        Connection conn = null;
        DBUtils dbUtils = null;
        PreparedStatement pst = null;
        try {
            String tableName = sinkParams.get("table");
            dbUtils = new DBUtils();
            //创建数据库连接库对象
            conn = dbUtils.getDBConn(sinkParams);
            String sql = dbUtils.getInsertSql(transformResult, tableName);

            long start = System.currentTimeMillis();
            conn.setAutoCommit(false);
            pst = conn.prepareStatement(sql);
            int count = 0;
            int index = 1;
            for (int i = 1; i < transformResult.size(); i++) {
                for (Object val : transformResult.get(i - 1).values()) {
                    if (val instanceof Long) {
                        pst.setLong((index++), Long.valueOf(StrUtil.toString(val)));
                    } else {
                        pst.setString((index++), StrUtil.toString(val));
                    }
                }
                index = 1;
                pst.addBatch();
                //1w提交一次
                if (i % 10000 == 0) {
                    ++count;
                    pst.executeBatch();
                    conn.commit();
                    pst.clearBatch();
                }
            }
            int[] ints = pst.executeBatch();
            conn.commit();
            logger.info("花费的时间:" + (System.currentTimeMillis() - start) + "ms");
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            pst.close();
            conn.close();
            dbUtils.close();
        }
    }


}
