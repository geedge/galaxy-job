package com.mesalab.executor.core.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.pojo.JDBCParam;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DBUtils {

    private static Log logger = Log.get();
//    private static HikariDataSource hikariDataSource;

    public static Connection getDBConn(JDBCParam jdbcParam) throws SQLException {
        String url = "jdbc:mysql://" + jdbcParam.getIp() + ":3306/" + jdbcParam.getDatabase() + "?useUnicode=true&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false&transformedBitIsBoolean=false";
//        HikariConfig config = new HikariConfig();
//        config.setJdbcUrl(url);
//        config.setUsername(jdbcParam.getUsername());
//        config.setPassword(jdbcParam.getPin());
//
//        config.setConnectionTestQuery("SELECT 1");
//        config.setMinimumIdle(1);
//        config.setMaximumPoolSize(10);
//        config.setConnectionTimeout(10000);
//        config.setIdleTimeout(60000);
//        config.setMaxLifetime(300000);
//
//        hikariDataSource = new HikariDataSource(config);

//        Connection connection = hikariDataSource.getConnection();
        Connection connection = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, jdbcParam.getUsername(), jdbcParam.getPin());
            logger.info("create dataSource connection success");
        } catch (ClassNotFoundException e) {
            logger.error(e);
        }

        return connection;

    }
    public void close() {
//        hikariDataSource.close();
    }

    /**
     * @param transformResult
     * @param tableName
     */
    public String getInsertSql(List<Map> transformResult, String tableName) {
        String sql = "";
        String sqlStr1 = "INSERT IGNORE INTO " + tableName + " (";
        String sqlStr2 = ") VALUES (";
        String sqlStr3 = ")";
        String sqlKey = "";
        String sqlValue = "";
        for (Object key : transformResult.get(0).keySet()) {
            sqlKey += key + ",";
            sqlValue += "?,";
        }
        sqlKey = sqlKey.substring(0, sqlKey.length() - 1);
        sqlValue = sqlValue.substring(0, sqlValue.length() - 1);
        sql = StrUtil.concat(true, sqlStr1, sqlKey, sqlStr2, sqlValue, sqlStr3);
        logger.info(" get insert sql：" + sql);
        return sql;
    }

    /**
     * update table set a=? where id=1;
     */
    public String getUpdateSql(List<Map> transformResult, String tableName) {
        String sqlKV = "";
        for (Object key : transformResult.get(0).keySet()) {
            if (!"id".equals(key)) {
                sqlKV += key + "=?,";
            }
        }
        sqlKV = sqlKV.substring(0, sqlKV.length() - 1);
        String sql = StrUtil.join(" ", "UPDATE", tableName, "SET", sqlKV, "WHERE id=?");
        logger.info(" get update sql: " + sql);
        return sql;
    }
}