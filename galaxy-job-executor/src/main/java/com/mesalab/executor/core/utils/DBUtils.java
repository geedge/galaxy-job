package com.mesalab.executor.core.utils;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.pojo.JDBCParam;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.util.ObjectUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DBUtils {

    private static Log logger = Log.get();
    private static HikariDataSource hikariDataSource;
    private static String dbParams = "?useUnicode=true&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false&transformedBitIsBoolean=false";

    public static Connection getDBConn(JDBCParam jdbcParam) throws SQLException {
        String url = "jdbc:mysql://" + jdbcParam.getIp() + ":3306/" + jdbcParam.getDatabase() + dbParams;
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

    public synchronized static Connection getConnPool(JDBCParam jdbcParam) throws SQLException {
        String url = "jdbc:mysql://" + jdbcParam.getIp() + ":3306/" + jdbcParam.getDatabase() + dbParams;
        if (hikariDataSource == null || isDataSourceChanged(url, jdbcParam.getUsername(), jdbcParam.getPin())) {
            // 关闭旧的数据源
            if (hikariDataSource != null) {
                hikariDataSource.close();
                IoUtil.close(hikariDataSource);
            }
            // 创建新的数据源
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(jdbcParam.getUsername());
            config.setPassword(jdbcParam.getPin());
            config.setMaximumPoolSize(10);
            config.setConnectionTimeout(10000);
            config.setIdleTimeout(60000);
            config.setMaxLifetime(300000);
            hikariDataSource = new HikariDataSource(config);
        }

        return hikariDataSource.getConnection();
    }

    private static boolean isDataSourceChanged(String url, String username, String password) {
        return !hikariDataSource.getJdbcUrl().equals(url)
                || !hikariDataSource.getUsername().equals(username)
                || !hikariDataSource.getPassword().equals(password);
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

    /**
     * update table set a=? where conditions;
     */
    public String getUpdateSql(List<Map> transformResult, String tableName, String[]... conditions) {
        String sqlKV = "";
        for (Object key : transformResult.get(0).keySet()) {
            if (!"id".equals(key)) {
                sqlKV += key + "= ?,";
            }
        }
        sqlKV = sqlKV.substring(0, sqlKV.length() - 1);
        String sql = StrUtil.join(" ", "UPDATE", tableName, "SET", sqlKV);
        String whereSql = " WHERE ";
        if (ObjectUtils.isEmpty(conditions)) {
            whereSql = whereSql.concat("id=?");
        } else {
            whereSql = whereSql.concat(StrUtil.join("=? AND ", conditions).concat("=? "));
        }
        sql = sql.concat(whereSql);
        logger.info(" get update sql: " + sql);
        return sql;
    }
}