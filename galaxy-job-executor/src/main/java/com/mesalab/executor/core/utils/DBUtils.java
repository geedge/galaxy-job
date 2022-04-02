package com.mesalab.executor.core.utils;

import cn.hutool.log.Log;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DBUtils {

    private Log logger = Log.get();
    private static HikariDataSource hikariDataSource;


    public Connection getDBConn(Map<String, String> sinkParams) throws SQLException {
        String ip = sinkParams.get("ip");
        String database = sinkParams.get("database");
        String username = sinkParams.get("username");
        String pin = sinkParams.get("pin");
        String url = "jdbc:mysql://" + ip + ":3306/" + database + "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&serverTimezone=Asia/Shanghai";
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(pin);

        config.setConnectionTestQuery("SELECT 1");
        config.setMinimumIdle(2);
        config.setMaximumPoolSize(20);
        config.setConnectionTimeout(10000);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(3000000);

        hikariDataSource = new HikariDataSource(config);

        Connection connection = hikariDataSource.getConnection();
        logger.info("create dataSource connection success");
        return connection;

    }

    public void close() {
        hikariDataSource.close();
    }

    /**
     * @param transformResult
     * @param tableName
     */
    public String getInsertSql(List<Map> transformResult, String tableName) {
        String sql = "";
        String sqlStr1 = "INSERT INTO " + tableName + " (";
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
        sql = sqlStr1 + sqlKey + sqlStr2 + sqlValue + sqlStr3;
        logger.info(" get insert SQL：" + sql);
        return sql;
    }
}