package com.mesalab.executor.core.utils;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.pojo.JDBCParam;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class DBUtils {

    private static Log logger = Log.get();
    private static HikariDataSource hikariDataSource;
    private static String dbParams = "?useUnicode=true&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&tinyInt1isBit=false&transformedBitIsBoolean=false";
    private final static int batchSize = 10000;

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
    public static String getInsertSql(List<Map> transformResult, String tableName) {
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
    public static String getUpdateSql(List<Map> transformResult, String tableName, String[]... conditions) {
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

    /**
     * 构建 INSERT INTO ... ON DUPLICATE KEY UPDATE
     *
     * @param transformResult
     * @param tableName
     * @return
     */
    public static String getInsertOrUpdateSql(List<Map> transformResult, String tableName) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        StringBuilder updateStatements = new StringBuilder();
        Map<String, Object> data = transformResult.get(0);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey();
            if (!"id".equalsIgnoreCase(column)) {
                columns.append(column).append(",");
                values.append("?,");
                updateStatements.append(column).append("=VALUES(").append(column).append("),");
            }
        }
        columns.setLength(columns.length() - 1);
        values.setLength(values.length() - 1);
        updateStatements.setLength(updateStatements.length() - 1);
        String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES(" + values + ") ON DUPLICATE KEY UPDATE " + updateStatements;
        logger.info(" get insert or update sql: " + sql);
        return sql;

    }


    public void saveByMap(List<Map> dataList, JDBCParam jdbcParam) throws Exception {
        if (ObjectUtil.isEmpty(dataList)) {
            logger.info("save data is empty ");
            return;
        }
        Connection conn = null;
        PreparedStatement pst = null;
        try {
            String tableName = jdbcParam.getTable();
            conn = DBUtils.getDBConn(jdbcParam);
            long start = System.currentTimeMillis();
            conn.setAutoCommit(false);
            pst = conn.prepareStatement(getInsertSql(dataList, tableName));
            int count = 0;
            int index = 1;
            for (int i = 1; i <= dataList.size(); i++) {
                for (Object val : dataList.get(i - 1).values()) {
                    index = setPstVal(pst, index, val);
                }
                index = 1;
                pst.addBatch();
                //批量提交
                if (i % batchSize == 0) {
                    int[] ints = pst.executeBatch();
                    count = count + ints.length;
                    conn.commit();
                    pst.clearBatch();
                }
            }
            int[] ints = pst.executeBatch();
            count = count + ints.length;
            conn.commit();
            JobUtil.infoLog("save mysql table {}, count {}, take {},", tableName, count, (System.currentTimeMillis() - start) + "ms");
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

    private int setPstVal(PreparedStatement pst, int index, Object val) throws SQLException {
        if (val instanceof String) {
            if (val == null) {
                pst.setNull(index++, Types.VARCHAR);
            } else {
                pst.setString((index++), StrUtil.toString(val));
            }
        } else if (val instanceof Integer) {
            if (val == null) {
                pst.setNull(index++, Types.INTEGER);
            } else {
                pst.setInt((index++), Integer.valueOf(StrUtil.toString(val)));
            }
        } else if (val instanceof Long) {
            if (val == null) {
                pst.setNull(index++, Types.BIGINT);
            } else {
                pst.setLong((index++), Long.valueOf(StrUtil.toString(val)));
            }
        } else if (val instanceof Boolean) {
            if (val == null) {
                pst.setNull(index++, Types.BOOLEAN);
            } else {
                pst.setBoolean((index++), Boolean.valueOf(StrUtil.toString(val)));
            }
        } else {
            pst.setObject((index++), val);
        }
        return index;
    }

    public static <T> void save(List<T> dataList, JDBCParam jdbcParam) throws Exception {
        if (dataList == null || dataList.isEmpty()) {
            logger.info("Data list is empty, nothing to save.");
            return;
        }
        try (Connection conn = DBUtils.getDBConn(jdbcParam)) {
            conn.setAutoCommit(false);
            String sql = prepareInsertSql(dataList.get(0), jdbcParam.getTable());
            try (PreparedStatement pst = conn.prepareStatement(sql)) {
                int count = executeBatchInsert(dataList, pst);
                conn.commit();
                logger.info("Saved {} records to table {} successfully.", count, jdbcParam.getTable());
            } catch (SQLException e) {
                conn.rollback();
                logger.error("Error executing batch insert, transaction rolled back.", e);
                throw e;
            }
        } catch (Exception e) {
            logger.error("Failed to save data.", e);
            throw e;
        }
    }

    private static <T> int executeBatchInsert(List<T> dataList, PreparedStatement pst) throws IllegalAccessException, SQLException {
        int count = 0;
        for (int i = 0; i < dataList.size(); i++) {
            T item = dataList.get(i);
            setPreparedStatementParameters(pst, item);
            pst.addBatch();
            if ((i + 1) % batchSize == 0 || i == dataList.size() - 1) {
                count += executeAndClearBatch(pst);
            }
        }
        return count;
    }

    private static <T> void setPreparedStatementParameters(PreparedStatement pst, T item) throws IllegalAccessException, SQLException {
        Field[] fields = item.getClass().getDeclaredFields();
        for (int j = 0; j < fields.length; j++) {
            fields[j].setAccessible(true);
            pst.setObject(j + 1, fields[j].get(item));
        }
    }

    private static int executeAndClearBatch(PreparedStatement pst) throws SQLException {
        int[] results = pst.executeBatch();
        pst.clearBatch();
        return results.length;
    }

    private static <T> String prepareInsertSql(T item, String tableName) {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        Field[] fields = item.getClass().getDeclaredFields();
        for (Field field : fields) {
            columns.append( StrUtil.toUnderlineCase(field.getName())).append(",");
            placeholders.append("?,");
        }
        columns.setLength(columns.length() - 1);
        placeholders.setLength(placeholders.length() - 1);
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

}
