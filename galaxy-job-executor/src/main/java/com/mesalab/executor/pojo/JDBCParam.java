package com.mesalab.executor.pojo;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: zhq
 * @create: 2022-04-02
 **/
@Data
@Builder
@NoArgsConstructor
public class JDBCParam {

    private String type;
    private String ip;
    private String database;
    private String table;
    private String username;
    private String pin;
    private String sql;
    private Integer batch;

    public JDBCParam(Map<String, String> sourceParams) {

        JDBCParam jdbcParam = BeanUtil.fillBeanWithMap(sourceParams, new JDBCParam(), false);
        this.type = jdbcParam.getType();
        this.ip = jdbcParam.getIp();
        this.sql = jdbcParam.getSql();
        this.database = jdbcParam.getDatabase();
        this.table = StrUtil.isEmpty(table) ? getTableName(sql) : table;
        this.username = jdbcParam.getUsername();
        this.pin = jdbcParam.getPin();
        this.batch = jdbcParam.getBatch();
    }

    public JDBCParam(String type, String ip, String database, String table, String username, String pin, String sql,Integer batch) {
        this.type = type;
        this.ip = ip;
        this.sql = sql;
        this.database = database;
        this.table = StrUtil.isEmpty(table) ? getTableName(sql) : table;
        this.username = username;
        this.pin = pin;
        this.batch = batch;
    }

    public String getTableName(String sql) {
        try {
            if (StrUtil.isNotEmpty(sql)) {
                CCJSqlParserManager parserManager = new CCJSqlParserManager();
                Select select = (Select) parserManager.parse(new StringReader(sql));
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(select);
                return tableList.get(0);
            }
        } catch (JSQLParserException e) {
            return null;
        }
        return null;
    }
}
