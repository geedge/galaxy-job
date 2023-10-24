package com.mesalab.executor.core.connector;

import com.mesalab.executor.pojo.JDBCParam;

import java.util.stream.Stream;

public class MysqlSource implements Source {

  private JDBCParam jdbcParam;

  @Override
  public Stream<String> fetchData() {

    return null;
  }
}
