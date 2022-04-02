package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: zhq
 * @create: 2022-04-02
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DBParam {

    private String type;
    private String ip;
    private String database;
    private String table;
    private String username;
    private String pin;
    private String sql;

}
