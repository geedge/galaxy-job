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
public class ColumnParam {

    private String name;
    private String type;
    private String ttl;


}
