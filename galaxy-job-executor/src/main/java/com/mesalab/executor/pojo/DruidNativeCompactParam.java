package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Author pengfeixu
 * @ Date 2021/7/8
 * @ Description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidNativeCompactParam {
    private String dataSource;
    private String segmentGranularity;
    private int numOfDay;//输入压缩范围天数，如：输入3，起始时间为2021-07-05，结束时间为2021-07-08，格式为2021-07-05/2021-07-08。
}
