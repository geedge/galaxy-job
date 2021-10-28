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
//    private int numOfDay;
    private String internalOrNumOfHistory;
    //internal两种方式
    //1）如：输入"3"，起始时间为2021-07-05，结束时间为2021-07-08，格式为2021-07-05/2021-07-08。
    //2）"2021-06-18/2021-09-18"
}
