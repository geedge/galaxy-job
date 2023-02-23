package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ Author xu
 * @ Date 2023/2/23
 * @ Description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DruidNativeMergeParam {
    private String dataSource;
    private String segmentGranularity;
    private String queryGranularity;
//    private int numOfDay;
    private String internalOrNumOfHistory;
    //internal两种方式
    //1）如：输入"3"，起始时间为2021-07-05，结束时间为2021-07-08，格式为2021-07-05/2021-07-08。
    //2）"2021-06-18/2021-09-18"
}
