package com.mesalab.executor.pojo;

import lombok.Data;

import java.util.List;

@Data
public class MMDBStatus {

    private Integer status;
    private String message;
    private Long updateTime;
    private List<String> ipv4Sample;
    private List<String> ipv6Sample;

}
