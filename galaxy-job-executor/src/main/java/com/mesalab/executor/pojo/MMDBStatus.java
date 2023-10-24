package com.mesalab.executor.pojo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class MMDBStatus {

    private Integer status;
    private String message;
    private Long updateTime;
    private List<String> ipSample = new ArrayList<>();

}
