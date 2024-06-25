package com.mesalab.executor.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author galaxy
 */
@Data
@NoArgsConstructor
public class JobResult<T> {

    private Integer code = 200;
    private String message;
    private Integer total;
    private Integer failCount;
    private List<T> data;

}
