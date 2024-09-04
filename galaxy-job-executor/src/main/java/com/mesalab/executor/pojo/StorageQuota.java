package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Description:
 * @Author: zhq
 * @CreateDate: 2024/9/2
 * @Version: 1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StorageQuota {
    private Integer default_max_days;
    private Integer max_usage;
    private String type;
    private List<String> source_list;
    private Integer max_days;
    private Long total_allocated_size;
    private String since_time;
    private Long used_size;
}
