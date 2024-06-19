package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: zhq
 * @CreateDate: 2024/6/13
 * @Version: 1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SysStorageEvent {

    private Long id;
    private String logType;
    private String dataCenter;
    private Long totalAllocatedSize;
    private Long usedSize;
    private Long bytes;
    private Long sinceTime;
    private Long generatedTime;

}
