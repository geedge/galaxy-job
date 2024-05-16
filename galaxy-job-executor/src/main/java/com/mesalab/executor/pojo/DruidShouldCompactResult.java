package com.mesalab.executor.pojo;

public class DruidShouldCompactResult {
    final public boolean shouldCompact;
    final public String reason;
    final public long rowCount;
    final public long byteSize;
    final public int fileCount;
    final public int maxNumConcurrentSubTasks;

    public DruidShouldCompactResult(boolean shouldCompact, String reason, long rowCount, long byteSize, int fileCount, int maxNumConcurrentSubTasks) {
        this.shouldCompact = shouldCompact;
        this.reason = reason;
        this.rowCount = rowCount;
        this.byteSize = byteSize;
        this.fileCount = fileCount;
        this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    }

    public static DruidShouldCompactResult should(String reason, long rowCount, long byteSize, int fileCount, int maxNumConcurrentSubTasks){
        return new DruidShouldCompactResult(true, reason, rowCount, byteSize, fileCount, maxNumConcurrentSubTasks);
    }

    public static DruidShouldCompactResult notShould(String reason, long rowCount, long byteSize, int fileCount, int maxNumConcurrentSubTasks){
        return new DruidShouldCompactResult(false, reason, rowCount, byteSize, fileCount, maxNumConcurrentSubTasks);
    }

    public static DruidShouldCompactResult notShould(String reason){
        return new DruidShouldCompactResult(false, reason, 0, 0, 0, 1);
    }

    @Override
    public String toString() {
        return String.format("shouldCompact:%s(%s), rowCount:%d, byteSize:%d(%.2fG), fileCount:%d, maxNumConcurrentSubTasks:%d",
                shouldCompact, reason, rowCount, byteSize, byteSize/(double)(1 << 30), fileCount, maxNumConcurrentSubTasks
                );
    }
}
