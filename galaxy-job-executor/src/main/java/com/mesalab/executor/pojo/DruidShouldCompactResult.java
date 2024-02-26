package com.mesalab.executor.pojo;

public class DruidShouldCompactResult {
    final public boolean shouldCompact;
    final public String reason;
    final public long rowCount;
    final public long byteSize;
    final public int fileCount;

    public DruidShouldCompactResult(boolean shouldCompact, String reason, long rowCount, long byteSize, int fileCount) {
        this.shouldCompact = shouldCompact;
        this.reason = reason;
        this.rowCount = rowCount;
        this.byteSize = byteSize;
        this.fileCount = fileCount;
    }

    public static DruidShouldCompactResult should(String reason, long rowCount, long byteSize, int fileCount){
        return new DruidShouldCompactResult(true, reason, rowCount, byteSize, fileCount);
    }

    public static DruidShouldCompactResult notShould(String reason, long rowCount, long byteSize, int fileCount){
        return new DruidShouldCompactResult(false, reason, rowCount, byteSize, fileCount);
    }

    public static DruidShouldCompactResult notShould(String reason){
        return new DruidShouldCompactResult(false, reason, 0, 0, 0);
    }

    @Override
    public String toString() {
        return String.format("shouldCompact:%s(%s), rowCount:%d, byteSize:%d(%.2fG), fileCount:%d",
                shouldCompact, reason, rowCount, byteSize, byteSize/(double)(1 << 30), fileCount
                );
    }
}
