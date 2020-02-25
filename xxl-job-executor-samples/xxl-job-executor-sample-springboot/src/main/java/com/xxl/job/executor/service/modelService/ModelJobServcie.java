package com.xxl.job.executor.service.modelService;

public interface ModelJobServcie {
    public Object queryClickHouse(String querySql);

    public void pushToKafka(String topic, String message);
}
