package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DataflowJob {
    private static Logger logger = LoggerFactory.getLogger(DataflowJob.class);

    @XxlJob("httpToKafkaJobHandler")
    /**
     * params[0]: Http Rest url
     * params[1]: Result BODY key ,Parser Json Data to Records to Kafka
     * params[2]: topicName, kafka topic name
     */
    public ReturnT<String> httpToKafkaJobHandler(String params) {
        if(StringUtil.isBlank(params)
            || params.split(",").length != 4 ) {
            XxlJobLogger.log("params is Empty or params must 4 params!" );
            return IJobHandler.FAIL;
        }

        String[] paramsArray = params.split(",");

        //TODO

        XxlJobLogger.log(paramsArray[0], paramsArray[1], paramsArray[2]);
        return IJobHandler.SUCCESS;
    }





}
