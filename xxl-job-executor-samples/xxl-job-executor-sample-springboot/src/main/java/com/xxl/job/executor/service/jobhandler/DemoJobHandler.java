package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.modelService.ModelJobServcie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class DemoJobHandler {
    @Autowired
    ModelJobServcie modelJobServcie;

    /**
     * 一个固定的任务（Bean模式）
     */
    @XxlJob("demoQueryHandle")
    public  ReturnT<String> executeJob(String param) throws Exception {
      XxlJobLogger.log("execute job ");
      String querySql = "select * from policy_event_log limit 1 FORMAT JSON";
      Object data = modelJobServcie.queryClickHouse(querySql);
      
      modelJobServcie.pushToKafka("test-topic", data.toString());
      return ReturnT.SUCCESS;
    }

    @XxlJob("firstJobHandler")
    public ReturnT<String> execute(String param) throws Exception {
        XxlJobLogger.log("success job");
        System.err.println("hello xxl job");
        return ReturnT.SUCCESS;
    }

    /**
     * 一个任意自定义任务demo（Bean模式）
     */

    public static ReturnT<String> demoJobHandler(String param) throws Exception {
        XxlJobLogger.log("XXL-JOB, Hello World.");
;
        for (int i = 0; i < 5; i++) {
            XxlJobLogger.log("beat at:" + i);
            TimeUnit.SECONDS.sleep(2);
        }

        return ReturnT.SUCCESS;
    }


}
