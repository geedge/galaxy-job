package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.core.util.ShardingUtil;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


@Component
public class TroubleshootingJob {

    private static Logger logger = LoggerFactory.getLogger(TroubleshootingJob.class);

    @XxlJob("commandJobHandler")
    public ReturnT<String> commandJobHandler(String param) throws Exception {
        String command = param;
        int exitValue = -1;

        BufferedReader bufferedReader = null;
        try {
            // command process
            Process process = Runtime.getRuntime().exec(command);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(process.getInputStream());
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream));

            // command log
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                XxlJobLogger.log(line);
            }

            // command exit
            process.waitFor();
            exitValue = process.exitValue();
        } catch (Exception e) {
            XxlJobLogger.log(e);
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }

        if (exitValue == 0) {
            return IJobHandler.SUCCESS;
        } else {
            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "command exit value("+exitValue+") is failed");
        }
    }


    @XxlJob("httpJobHandler")
    public ReturnT<String> httpJobHandler(String param) {

      try {
          String result =  HttpClientUtils.httpGet(param);
          if (result.equals("-1")) {
              return ReturnT.FAIL;
          } else {
              return new ReturnT<String>(IJobHandler.SUCCESS.getCode(), result);
          }
      } catch (BusinessException e) {
          XxlJobLogger.log(e);
          return new ReturnT<String>(IJobHandler.FAIL.getCode(), e.getMessage());
      }




    }

    /**
     * 生命周期任务测试：任务初始化与销毁时，支持自定义相关逻辑；
     * @param param
     * @return
     * @throws Exception
     */
    @XxlJob(value = "demoJobHandler", init = "init", destroy = "destroy")
    public ReturnT<String> demoJobHandler(String param) throws Exception {
        XxlJobLogger.log("XXL-JOB, Hello World.");
        return ReturnT.SUCCESS;
    }

    /**
     * 分片广播任务测试
     * @param param
     * @return
     * @throws Exception
     */
    @XxlJob("shardingJobHandler")
    public ReturnT<String> shardingJobHandler(String param) throws Exception {

        // 分片参数
        ShardingUtil.ShardingVO shardingVO = ShardingUtil.getShardingVo();
        XxlJobLogger.log("分片参数：当前分片序号 = {}, 总分片数 = {}", shardingVO.getIndex(), shardingVO.getTotal());

        // 业务逻辑
        for (int i = 0; i < shardingVO.getTotal(); i++) {
            if (i == shardingVO.getIndex()) {
                XxlJobLogger.log("第 {} 片, 命中分片开始处理", i);
            } else {
                XxlJobLogger.log("第 {} 片, 忽略", i);
            }
        }

        return ReturnT.SUCCESS;
    }




    public void init(){
        logger.info("init");
    }
    public void destroy(){
        logger.info("destory");
    }







}
