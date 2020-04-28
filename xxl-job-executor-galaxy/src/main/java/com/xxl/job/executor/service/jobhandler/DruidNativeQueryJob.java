package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.Exception.BusinessException;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.KafkaUtils;
import com.xxl.job.executor.pojo.DruidNativeParam;
import com.xxl.job.executor.pojo.DruidNativeQueryResultParam;
import com.xxl.job.executor.pojo.DruidNativeQueryToKafkaParam;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import io.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
public class DruidNativeQueryJob {
    private static Logger logger = LoggerFactory.getLogger(DruidNativeQueryJob.class);

    /**
     * 调度模型:httpDruidNativeQueryToKafka
     * source:druid native query
     * sink:kafka
     * params 参数定义如下：
     * {
     *     url:<Http druidNative url>, // 查询url http://192.168.40.224:8082/druid/v2?pretty
     *     method:[request method], // get|post 可选，默认get
     *     requestBody:{query json}, //druid native查询语句
     *     topic:<topicName>  // topic名称
     * }
     * @param params Json 结构
     * @return
     */
    @XxlJob("druidNativeQueryToKafkaJobHandler")
    public ReturnT<String> druidNativeQueryToKafkaJobHandler(String params){
        DruidNativeQueryToKafkaParam druidNativeQueryToKafkaParam = parseDruidNativeToKafkaParam(params);

        if (StringUtil.isEmpty(druidNativeQueryToKafkaParam)) {
            logger.error("params parser error , params is {}",params);
            return IJobHandler.FAIL;
        }

        try {
            String resultMessage;

            if (HttpMethod.POST.name().equalsIgnoreCase(druidNativeQueryToKafkaParam.getMethod())) {
                resultMessage = HttpClientUtils.httpPost(druidNativeQueryToKafkaParam.getUrl(), JsonMapper.toJsonString(druidNativeQueryToKafkaParam.getRequestBody()));
            }  else {
                return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Not support Method:" + druidNativeQueryToKafkaParam.getMethod());
            }

            if (resultMessage.equals("-1")) {
                return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Http Client connection Error!");
            }

            new KafkaUtils().sendMessage(druidNativeQueryToKafkaParam.getTopic(), formatDruidNativeQueryResult(resultMessage));

            XxlJobLogger.log("Send message to kafka, topic is {}", druidNativeQueryToKafkaParam.getTopic());

            return ReturnT.SUCCESS;

        } catch (BusinessException be) {
            logger.error(be.getErrorMessage());
            XxlJobLogger.log(be.getErrorMessage());
            return new ReturnT<String>(be.getErrorCode(), be.getErrorMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e.getMessage());
            return new ReturnT<String>(IJobHandler.FAIL.getCode(), e.getMessage());
        }
    }

    /**
     * 参数验证，时间替换
     * @param params
     * @return
     */
    private DruidNativeQueryToKafkaParam parseDruidNativeToKafkaParam(String params){
        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        Map<String,Object> druidNativeToKafkaParamObject = (Map<String,Object>) JsonMapper.fromJsonString(params, Object.class);
        if(StringUtil.isEmpty(druidNativeToKafkaParamObject)) {
            XxlJobLogger.log("params is not format json ! ");
            return null;
        }
        if (StringUtil.isBlank(druidNativeToKafkaParamObject.get("url").toString())) {
            XxlJobLogger.log("url is empty ! ");
            return null;
        }
        if(StringUtil.isBlank(druidNativeToKafkaParamObject.get("topic").toString())) {
            XxlJobLogger.log("kafka topic is empty ! ");
            return null;
        }

        if (StringUtil.isBlank(druidNativeToKafkaParamObject.get("method").toString())) {
            XxlJobLogger.log("this request method is not supported");
        }

        if (StringUtil.isBlank(druidNativeToKafkaParamObject.get("requestBody").toString())){
            XxlJobLogger.log("druid native query request body is empty ! ");
            return null;
        }
        String queryRequestJson = JsonMapper.toJsonString(druidNativeToKafkaParamObject.get("requestBody"));
        DruidNativeParam druidNativeParam = (DruidNativeParam)JsonMapper.fromJsonString(queryRequestJson, DruidNativeParam.class);
        druidNativeParam.setIntervals(getIntervalList());
        druidNativeToKafkaParamObject.put("requestBody",druidNativeParam);
        DruidNativeQueryToKafkaParam druidNativeToKafkaParam =
                (DruidNativeQueryToKafkaParam)JsonMapper.fromJsonString(JsonMapper.toJsonString(druidNativeToKafkaParamObject), DruidNativeQueryToKafkaParam.class);
        return druidNativeToKafkaParam;

    }

    /**
     * 获取本次查询时间范围
     */
    private List<Object> getIntervalList(){
        //获取当前时间，按小时取整，再取24小时间隔
        long currentTime = new Date().getTime();
        long startTime =  currentTime/(60*60*1000) * 60*60*1000 - 60*60*24*1000;
        long endTime = currentTime/(60*60*1000) * 60*60*1000;
        Interval interval = new Interval(startTime,endTime, DateTimeZone.UTC);
        ArrayList<Object> intervalList = new ArrayList<>();
        intervalList.add(interval.toString());
        return intervalList;
    }

    /**
     * 转换最终结果数据
     * @param resultMessage
     * @return
     */
    private List<Object> formatDruidNativeQueryResult(String resultMessage){
        ArrayList<Object> resultLst = new ArrayList<>();
        ArrayList<Object> lst = (ArrayList<Object>)JsonMapper.fromJsonString(resultMessage, Object.class);
        Map<String, Object> resultMap = (Map<String, Object>) lst.get(0);
//        Object timestamp = resultMap.get("timestamp");
        ArrayList<Object> resultRowLst = (ArrayList<Object>) resultMap.get("result");
        String interval = (String)getIntervalList().get(0);
        String statTime = interval.split("/")[1];
        for (Object rowObj: resultRowLst){
            DruidNativeQueryResultParam row = (DruidNativeQueryResultParam)JsonMapper.fromJsonString(JsonMapper.toJsonString(rowObj), DruidNativeQueryResultParam.class);
            row.setStat_time(statTime);
//            row.setStat_time(timestamp.toString());
            resultLst.add(row);
        }
        return resultLst;
    }


}
