package com.xxl.job.executor.jobhandler;

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
import org.joda.time.chrono.ISOChronology;
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

//            if (resultMessage.equals("[ ]")){
//                return ReturnT.SUCCESS;
//            }
            new KafkaUtils().sendMessage(druidNativeQueryToKafkaParam.getTopic(), formatDruidNativeQueryResult(resultMessage,druidNativeQueryToKafkaParam));

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
        druidNativeParam.setIntervals(getIntervalList(druidNativeParam));
        druidNativeParam.setGranularity("all");
        druidNativeToKafkaParamObject.put("requestBody",druidNativeParam);
        DruidNativeQueryToKafkaParam druidNativeToKafkaParam =
                (DruidNativeQueryToKafkaParam)JsonMapper.fromJsonString(JsonMapper.toJsonString(druidNativeToKafkaParamObject), DruidNativeQueryToKafkaParam.class);
        return druidNativeToKafkaParam;

    }

    /**
     * 根据传入的参数确定查询粒度以及范围，目前支持minute、hour、day级别粒度查询
     * @param druidNativeParam
     * @return
     */
    private List<Object> getIntervalList(DruidNativeParam druidNativeParam){
        String granularity = druidNativeParam.getGranularity();
        long currentTime = new Date().getTime();
        long startTime = 0L;
        long endTime = 0L;
        String initIntervalStr = druidNativeParam.getIntervals().get(0).toString();
        if (StringUtil.isBlank(initIntervalStr)){
            XxlJobLogger.log("intervals is empty ! ");
            return null;
        }
        Interval initInterval = new Interval(initIntervalStr);
        long intervalLong = initInterval.getEndMillis()-initInterval.getStartMillis();

        if (StringUtil.isBlank(granularity)){
            XxlJobLogger.log("granularity is empty ! ");
            return null;
        }
        switch (granularity){
            case "minute":
                startTime = currentTime/(60*1000) * 60*1000 - intervalLong;
                endTime = currentTime/(60*1000) * 60*1000;
                break;
            case "hour":
                startTime = currentTime/(60*60*1000) * 60*60*1000 - intervalLong;
                endTime = currentTime/(60*60*1000) * 60*60*1000;
                break;
            case "day":
                startTime = currentTime/(60*60*24*1000) * 60*60*24*1000 - intervalLong;
                endTime = currentTime/(60*60*24*1000) * 60*60*24*1000;
                break;
            default:
                XxlJobLogger.log("Could not resolve type id {} into a subtype of granularity,known type ids = [minute,hour,day]",granularity);
                return null;
        }
//        Interval interval = new Interval(startTime,endTime, DateTimeZone.UTC);
        Interval interval = new Interval(startTime,endTime);
        ArrayList<Object> intervalList = new ArrayList<>();
        intervalList.add(interval.toString());
        return intervalList;
    }

    /**
     * 转换最终结果数据
     * @param resultMessage
     * @return
     */
    private List<Object> formatDruidNativeQueryResult(String resultMessage,DruidNativeQueryToKafkaParam druidNativeQueryToKafkaParam){
        if (resultMessage.equals("[ ]")){
            return new ArrayList<>();
        }
        //从json数组中解析出结果集
        ArrayList<Object> lst = (ArrayList<Object>)JsonMapper.fromJsonString(resultMessage, Object.class);
        Map<String, Object> resultMap = (Map<String, Object>) lst.get(0);
        ArrayList<Object> resultRowLst = (ArrayList<Object>) resultMap.get("result");

        //获取当前查询时间
        Map<String, Object> requestBody = druidNativeQueryToKafkaParam.getRequestBody();
        ArrayList<Object> intervalLst = (ArrayList<Object>) requestBody.get("intervals");
        String intervalStr = intervalLst.get(0).toString();
        Interval interval = new Interval(intervalStr, ISOChronology.getInstance(DateTimeZone.UTC));
        String statTime = interval.getEnd().toString();

        //组装最终结果集
        ArrayList<Object> resultLst = new ArrayList<>();
        for (Object rowObj: resultRowLst){
            DruidNativeQueryResultParam row = (DruidNativeQueryResultParam)JsonMapper.fromJsonString(JsonMapper.toJsonString(rowObj), DruidNativeQueryResultParam.class);
            row.setStat_time(statTime);
            resultLst.add(row);
        }
        return resultLst;
    }


}
