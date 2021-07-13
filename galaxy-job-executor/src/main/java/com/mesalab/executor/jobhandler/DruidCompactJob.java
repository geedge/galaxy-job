package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.DruidToHttpParam;
import com.mesalab.executor.pojo.DruidNativeCompactParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2021/7/6
 * @ Description druid压缩segment任务
 */
@Component
public class DruidCompactJob {
    private Log logger = Log.get();


    @XxlJob("druidCompactSegmentJobHandler")
    public ReturnT<String> druidCompactSegmentJobHandler (String params) throws ParseException {

        if (StringUtil.isEmpty(druidToHttpParam(params))){
            logger.error("params parser error , params is {}",params);
            return IJobHandler.FAIL;
        }
        try{
            return ReturnT.SUCCESS;
        }catch (BusinessException e){
            logger.error(e.getMessage());
            XxlJobLogger.log(e.getMessage());
            return new ReturnT<String>(e.getCode(), e.getMessage());
        }
    }

    /**
     * 参数验证
     */
    public DruidToHttpParam druidToHttpParam(String params) throws ParseException {
        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }
        Map<String,Object> druidToHttpParamObject = (Map<String,Object>) JsonMapper.fromJsonString(params, Object.class);
        if (StringUtil.isBlank(druidToHttpParamObject.get("url").toString())) {
            XxlJobLogger.log("url is empty ! ");
            return null;
        }
        if(StringUtil.isBlank(druidToHttpParamObject.get("interval").toString())) {
            XxlJobLogger.log("interval  is empty ! ");
            return null;
        }
        if(StringUtil.isBlank(druidToHttpParamObject.get("segmentGranularity").toString())) {
            XxlJobLogger.log("segmentGranularity is empty , execute default segmentGranularity! ");
            return null;
        }
        if (StringUtil.isBlank(druidToHttpParamObject.get("requestBody").toString())){
            XxlJobLogger.log("druid native query request body is empty ! ");
            return null;
        }
        String queryRequestJson = JsonMapper.toJsonString(druidToHttpParamObject.get("requestBody"));
        DruidNativeCompactParam druidNativeCompactParam=(DruidNativeCompactParam)JsonMapper.fromJsonString(queryRequestJson,DruidNativeCompactParam.class);
        druidNativeCompactParam.setDataSource(getDataSource(druidNativeCompactParam));
        druidNativeCompactParam.setNumOfDay(getTimeInterval(druidNativeCompactParam));
        druidNativeCompactParam.setSegmentGranularity(getSegmentGranularity(druidNativeCompactParam));
        DruidToHttpParam druidCompactParam = (DruidToHttpParam)JsonMapper.fromJsonString(JsonMapper.toJsonString(druidToHttpParamObject),DruidNativeCompactParam.class);
        return druidCompactParam;
    }

    /**
     * 根据传入的DataSource,来确定要压缩的表
     */
    public String getDataSource(DruidNativeCompactParam druidNativeCompactParam){
        if (druidNativeCompactParam!=null){
            druidNativeCompactParam.setDataSource(druidNativeCompactParam.getDataSource());
            }
        return druidNativeCompactParam.getDataSource();
    }

    /**
     * 根据传入参数来确定压缩时间范围
     */
    public String getTimeInterval (DruidNativeCompactParam druidNativeCompactParam) throws ParseException {
      String interval="";
        if (druidNativeCompactParam!=null) {
            //使用日历类，以根据传入参数来确定压缩起始时间
            Calendar date = Calendar.getInstance();
            SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd");
            String year = "" + date.get(Calendar.YEAR);
            String month = "" + (date.get(Calendar.MONTH) + 1);
            String day = "" + (date.get(Calendar.DAY_OF_MONTH));
            String intervalTime = year + "-" + month + "-" + day;//2021-7-13
            Date date2 = time.parse(intervalTime);
            Calendar date1 = Calendar.getInstance();
            date1.add(Calendar.DATE, -Integer.valueOf(druidNativeCompactParam.getNumOfDay()));
            String intervalEndTime = time.format(date2.getTime());
            String intervalStartTime = time.format(date1.getTime());
            interval=intervalStartTime+"/"+intervalEndTime;
        }
        return  interval;
    }

    /**
     * 根据传入的参数来确定压缩粒度，时间粒度为day、week、month
     */
    public String getSegmentGranularity(DruidNativeCompactParam druidNativeCompactParam){
        String segmentGranularity = druidNativeCompactParam.getSegmentGranularity();

        if (druidNativeCompactParam!=null){
            //segmentGranularity
            switch (segmentGranularity){
            case "DAY":
                druidNativeCompactParam.setSegmentGranularity("DAY");
                break;
            case "WEEK":
                druidNativeCompactParam.setSegmentGranularity("WEEK");
                break;
            case "MONTH":
                druidNativeCompactParam.setSegmentGranularity("MONTH");
                break;
            case "":
                druidNativeCompactParam.setSegmentGranularity("");
            default:
                XxlJobLogger.log("Could not resolve type id {} into a subtype of setSegmentGranularity,known type ids = [day,week,month]",segmentGranularity);
                return null;
        }
        }
        return segmentGranularity;
    }

    /**
     * 将接收到的参数与json内的其他参数组成json格式,并将其作为请求体传入Http请求中
     */
    public String jsonForRequestBodyToHttp (DruidNativeCompactParam druidNativeCompactParam,DruidToHttpParam druidToHttpParam) throws ParseException {
        StringBuilder url = new StringBuilder("http://");
        url.append(druidToHttpParam.getUrl())
                .append(":")
                .append(druidToHttpParam.getPort())
                .append("/druid/indexer/v1/task");
        String json="{\n" +
                "    \"type\":\"compact\",\n" +
                "    \"dataSource\""+":"+"\""+getDataSource(druidNativeCompactParam)+"\","+"\n" +
                "    \"ioConfig\":{\n" +
                "        \"type\":\"compact\",\n" +
                "        \"inputSpec\":{\n" +
                "            \"type\":\"interval\",\n" +
                "            \"interval\""+":"+"\""+getTimeInterval(druidNativeCompactParam)+"\""+"\n" +
                "        }\n" +
                "    },\n" +
                "    \"segmentGranularity\""+":"+"\""+getSegmentGranularity(druidNativeCompactParam)+"\""+",\n" +
                "    \"tuningConfig\":{\n" +
                "        \"type\":\"index_parallel\",\n" +
                "        \"maxRowsPerSegment\":5000000,\n" +
                "        \"maxRowsInMemory\":25000\n" +
                "    }\n" +
                "}";
        HttpClientUtils.httpPost(url.toString(),json);
        return json;
    }
}
