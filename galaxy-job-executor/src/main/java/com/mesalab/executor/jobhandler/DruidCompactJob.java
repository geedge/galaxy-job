package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.config.XxlJobConfig;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.core.utils.UrlUtil;
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

    private static StorgeConfig compactConfig =  (StorgeConfig) SpringContextUtil.getBean("storgeConfig");


    @XxlJob("druidCompactSegmentJobHandler")
    public ReturnT<String> druidCompactSegmentJobHandler (String param)  {
        try{
            if (StringUtil.isEmpty(param)){
                logger.error("params parser error , params is {}", param);
                return IJobHandler.FAIL;
            }
            DruidNativeCompactParam druidNativeCompactParam = (DruidNativeCompactParam)JsonMapper.fromJsonString(param, DruidNativeCompactParam.class);
            //传入json为http的请求体
            jsonForRequestBodyToHttp(druidNativeCompactParam);
        }catch (Exception e){
            logger.error(e.getMessage());
            XxlJobLogger.log(e.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }


    /**
     * 根据传入的DataSource,来确定要压缩的表
     */
    public String getDataSource(DruidNativeCompactParam druidNativeCompactParam){
        try {
            if (druidNativeCompactParam!=null){
                return druidNativeCompactParam.getDataSource();
            }
        }catch (BusinessException be){
            logger.error(be.getMessage());
            XxlJobLogger.log("dataSource is empty");
        }
        return null;
    }

    /**
     * 根据传入参数来确定压缩时间范围
     */
    public String getTimeInterval (DruidNativeCompactParam druidNativeCompactParam) throws ParseException {
        try {
            String interval="";
            if (druidNativeCompactParam!=null) {
                //初始当前时间
                Calendar initTime = Calendar.getInstance();
                //初始历史时间
                Calendar initPastTime = Calendar.getInstance();
                SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd");
                //当前时间
                String intervalTime = "" + initTime.get(Calendar.YEAR) + "-" + "" + (initTime.get(Calendar.MONTH) + 1) + "-"
                        + "" + (initTime.get(Calendar.DAY_OF_MONTH));
                //当前时间格式化
                Date formatCurrentTime = timeFormat.parse(intervalTime);
                //根据传入参数，输出历史时间
                initPastTime.add(Calendar.DATE, -druidNativeCompactParam.getNumOfDay());
                //格式化当前和历史时间
                String intervalEndTime = timeFormat.format(formatCurrentTime.getTime());
                String intervalStartTime = timeFormat.format(initPastTime.getTime());
                interval=intervalStartTime+"/"+intervalEndTime;
            }
            return interval;
        }catch (BusinessException be){
            logger.error(be.getMessage());
            XxlJobLogger.log("numOfDay  is empty , interval could not be null");
            return null;
        }
    }

    /**
     * 根据传入的参数来确定压缩粒度，时间粒度为day、week、month
     */
    public String getSegmentGranularities(DruidNativeCompactParam druidNativeCompactParam){
        try{
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
                    default:
                        XxlJobLogger.log("Could not resolve type id {} into a subtype of setSegmentGranularity,known type ids = [day,week,month]",segmentGranularity);
                        return null;
                }
                return segmentGranularity;
            }
            return null;
        }catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log("segmentGranularity is empty");
            return null;
        }
    }

    /**
     * 根据传入的参数拼接json，并将json设置为请求体，提交Http请求
     */
    public void jsonForRequestBodyToHttp(DruidNativeCompactParam druidNativeCompactParam) throws ParseException{
        String url = UrlUtil.getUrl(compactConfig.getAnalyticServer(),"/druid/indexer/v1/task");
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
                "    \"segmentGranularity\""+":"+"\""+getSegmentGranularities(druidNativeCompactParam)+"\""+",\n" +
                "    \"tuningConfig\":{\n" +
                "        \"type\":\"index_parallel\",\n" +
                "        \"maxRowsPerSegment\":5000000,\n" +
                "        \"maxRowsInMemory\":25000\n" +
                "    }\n" +
                "}";
        HttpClientUtils.httpPost(url,json);
//        return json;
    }
}
