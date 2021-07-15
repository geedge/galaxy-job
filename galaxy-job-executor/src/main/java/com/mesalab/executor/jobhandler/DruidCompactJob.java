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
    public ReturnT<String> druidCompactSegmentJobHandler (String param) throws ParseException {

        try{
            if (StringUtil.isEmpty(param)){
                logger.error("params parser error , params is {}", param);
                return IJobHandler.FAIL;
            }
            //获取调度平台传入的参数并拼接成json，提交http请求至druid
            String dataSource = JSONObject.parseObject(param).getString("dataSource");
            int numOfDay = JSONObject.parseObject(param).getIntValue("numOfDay");
            String segmentGranularity = JSONObject.parseObject(param).getString("segmentGranularity");
            DruidNativeCompactParam druidNativeCompactParam=new DruidNativeCompactParam();
            druidNativeCompactParam.setDataSource(dataSource);
            druidNativeCompactParam.setNumOfDay(numOfDay);
            druidNativeCompactParam.setSegmentGranularity(segmentGranularity);
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
        if (druidNativeCompactParam!=null){
            druidNativeCompactParam.getDataSource();
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
            date1.add(Calendar.DATE, -druidNativeCompactParam.getNumOfDay());
            String intervalEndTime = time.format(date2.getTime());
            String intervalStartTime = time.format(date1.getTime());
            interval=intervalStartTime+"/"+intervalEndTime;
        }
        return  interval;
    }

    /**
     * 根据传入的参数来确定压缩粒度，时间粒度为day、week、month
     */
    public String getSegmentGranularities(DruidNativeCompactParam druidNativeCompactParam){
        String segmentGranularity = druidNativeCompactParam.getSegmentGranularity();

        if (druidNativeCompactParam!=null){
            //segmentGranularity
            switch (segmentGranularity){
                case "DAY":
                    druidNativeCompactParam.setSegmentGranularity("DAY");
                    druidNativeCompactParam.getSegmentGranularity();
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
}
