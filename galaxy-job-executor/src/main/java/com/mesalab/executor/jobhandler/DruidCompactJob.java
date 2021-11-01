package com.mesalab.executor.jobhandler;

import cn.hutool.log.Log;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.exception.BusinessException;
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
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.*;


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
    public ReturnT<String> druidCompactSegmentJobHandler (String params)  {
        try{
            if (StringUtil.isEmpty(params)){
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            //参数不为空，多参数切割并分别传入post请求中
            String[] paramOfPost = params.split(";");
            for (String param:paramOfPost) {
                DruidNativeCompactParam druidNativeCompactParam = (DruidNativeCompactParam)JsonMapper.fromJsonString(param, DruidNativeCompactParam.class);
                //传入json为http的请求体
                jsonForRequestBodyToHttp(druidNativeCompactParam);
                System.out.println("卧槽 --------");
                deleteMetaData(druidNativeCompactParam);
            }
        }catch (BusinessException be){
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
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
            XxlJobLogger.log("dataSource is empty , dataSource could not be null");
        }
        return null;
    }

    /**
     * 根据传入参数来确定压缩时间范围,可输入数字，如："1"，也可输入时间范围，如："2021-10-27/2021-10-28"
     */
    public String getTimeInterval (DruidNativeCompactParam druidNativeCompactParam) {
        try {
            String interval="";
            if (druidNativeCompactParam!=null) {
                if (druidNativeCompactParam.getInternalOrNumOfHistory().contains("/")){
                    return druidNativeCompactParam.getInternalOrNumOfHistory();
                }else {
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
                    initPastTime.add(Calendar.DATE, -Integer.parseInt(druidNativeCompactParam.getInternalOrNumOfHistory()));
                    //格式化当前和历史时间
                    String intervalEndTime = timeFormat.format(formatCurrentTime.getTime());
                    String intervalStartTime = timeFormat.format(initPastTime.getTime());
                    interval=intervalStartTime+"/"+intervalEndTime;
                    return interval;
                }
            }
            return null;
        }catch (BusinessException be){
            logger.error(be.getMessage());
            XxlJobLogger.log("interval  is empty , interval could not be null");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
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
                    case "QUARTER":
                        druidNativeCompactParam.setSegmentGranularity("QUARTER");
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
            XxlJobLogger.log("segmentGranularity is empty , segmentGranularity could not be null");
            return null;
        }
    }

    /**
     * 根据传入的参数拼接json，并将json设置为请求体，提交Http请求
     */
    public void jsonForRequestBodyToHttp(DruidNativeCompactParam druidNativeCompactParam){
        try {
            String url = UrlUtil.getUrl(compactConfig.getAnalyticServer(), "/druid/indexer/v1/task");
            String json = "{\n" +
                    "    \"type\":\"compact\",\n" +
                    "    \"dataSource\"" + ":" + "\"" + getDataSource(druidNativeCompactParam) + "\"," + "\n" +
                    "    \"ioConfig\":{\n" +
                    "        \"type\":\"compact\",\n" +
                    "        \"inputSpec\":{\n" +
                    "            \"type\":\"interval\",\n" +
                    "            \"interval\"" + ":" + "\"" + getTimeInterval(druidNativeCompactParam) + "\"" + "\n" +
                    "        }\n" +
                    "    },\n" +
                    "    \"segmentGranularity\"" + ":" + "\"" + getSegmentGranularities(druidNativeCompactParam) + "\"" + ",\n" +
                    "    \"tuningConfig\":{\n" +
                    "        \"type\":\"index_parallel\",\n" +
                    "        \"maxRowsPerSegment\":5000000,\n" +
                    "        \"maxRowsInMemory\":25000\n" +
                    "    }\n" +
                    "}";
            HttpClientUtils.httpPost(url, json);
        }catch (BusinessException be) {
            logger.error(be.getMessage());
        }
    }
    /**
     * 压缩任务通过http请求提交后，获取任务状态码，根据任务状态删除历史元数据
     */
    public void deleteMetaData(DruidNativeCompactParam druidNativeCompactParam){
        try {
            //查询任务状态限时2小时
            long startTime = System.currentTimeMillis();
            long endTime = startTime + 1000 * 60 * 120;
            //获取正在运行的任务
            String queryUrl = UrlUtil.getUrl(compactConfig.getAnalyticServer(), "/druid/indexer/v1/runningTasks");
            String responseMsg = HttpClientUtils.httpGet(queryUrl);
            JSONArray responseJsonMsg = JSONObject.parseArray(responseMsg);
            for (Object responseJson : responseJsonMsg) {
                JSONObject responseMsgJson = (JSONObject) responseJson;
                //获取compact开头的正在运行的任务id
                String id = responseMsgJson.getString("id");
                if (id.startsWith("compact")) {
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            while (true){
                                //获取这个任务状态
                                //2.1
                                String taskStatusUrl = "http://" + compactConfig.getAnalyticServer() + "/druid/indexer/v1/task/" + id + "/status";
                                String taskStatusMsg = HttpClientUtils.httpGet(taskStatusUrl);
                                //通过http返回的信息进行判断任务是否为success
                                JSONObject jsonObject = JSONObject.parseObject(taskStatusMsg);
                                String  status = jsonObject.getJSONObject("status").getString("status");
                                if (status.equals("SUCCESS")) {
                                    String url = UrlUtil.getUrl(compactConfig.getAnalyticServer(), "/druid/indexer/v1/task");
                                    String deleteJson = "{\"type\": \"kill\"," +
                                            "\"dataSource\"" + ":" + "\"" + getDataSource(druidNativeCompactParam) + "\"," +
                                            "\"interval\"" + ":" + "\"" + getTimeInterval(druidNativeCompactParam) + "\"}";
                                    HttpClientUtils.httpPost(url, deleteJson);
                                    break;
                                } else if (status.equals("RUNNING") || status.equals("WAITING")) {
                                    //nothing
                                    try {
                                        Thread.sleep(60*1000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                } else {
                                    logger.error("query druid running task failed error");
                                }

                                if (System.currentTimeMillis() > endTime) {
                                    logger.error("query druid running task time out error");
                                    return;
//                            XxlJobLogger.log("druid compact task is running , kill task post timeout");
                                }
                            }
                        }
                    }).start();
                }
            }
        }catch (BusinessException be){
            logger.error(be.getMessage());
        }


    }
}
