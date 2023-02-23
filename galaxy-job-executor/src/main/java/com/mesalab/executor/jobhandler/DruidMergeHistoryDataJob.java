package com.mesalab.executor.jobhandler;

import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.SpringContextUtil;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.exception.BusinessException;

import com.mesalab.executor.pojo.DruidNativeMergeParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.StringUtil;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**
 * @ Author xu
 * @ Date 2023/2/23
 * @ Description druid归并历史数据任务
 * 参数定义如下：
 * [
 * {"dataSource":"table name",
 * "internalOrNumOfHistory":"时间范围或数字",  //如2023-02-22/2023-02-23，或者1
 * "queryGranularity":"Merge后的时间粒度",   //可选PT1M/PT5M/PT10M/PT15M
 * "segmentGranularity":"时间粒度"   //可选DAY/WEEK/MONTH/QUARTER
 * }
 * ]
 */
@Component
public class DruidMergeHistoryDataJob {
    private Log logger = Log.get();

    private static StorgeConfig updateConfig =  (StorgeConfig) SpringContextUtil.getBean("storgeConfig");


    @XxlJob("druidMergeHistoryDataJobHandler")
    public ReturnT<String> druidMergeHistoryDataJobHandler (String params)  {
        try{
            if (StringUtil.isEmpty(params)){
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            List<DruidNativeMergeParam> paramOfPost = JSONUtil.toList(params, DruidNativeMergeParam.class);
            for (DruidNativeMergeParam param:paramOfPost) {
                //根据DataSource，传入json为http的请求体
                jsonForRequestBodyToHttp(param);
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
    public String getDataSource(DruidNativeMergeParam druidNativeMergeParam){
        try {
            if (druidNativeMergeParam!=null){
                return druidNativeMergeParam.getDataSource();
            }
        }catch (BusinessException be){
            logger.error(be.getMessage());
            XxlJobLogger.log("dataSource is empty , dataSource could not be null");
        }
        return null;
    }

    /**
     * 根据传入参数来确定更新数据的时间范围,可输入数字，如："1"，也可输入时间范围，如："2023-02-22/2021-02-23"
     */
    public String getTimeInterval (DruidNativeMergeParam druidNativeMergeParam) {
        try {
            String interval="";
            if (druidNativeMergeParam!=null) {
                if (druidNativeMergeParam.getInternalOrNumOfHistory().contains("/")){
                    return druidNativeMergeParam.getInternalOrNumOfHistory();
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
                    initPastTime.add(Calendar.DATE, -Integer.parseInt(druidNativeMergeParam.getInternalOrNumOfHistory()));
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
     * 时间粒度为day、week、month、quarter
     */
    public String getSegmentGranularities(DruidNativeMergeParam druidNativeMergeParam){
        try{
            String segmentGranularity = druidNativeMergeParam.getSegmentGranularity();
            if (druidNativeMergeParam!=null){
                //segmentGranularity
                switch (segmentGranularity){
                    case "DAY":
                        druidNativeMergeParam.setSegmentGranularity("DAY");
                        break;
                    case "WEEK":
                        druidNativeMergeParam.setSegmentGranularity("WEEK");
                        break;
                    case "MONTH":
                        druidNativeMergeParam.setSegmentGranularity("MONTH");
                        break;
                    case "QUARTER":
                        druidNativeMergeParam.setSegmentGranularity("QUARTER");
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
     * 时间粒度为1min、5min、10min、15min
     */
    public String getQueryGranularity(DruidNativeMergeParam druidNativeMergeParam){
        try{
            String queryGranularity = druidNativeMergeParam.getQueryGranularity();
            if (druidNativeMergeParam!=null){
                //segmentGranularity
                switch (queryGranularity){
                    case "PT1M":
                        druidNativeMergeParam.setQueryGranularity("PT1M");
                        break;
                    case "PT5M":
                        druidNativeMergeParam.setQueryGranularity("PT5M");
                        break;
                    case "PT10M":
                        druidNativeMergeParam.setQueryGranularity("PT10M");
                        break;
                    case "PT15M":
                        druidNativeMergeParam.setQueryGranularity("PT15M");
                        break;
                    default:
                        XxlJobLogger.log("Could not resolve type id {} into a subtype of setQueryGranularity,known type ids = [1min,5min,10min,15min]",queryGranularity);
                        return null;
                }
                return queryGranularity;
            }
            return null;
        }catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log("queryGranularity is empty , queryGranularity could not be null");
            return null;
        }
    }

    /**
     * 根据传入的参数拼接json，并将json设置为请求体，提交Http请求
     */
    public void jsonForRequestBodyToHttp(DruidNativeMergeParam druidNativeMergeParam){
        try {
            String url = UrlUtil.getUrl(updateConfig.getAnalyticServer(), "/druid/indexer/v1/task");
            if ("service_function_status".equals(druidNativeMergeParam.getDataSource())){
                String functionJson = "{\n" +
                        "  \"type\": \"index_parallel\",\n" +
                        "  \"spec\": {\n" +
                        "    \"ioConfig\": {\n" +
                        "      \"type\": \"index_parallel\",\n" +
                        "      \"inputSource\": {\n" +
                        "        \"type\": \"druid\",\n" +
                        "        \"dataSource\"" + ":" + "\"" + getDataSource(druidNativeMergeParam) + "\"," + "\n" +
                        "       \"interval\"" + ":" + "\"" + getTimeInterval(druidNativeMergeParam) + "\"" + "\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"tuningConfig\": {\n" +
                        "      \"type\": \"index_parallel\",\n" +
                        "      \"partitionsSpec\": {\n" +
                        "        \"type\": \"dynamic\"\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"dataSchema\": {\n" +
                        "      \"dataSource\"" + ":" + "\"" + getDataSource(druidNativeMergeParam) + "\"," + "\n" +
                        "      \"granularitySpec\": {\n" +
                        "        \"type\": \"uniform\",\n" +
                        "        \"queryGranularity\": {\"type\": \"period\", \"period\""+":"+"\""+getQueryGranularity(druidNativeMergeParam)+"\""+","+ "\"timeZone\": \"Asia/Hong_Kong\"},\n" +
                        "        \"rollup\": true,\n" +
                        "        \"segmentGranularity\"" + ":" + "\"" + getSegmentGranularities(druidNativeMergeParam) + "\"" + "\n" +
                        "      },\n" +
                        "      \"timestampSpec\": {\n" +
                        "        \"column\": \"__time\",\n" +
                        "        \"format\": \"iso\"\n" +
                        "      },\n" +
                        "      \"dimensionsSpec\": {\n" +
                        "        \"dimensions\": [\n" +
                        "            \"device_id\",\n" +
                        "            \"device_group\",\n" +
                        "            \"data_center\",\n" +
                        "            {\n" +
                        "              \"name\": \"vsys_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"sf_profile_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"sf_status\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            }\n" +
                        "          ]\n" +
                        "      },\n" +
                        "      \"metricsSpec\": [\n" +
                        "        {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"sf_latency_us\",\n" +
                        "        \"fieldName\": \"sf_latency_us\",\n" +
                        "        \"expression\": null\n" +
                        "      }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";
                HttpClientUtils.httpPost(url, functionJson);
            } else if ("service_chaining_metrics".equals(druidNativeMergeParam.getDataSource())){
                String chainingJson = "{\n" +
                        "  \"type\": \"index_parallel\",\n" +
                        "  \"spec\": {\n" +
                        "    \"ioConfig\": {\n" +
                        "      \"type\": \"index_parallel\",\n" +
                        "      \"inputSource\": {\n" +
                        "        \"type\": \"druid\",\n" +
                        "        \"dataSource\"" + ":" + "\""+ getDataSource(druidNativeMergeParam) + "\"," + "\n" +
                        "        \"interval\"" + ":" + "\"" + getTimeInterval(druidNativeMergeParam) + "\"" + "\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"tuningConfig\": {\n" +
                        "      \"type\": \"index_parallel\",\n" +
                        "      \"partitionsSpec\": {\n" +
                        "        \"type\": \"dynamic\"\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"dataSchema\": {\n" +
                        "      \"dataSource\"" + ":" + "\"" + getDataSource(druidNativeMergeParam) + "\"," + "\n" +
                        "      \"granularitySpec\": {\n" +
                        "        \"type\": \"uniform\",\n" +
                        "        \"queryGranularity\": {\"type\": \"period\", \"period\""+":"+"\""+getQueryGranularity(druidNativeMergeParam)+"\""+","+ "\"timeZone\": \"Asia/Hong_Kong\"},\n" +
                        "        \"rollup\": true,\n" +
                        "        \"segmentGranularity\"" + ":" + "\"" + getSegmentGranularities(druidNativeMergeParam) + "\"" + "\n" +
                        "      },\n" +
                        "      \"timestampSpec\": {\n" +
                        "        \"column\": \"__time\",\n" +
                        "        \"format\": \"iso\"\n" +
                        "      },\n" +
                        "      \"dimensionsSpec\":  {\n" +
                        "          \"dimensions\": [\n" +
                        "            \"device_id\",\n" +
                        "            \"device_group\",\n" +
                        "            \"data_center\",\n" +
                        "            {\n" +
                        "              \"name\": \"vsys_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"rule_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"sff_profile_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"sf_profile_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            }\n" +
                        "          ]\n" +
                        "        },\n" +
                        "      \"metricsSpec\": [\n" +
                        "          {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"sent_pkts\",\n" +
                        "        \"fieldName\": \"sent_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"sent_bytes\",\n" +
                        "        \"fieldName\": \"sent_bytes\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"recv_pkts\",\n" +
                        "        \"fieldName\": \"recv_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"recv_bytes\",\n" +
                        "        \"fieldName\": \"recv_bytes\",\n" +
                        "        \"expression\": null\n" +
                        "      }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";
                HttpClientUtils.httpPost(url, chainingJson);
            }else if ("traffic_shaping_metrics".equals(druidNativeMergeParam.getDataSource())){
                String shapingJson ="{\n" +
                        "  \"type\": \"index_parallel\",\n" +
                        "  \"spec\": {\n" +
                        "    \"ioConfig\": {\n" +
                        "      \"type\": \"index_parallel\",\n" +
                        "      \"inputSource\": {\n" +
                        "        \"type\": \"druid\",\n" +
                        "        \"dataSource\"" + ":" + "\"" + getDataSource(druidNativeMergeParam) + "\"," + "\n" +
                        "        \"interval\"" + ":" + "\"" + getTimeInterval(druidNativeMergeParam) + "\"" + "\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"tuningConfig\": {\n" +
                        "      \"type\": \"index_parallel\",\n" +
                        "      \"partitionsSpec\": {\n" +
                        "        \"type\": \"dynamic\"\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"dataSchema\": {\n" +
                        "      \"dataSource\": \"traffic_shaping_metrics\",\n" +
                        "      \"granularitySpec\": {\n" +
                        "        \"type\": \"uniform\",\n" +
                        "         \"queryGranularity\": {\"type\": \"period\", \"period\""+":"+"\""+getQueryGranularity(druidNativeMergeParam)+"\""+","+ "\"timeZone\": \"Asia/Hong_Kong\"},\n" +
                        "        \"rollup\": true,\n" +
                        "        \"segmentGranularity\"" + ":" + "\"" + getSegmentGranularities(druidNativeMergeParam) + "\"" + "\n" +
                        "      },\n" +
                        "      \"timestampSpec\": {\n" +
                        "        \"column\": \"__time\",\n" +
                        "        \"format\": \"iso\"\n" +
                        "      },\n" +
                        "      \"dimensionsSpec\": {\n" +
                        "         \"dimensions\": [\n" +
                        "            {\n" +
                        "              \"name\": \"profile_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            \"device_id\",\n" +
                        "            \"device_group\",\n" +
                        "            \"data_center\",\n" +
                        "            {\n" +
                        "              \"name\": \"vsys_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"rule_id\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "              \"name\": \"priority\",\n" +
                        "              \"type\": \"long\"\n" +
                        "            }\n" +
                        "          ]\n" +
                        "      },\n" +
                        "      \"metricsSpec\": [\n" +
                        "          {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"active_sessions\",\n" +
                        "        \"fieldName\": \"active_sessions\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"in_rx_pkts\",\n" +
                        "        \"fieldName\": \"in_rx_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"in_tx_pkts\",\n" +
                        "        \"fieldName\": \"in_tx_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"in_rx_bytes\",\n" +
                        "        \"fieldName\": \"in_rx_bytes\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"in_tx_bytes\",\n" +
                        "        \"fieldName\": \"in_tx_bytes\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"out_rx_pkts\",\n" +
                        "        \"fieldName\": \"out_rx_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"out_tx_pkts\",\n" +
                        "        \"fieldName\": \"out_tx_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"out_rx_bytes\",\n" +
                        "        \"fieldName\": \"out_rx_bytes\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"out_tx_bytes\",\n" +
                        "        \"fieldName\": \"out_tx_bytes\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"in_drop_pkts\",\n" +
                        "        \"fieldName\": \"in_drop_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longSum\",\n" +
                        "        \"name\": \"out_drop_pkts\",\n" +
                        "        \"fieldName\": \"out_drop_pkts\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"in_max_latency_us\",\n" +
                        "        \"fieldName\": \"in_max_latency_us\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"out_max_latency_us\",\n" +
                        "        \"fieldName\": \"out_max_latency_us\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"in_queue_len\",\n" +
                        "        \"fieldName\": \"in_queue_len\",\n" +
                        "        \"expression\": null\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"type\": \"longMax\",\n" +
                        "        \"name\": \"out_queue_len\",\n" +
                        "        \"fieldName\": \"out_queue_len\",\n" +
                        "        \"expression\": null\n" +
                        "      }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";

                HttpClientUtils.httpPost(url, shapingJson);
            }

        }catch (BusinessException be) {
            logger.error(be.getMessage());
        }
    }



}
