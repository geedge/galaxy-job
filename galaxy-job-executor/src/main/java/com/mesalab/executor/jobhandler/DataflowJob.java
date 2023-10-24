package com.mesalab.executor.jobhandler;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.log.Log;
import com.alibaba.fastjson.util.TypeUtils;
import com.geedgenetworks.utils.DateUtils;
import com.geedgenetworks.utils.JsonMapper;
import com.geedgenetworks.utils.SnowflakeId;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Splitter;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.*;
import com.mesalab.executor.exception.BusinessException;
import com.mesalab.executor.pojo.HttpParam;
import com.mesalab.executor.pojo.HttpToKafkaParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import static com.mesalab.executor.core.utils.Constant.DEFAULT_VSYS_ID_VALUE;

@Component
public class DataflowJob {
    private Log logger = Log.get();

    private static StorgeConfig storeConfig = (StorgeConfig) SpringContextUtil.getBean("storgeConfig");

    /**
     * 调度模型httpToKafka
     * source: http rest url
     * sink: kafka
     * params 参数定义如下：
     * {
     * url:<Http Rest url>,
     * method:[request method], // get|post 可选，默认get
     * requestBody:[{query:query sql json }],
     * resultKey:[key], //可选，对结果做二次解析
     * topic:<topicName>  // 分区名称，依据系统预置的kafka broker
     * }
     *
     * @param params Json 结构
     * @return
     */
    @XxlJob("httpToKafkaJobHandler")
    public ReturnT<String> httpToKafkaJobHandler(String params) {


        HttpToKafkaParam httpToKafkaParam = parserHttpToKafkaParams(params);

        if (StringUtil.isEmpty(httpToKafkaParam)) {
            logger.error("params parser error , params is {}", params);
            return IJobHandler.FAIL;
        }

        try {
            String resultMessage;

            if (HttpMethod.POST.name().equalsIgnoreCase(httpToKafkaParam.getMethod())) {
                resultMessage = HttpClientUtils.httpPost(httpToKafkaParam.getUrl(), JsonMapper.toJsonString(httpToKafkaParam.getRequestBody()));
            } else if (HttpMethod.GET.name().equalsIgnoreCase(httpToKafkaParam.getMethod())) {
                resultMessage = HttpClientUtils.httpGet(httpToKafkaParam.getUrl());
            } else {
                return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Not support Method:" + httpToKafkaParam.getMethod());
            }

            if (resultMessage.equals("-1")) {
                return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Http Client connection Error!");

            }

            Object resultObj = JsonMapper.fromJsonString(resultMessage, Object.class);

            if (StringUtil.isNotBlank(httpToKafkaParam.getResultKey())) {
                resultObj = ((Map) resultObj).get(httpToKafkaParam.getResultKey());

            }

            new KafkaUtils().sendMessage(httpToKafkaParam.getTopic(), (List<Object>) resultObj);

            XxlJobLogger.log("Send message to kafka, topic is {}", httpToKafkaParam.getTopic());

            return ReturnT.SUCCESS;

        } catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
            return new ReturnT<String>(be.getCode(), be.getMessage());
        }
    }


    /**
     * 必填参数进行验证，其它自行配置
     *
     * @param params
     * @return
     */
    private HttpToKafkaParam parserHttpToKafkaParams(String params) {

        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }

        HttpToKafkaParam httpToKafkaParam = (HttpToKafkaParam) JsonMapper.fromJsonString(params, HttpToKafkaParam.class);

        if (StringUtil.isEmpty(httpToKafkaParam)) {
            XxlJobLogger.log("params is not format json ! ");
            return null;
        }
        if (StringUtil.isBlank(httpToKafkaParam.getUrl())) {
            XxlJobLogger.log("url is empty ! ");
            return null;
        }
        if (StringUtil.isEmpty(httpToKafkaParam.getTopic())) {
            XxlJobLogger.log("kafka topic is empty ! ");
            return null;
        }
        if (StringUtil.isBlank(httpToKafkaParam.getMethod())) {
            XxlJobLogger.log("this request method is not supported");
            httpToKafkaParam.setMethod(HttpMethod.GET.name());
        }
        return httpToKafkaParam;
    }


    @XxlJob("httpToStoreJobHandler")
    public ReturnT<String> httpToStoreJobHandler(String params) {

        List<HttpParam> httpParams = HttpParam.parserHttpParams(params);
        if (ObjectUtil.isEmpty(httpParams)) {
            logger.error("params parser error , params is {}", params);
            return IJobHandler.FAIL;
        }
        try {
            String lotNumber = DateUtils.getCurrentDate("yyyyMMdd") + "-" + RandomUtil.randomString(6);
            String filePrefix = "web-sketch-db-weekly-assement-".concat(lotNumber);
            String zipName = filePrefix.concat(".zip");
            String bucketName = "assessment_hos_bucket";
            String assessmentType = "web-sketch";
            String features = "";
            String trafficDatasource = storeConfig.getTrafficDatasource();
            String dataSource = trafficDatasource + ".assessment_event";
            //vsys_id,内置虚拟系统id默认为 0
            Integer defVsysId = DEFAULT_VSYS_ID_VALUE;
            Map<String, Object> requestBody = httpParams.get(0).getRequestBody();
            if(ObjectUtil.isNotEmpty(requestBody)&&ObjectUtil.isNotEmpty(requestBody.get("vsysId"))){
                defVsysId = TypeUtils.castToInt(requestBody.get("vsysId"));
            }

            String insertTemplate = "INSERT INTO {0} (common_log_id,common_recv_time,assessment_date,lot_number,file_name,assessment_file,assessment_type,features,`size`,file_checksum_sha,vsys_id) " +
                    "VALUES({1,number,#},{2,number,#},{3,number,#},''{4}'',''{5}'',''{6}'',''{7}'',''{8}'',{9,number,#},''{10}'',{11,number,#});";
            long logId = SnowflakeId.generateId(1, 1);
            long now = System.currentTimeMillis() / 1000;
            String tmpDir = "";
            try {
                tmpDir = new File("").getCanonicalPath().concat(File.separator);
            } catch (IOException e) {
                throw new BusinessException("create tmp directory error");
            }
            //查询结果压缩
            File[] tmpFiles = new File[httpParams.size()];
            for (int i = 0; i < httpParams.size(); i++) {
                String store = httpParams.get(i).getStore();
                XxlJobLogger.log("query clickhouse for {} domain", store);
                logger.info("query clickhouse for {} domain", store);
                features = i == httpParams.size() - 1 ? features.concat(store).concat(" domains") : features.concat(store).concat(" domains,");
                tmpFiles[i] = JobUtil.createCsvFile(queryCk(httpParams.get(i)), tmpDir.concat(filePrefix).concat("-").concat(store).concat(".csv"));
            }
            File zipFile = ZipUtil.zip(FileUtil.file(tmpDir.concat(zipName)), false, tmpFiles);
            long fileSize = zipFile.length();
            String sha256Hex = DigestUtil.sha256Hex(zipFile);

            XxlJobLogger.log("send file to hos, file is {}", zipName);

            //上传到hos
            Header header = new BasicHeader(Constant.TOKEN, storeConfig.getFilesToken());
            List<String> endpointList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(storeConfig.getFilesHosServer());
            String filesServer = UrlUtil.getUrl(endpointList.get(0));
            String assessmentFile = filesServer.concat("hos/").concat(bucketName).concat("/").concat(zipName);

            try (InputStream in = new BufferedInputStream(new FileInputStream(zipFile))) {
                String res = HttpClientUtils.httpPutStream(assessmentFile, in, header);
                if ("-1".equals(res)) {
                    throw new BusinessException("send file to hos fail ");
                }
            } catch (IOException e) {
                throw new BusinessException(e);
            } finally {
                //刪除临时文件
                FileUtil.del(zipFile);
                for (File file : tmpFiles) {
                    FileUtil.del(file);
                }
            }
            //元数据存入ck库
            String insertSql = MessageFormat.format(insertTemplate, dataSource, logId, now, now, lotNumber, zipName, assessmentFile, assessmentType, features, fileSize, sha256Hex, defVsysId);
            Map<String, Object> ckParamMap = storeConfig.getCkSource();
            ckParamMap.put("query", insertSql);

            UrlBuilder urlBuilder = UrlBuilder.ofHttp(storeConfig.getTrafficServer(), StandardCharsets.UTF_8);
            logger.info(" url info " + urlBuilder.toString());
            HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(urlBuilder.toString(), ckParamMap), "");
            logger.info("save web-sketch file metadata to clickhouse");
            XxlJobLogger.log("save web-sketch file metadata to clickhouse");
            return ReturnT.SUCCESS;

        } catch (BusinessException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
            return new ReturnT<String>(be.getCode(), be.getMessage());
        }
    }

    public List<Map> queryCk(HttpParam hp) {
        String resultMessage;
        if (HttpMethod.POST.name().equalsIgnoreCase(hp.getMethod())) {
            resultMessage = HttpClientUtils.httpPost(hp.getUrl(), JsonMapper.toJsonString(hp.getRequestBody()));
        } else if (HttpMethod.GET.name().equalsIgnoreCase(hp.getMethod())) {
            resultMessage = HttpClientUtils.httpGet(hp.getUrl());
        } else {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Not support Method:" + hp.getMethod());
        }
        if (resultMessage.equals("-1")) {
            throw new BusinessException(IJobHandler.FAIL.getCode(), "Http Client connection Error!");
        }
        Object resultObj = JsonMapper.fromJsonString(resultMessage, Object.class);
        if (StringUtil.isNotBlank(hp.getResultKey())) {
            resultObj = ((Map) resultObj).get(hp.getResultKey());
        }
        List<Map> results = (List<Map>) resultObj;
        return results;
    }


}
