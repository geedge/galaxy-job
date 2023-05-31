package com.mesalab.executor.jobhandler;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.alibaba.fastjson.JSONPath;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.pojo.IpLocateLibraryParam;
import com.mesalab.executor.pojo.KnowledgeBase;
import com.mesalab.executor.pojo.MMDBStatus;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
public class CommandJob {

    private Log logger = Log.get();

    /**
     * //1.job execute
     * //2.monitoring write log
     * //3.result check
     * //4.push knowledge
     *
     * @param params
     * @return
     */
    @XxlJob("ipLocateLibraryJobHandler")
    public ReturnT<String> ipLocateLibrary(String params) {

        IpLocateLibraryParam parserParams = parserParams(params);
        if (ObjectUtils.isEmpty(parserParams)) {
            logger.error("params parser error , params is {}", params);
            XxlJobLogger.log("params parser error , params is {}", params);
            return IJobHandler.FAIL;
        }
        for (int i = 1; i <= parserParams.getRetry(); i++) {
            try {
                int executeResult = JobUtil.executeCommand(parserParams.getCommand());
                XxlJobLogger.log("ip locate library perl {} execute result {}", i, executeResult==0?"success":"fail");
                logger.error("ip locate library perl {} execute result {}", i, executeResult==0?"success":"fail");
                //check status
                if (executeResult == 0 && checkIpMMDBSample(parserParams)) {
                    logger.info("ip locate library built success");
                    XxlJobLogger.log("ip locate library built success");

                    if (parserParams.getPushKnowledge()) {
                        //push knowledge
                        int successCount = 0;
                        Map<String, String> historyMMDBInfo = historyMMDBInfo(parserParams);
                        for (KnowledgeBase knowledgeBase : parserParams.getKnowledgeBaseList()) {

                            File file = new File(StringUtil.join(parserParams.getPath(), File.separator, knowledgeBase.getName(), ".", knowledgeBase.getFormat()));
                            String newFile = DigestUtil.sha256Hex(file);
                            String historyFile = historyMMDBInfo.get(knowledgeBase.getName());
                            logger.info("ip locate library name {} ,new file {}, history file {}",knowledgeBase.getName(), newFile, historyFile);
                            XxlJobLogger.log("ip locate library name {} ,new file {}, history file {}",knowledgeBase.getName(),newFile,historyFile);
                            if (!newFile.equals(historyFile)) {
                                knowledgeBase.setSha256(newFile);
                                String resultStr = HttpClientUtils.httpPostFile(parserParams.getKnowledgeBaseServer(), file, BeanUtil.beanToMap(knowledgeBase));
                                logger.info("ip locate library push knowledge result: {},\n message {}", knowledgeBase, resultStr);
                                XxlJobLogger.log("ip locate library push knowledge result: {},\n message {}", knowledgeBase, resultStr);
                                if (!"-1".equals(resultStr)) {
                                    Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                                    if (result.get("status") != null && "200".equals(String.valueOf(result.get("status")))) {
                                        successCount++;
                                    }
                                }
                            } else {
                                successCount++;
                                logger.info("ip locate library {} has no changed, so it is not pushed to the knowledge base", knowledgeBase.getName());
                                XxlJobLogger.log("ip locate library {} has no changed, so it is not pushed to the knowledge base", knowledgeBase.getName());
                            }
                        }
                        if(successCount==parserParams.getKnowledgeBaseList().size()){
                            return ReturnT.SUCCESS;
                        }

                    } else {
                        return ReturnT.SUCCESS;
                    }
                }
            } catch (Exception e) {
                XxlJobLogger.log("ip locate library exception {}, retry {} ", e.getMessage(), i);
                logger.error("ip locate library exception {}, retry {} ", e.getMessage(), i);
            }
        }

        return ReturnT.FAIL;
    }

    /**
     * 验证历史文件sha256
     * @param ipLocateLibraryParam
     * @return
     */
    private Map<String, String> historyMMDBInfo(IpLocateLibraryParam ipLocateLibraryParam) {

        String result = HttpClientUtils.httpGet(ipLocateLibraryParam.getKnowledgeBaseServer());
        Object extract = JSONPath.extract(result, "$.data[version=='latest']");
        JSONArray objects = JSONUtil.parseArray(extract);
        List<KnowledgeBase> infos = JSONUtil.toList(objects, KnowledgeBase.class);

        Map<String, String> historyInfo = new HashMap<>();
        for (KnowledgeBase info : infos) {
            historyInfo.put(info.getName(), info.getSha256());
        }

        return historyInfo;

    }

    /**
     * 检查脚本运行结果和抽样ip是否打到包里
     *
     * @param parserParams
     * @return
     */
    public static boolean checkIpMMDBSample(IpLocateLibraryParam parserParams) throws Exception {

        String builtStatusFile = "built-status.json";
        String userStatusFile = "user-status.json";

        String path = parserParams.getPath().concat(File.separator);
        MMDBStatus builtMMDBStatus = JobUtil.readJsonFile(path + builtStatusFile, MMDBStatus.class);
        Integer builtStatus = builtMMDBStatus.getStatus();
        JobUtil.infoLog("built ip locate library result: status {}, message {}", builtMMDBStatus.getStatus(), builtMMDBStatus.getMessage());

        MMDBStatus userMMDBStatus = JobUtil.readJsonFile(path + userStatusFile, MMDBStatus.class);
        Integer userStatus = userMMDBStatus.getStatus();
        JobUtil.infoLog("user ip locate library result: status {}, message {}", userMMDBStatus.getStatus(), userMMDBStatus.getMessage());
        if (200 != builtStatus || 200 != userStatus) {
            return false;
        }
        String v4Built = "";
        String v6Built = "";
        String v4User = "";
        String v6User = "";
        boolean builtContains =  false;
        boolean userContains =  false;
        for (KnowledgeBase knowledgeBase : parserParams.getKnowledgeBaseList()) {
            String filePath = StringUtil.join(path, knowledgeBase.getName(), ".", knowledgeBase.getFormat());
            JobUtil.infoLog("ip locate library load path {} ", filePath);
            if (knowledgeBase.getName().contains("v4_built")) {
                v4Built = filePath;
                builtContains= true;
            } else if (knowledgeBase.getName().contains("v6_built")) {
                v6Built = filePath;
                builtContains= true;
            } else if (knowledgeBase.getName().contains("v4_user")) {
                v4User = filePath;
                userContains = true;
            } else if (knowledgeBase.getName().contains("v6_user")) {
                v6User = filePath;
                userContains = true;
            }
        }
        //测试抽样数据能查到结果说明打包成功
        IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
                .loadDataFileV4(v4Built)
                .loadDataFileV6(v6Built)
                .loadDataFilePrivateV4(v4User)
                .loadDataFilePrivateV6(v6User)
                .build();
        if(builtContains){
            List<String> builtIpSampleList = Stream.of(builtMMDBStatus.getIpv4Sample(),
                    builtMMDBStatus.getIpv6Sample())
                    .flatMap(Collection::stream).collect(Collectors.toList());
            for (String sampleIp : builtIpSampleList) {
                if (ObjectUtil.isEmpty(ipLookup.latLngLookup(sampleIp))) {
                    JobUtil.infoLog("built ip locate library sample validate fail, sampleIp {}, latLng {}", sampleIp, ipLookup.latLngLookup(sampleIp));
                    return false;
                }
            }
        }
        if(userContains){
            List<String> userIpSampleList = Stream.of(
                    userMMDBStatus.getIpv4Sample(),
                    userMMDBStatus.getIpv6Sample())
                    .flatMap(Collection::stream).collect(Collectors.toList());
            for (String sampleIp : userIpSampleList) {
                if (ObjectUtil.isEmpty(ipLookup.latLngLookup(sampleIp))) {
                    JobUtil.infoLog("user ip locate library sample validate fail, sampleIp {}, latLng {}", sampleIp, ipLookup.latLngLookup(sampleIp));
                    return false;
                }
            }
        }
        return true;
    }

    private IpLocateLibraryParam parserParams(String params) {
        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is empty!");
            return null;
        }

        IpLocateLibraryParam param = JSONUtil.toBean(params, IpLocateLibraryParam.class);

        if (ObjectUtil.isNull(param)) {
            XxlJobLogger.log("params is not format json ! ");
            logger.info("params is not format json ! ");
            return null;
        }
        if (ObjectUtil.isEmpty(param.getCommand())) {
            XxlJobLogger.log("command is empty ! ");
            logger.info("command is empty ! ");
            return null;
        }
        if (ObjectUtil.isNull(param.getPath())) {
            XxlJobLogger.log("path is null ! ");
            logger.info("path is null ! ");
            return null;
        }
        if (ObjectUtil.isNull(param.getRetry())) {
            XxlJobLogger.log("retry is null ! ");
            logger.info("retry is null ! ");
            return null;
        }
        if (ObjectUtil.isNull(param.getPushKnowledge())) {
            XxlJobLogger.log("pushKnowledge is null ! ");
            logger.info("pushKnowledge is null ! ");
            return null;
        }
        if (ObjectUtil.isNull(param.getKnowledgeBaseServer())) {
            XxlJobLogger.log("knowledgeServer is null ! ");
            logger.info("knowledgeServer is null ! ");
            return null;
        }
        if (ObjectUtil.isEmpty(param.getKnowledgeBaseList())) {
            XxlJobLogger.log("knowledgeList is null ! ");
            logger.info("knowledgeServer is null ! ");
            return null;
        }
        return param;
    }
}
