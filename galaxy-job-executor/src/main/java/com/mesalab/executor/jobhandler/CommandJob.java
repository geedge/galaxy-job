package com.mesalab.executor.jobhandler;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.alibaba.fastjson.JSONPath;
import com.geedgenetworks.utils.StringUtil;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.pojo.IpLocateLibraryParam;
import com.mesalab.executor.pojo.KnowledgeBase;
import com.mesalab.executor.pojo.MMDBStatus;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
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
     * @param params
     * @return
     */
    @XxlJob("ipLocateLibraryJobHandler")
    public ReturnT<String> ipLocateLibrary(String params) {

        IpLocateLibraryParam parserParams = parserParams(params);
        if (ObjectUtils.isEmpty(parserParams)) {
            JobUtil.infoLog("params parser error , params is {}", params);
            return IJobHandler.FAIL;
        }
        for (int i = 1; i <= parserParams.getRetry(); i++) {
            try {
                int executeResult = JobUtil.executeCommand(parserParams.getCommand());
                JobUtil.infoLog("ip locate library perl {} execute result {}", i, executeResult == 0 ? "success" : "fail");
                //check status
                if (executeResult == 0 && checkIpSample(parserParams)) {
                    JobUtil.infoLog("ip locate library built success");
                    if (parserParams.getPushKnowledge()) {
                        //push knowledge
                        if (pushKnowledge(parserParams) == parserParams.getKnowledgeBaseList().size()) {
                            return ReturnT.SUCCESS;
                        }
                    } else {
                        return ReturnT.SUCCESS;
                    }
                }
            } catch (Exception e) {
                JobUtil.errorLog("ip locate library exception {}, retry {} ", e.getMessage(), i);
            }
        }

        return ReturnT.FAIL;
    }

    private int pushKnowledge(IpLocateLibraryParam parserParams) {
        int successCount = 0;
        Map<String, String> historyMMDBInfo = historyMMDBInfo(parserParams);
        for (KnowledgeBase knowledgeBase : parserParams.getKnowledgeBaseList()) {

            File file = new File(StringUtil.join(parserParams.getPath(), File.separator, knowledgeBase.getName(), ".", knowledgeBase.getFormat()));
            String newFile = DigestUtil.sha256Hex(file);
            String historyFile = historyMMDBInfo.get(knowledgeBase.getName());
            JobUtil.infoLog("ip locate library name {} ,new file {}, history file {}", knowledgeBase.getName(), newFile, historyFile);
            if (!newFile.equals(historyFile)) {
                knowledgeBase.setSha256(newFile);
                String resultStr = HttpClientUtils.httpPostFile(parserParams.getKnowledgeBaseServer(), file, BeanUtil.beanToMap(knowledgeBase));
                JobUtil.infoLog("ip locate library push knowledge result: {},\n message {}", knowledgeBase, resultStr);
                if (!"-1".equals(resultStr)) {
                    Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
                    if (result.get("status") != null && "200".equals(String.valueOf(result.get("status")))) {
                        successCount++;
                    }
                }
            } else {
                successCount++;
                JobUtil.infoLog("ip locate library {} not updated.", knowledgeBase.getName());
            }
        }
        return successCount;
    }

    /**
     * 验证历史文件sha256
     *
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
    public boolean checkIpSample(IpLocateLibraryParam parserParams) throws Exception {

        final String builtinStatusFileName = "builtin-status.json";
        final String userStatusFileName = "user-status.json";
        String path = parserParams.getPath().concat(File.separator);

        MMDBStatus builtinStatusFile = JobUtil.readJsonFile(path + builtinStatusFileName, MMDBStatus.class);
        MMDBStatus userStatusFile = JobUtil.readJsonFile(path + userStatusFileName, MMDBStatus.class);
        JobUtil.infoLog("user ip locate library result: status {}, message {}. built ip locate library result: status {}, message {}",
                userStatusFile.getStatus(), userStatusFile.getMessage(), builtinStatusFile.getStatus(), builtinStatusFile.getMessage());
        if (200 != builtinStatusFile.getStatus() || 200 != userStatusFile.getStatus()) {
            return false;
        }
        String builtinPath = "";
        String userDefinedPath = "";
        boolean builtinContains = false;
        boolean userContains = false;
        for (KnowledgeBase knowledgeBase : parserParams.getKnowledgeBaseList()) {
            String filePath = StringUtil.join(path, knowledgeBase.getName(), ".", knowledgeBase.getFormat());
            JobUtil.infoLog("ip locate library load path {} ", filePath);
            if (knowledgeBase.getName().contains("built")) {
                builtinPath = filePath;
                builtinContains = true;
            } else if (knowledgeBase.getName().contains("user")) {
                userDefinedPath = filePath;
                userContains = true;
            }
        }
        //测试抽样数据能查到结果说明打包成功
        List<String> ipSamples = Stream.of(builtinStatusFile.getIpSample(), userStatusFile.getIpSample())
                .flatMap(Collection::stream).collect(Collectors.toList());
        if (builtinContains || userContains) {
            return JobUtil.ipKnowledgeNotEmpty(builtinPath, userDefinedPath, ipSamples);
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
