package com.mesalab.executor.jobhandler;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
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
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author: zhq
 * @date: 2023/11/8
 */
@Component
public class IpLocateLibraryJob {

    private Log logger = Log.get();

    /**
     * @author: zhq
     * @Description IP定位库
     * @param: [params]
     * @date: 2023/11/8
     */
    @XxlJob("ipLocateLibraryJobHandler")
    public ReturnT<String> ipLocateLibrary(String params) {

        IpLocateLibraryParam parserParams = parserParams(params);
        if (ObjectUtils.isEmpty(parserParams)) {
            JobUtil.infoLog("params parser error , params is {}", params);
            return IJobHandler.FAIL;
        }
        try {
            int successCount = 0;
            List<KnowledgeBase> knowledgeBaseList = parserParams.getKnowledgeBaseList();
            Map<String, String> historyFileInfo = historyFileInfo(parserParams);
            for (KnowledgeBase knowledgeBase : knowledgeBaseList) {

                String cmdStr = StrUtil.removeSuffix(StrUtil.trim(parserParams.getCommand()), "'") + " "
                        + ObjectUtils.defaultIfNull(knowledgeBase.getBuiltin(), "") + "' ";
                int executeResult = JobUtil.executeCommand(cmdStr);
                File newFile = new File(StringUtils.join(parserParams.getPath(), File.separator, knowledgeBase.getName(), ".", knowledgeBase.getFormat()));
                if (!checkExecuteResult(executeResult, newFile, knowledgeBase)) {
                    continue;
                }
                if (pushKnowledge(knowledgeBase, newFile, parserParams.getKnowledgeBaseServer(), historyFileInfo)) {
                    successCount++;
                }
            }

            if (successCount == knowledgeBaseList.size()) {
                JobUtil.infoLog("ip locate library job finished!");
                return ReturnT.SUCCESS;
            }
        } catch (Exception e) {
            JobUtil.errorLog("ip locate library exception {}", e.getMessage());
        }
        return ReturnT.FAIL;
    }

    private boolean checkExecuteResult(int executeResult, File newFile, KnowledgeBase knowledgeBase) throws Exception {
        if (executeResult != 0) {
            JobUtil.errorLog("{} built failed, perl execute result {}.", newFile.getName(), executeResult);
            return false;
        }
        if (!newFile.exists() || newFile.length() == 0) {
            JobUtil.errorLog("{} built failed, file does not exist.", newFile.getName(), executeResult);
            return false;
        }
        if (!checkIpSample(knowledgeBase, newFile)) {
            return false;
        }
        JobUtil.infoLog("{} built success, file length {} bytes.", knowledgeBase.getName(), newFile.length());
        return true;

    }

    private boolean pushKnowledge(KnowledgeBase knowledgeBase, File newFile, String knowledgeBasServer, Map<String, String> historyFileInfo) {

        String newFileSha256 = DigestUtil.sha256Hex(newFile);
        if (newFileSha256.equals(historyFileInfo.get(knowledgeBase.getName()))) {
            JobUtil.infoLog("{} has not changed.\n", knowledgeBase.getName());
            return true;
        }
        knowledgeBase.setSha256(newFileSha256);
        String resultStr = HttpClientUtils.httpPostFile(knowledgeBasServer, newFile, BeanUtil.beanToMap(knowledgeBase));
        JobUtil.infoLog("push knowledge info: {}", knowledgeBase);
        JobUtil.infoLog("push knowledge response: {}\n", resultStr);
        if (!"-1".equals(resultStr)) {
            Map<String, Object> result = JSONUtil.toBean(resultStr, Map.class);
            if (result.get("status") != null && "200".equals(String.valueOf(result.get("status")))) {
                return true;
            }
        }

        return false;
    }

    /**
     * 验证历史文件sha256
     */
    private Map<String, String> historyFileInfo(IpLocateLibraryParam ipLocateLibraryParam) {

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
     */
    private boolean checkIpSample(KnowledgeBase knowledgeBase, File newFile) throws Exception {

        String workPath = newFile.getParent().concat(File.separator);
        MMDBStatus mmdbStatus = JobUtil.readJsonFile(workPath + knowledgeBase.getName() + "_status.json", MMDBStatus.class);
        if (HttpStatus.OK.value() != mmdbStatus.getStatus()) {
            JobUtil.errorLog("perl query failed result: status {}, message {}.", mmdbStatus.getStatus(), mmdbStatus.getMessage());
            return false;
        }
        if (mmdbStatus.getTotal() == 0 ) {
            JobUtil.errorLog("perl query failed result: total {}.", mmdbStatus.getTotal());
            return false;
        }
        String builtinPath = "";
        String userDefinedPath = "";
        String filePath = newFile.getAbsolutePath();
        if (knowledgeBase.getName().contains("built")) {
            builtinPath = filePath;
        } else if (knowledgeBase.getName().contains("user")) {
            userDefinedPath = filePath;
        }
        if (StringUtils.isNotEmpty(filePath)) {
            return JobUtil.ipKnowledgeNotEmpty(builtinPath, userDefinedPath, mmdbStatus.getIpSample());
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
            logger.info("knowledgeBaseList is null ! ");
            return null;
        }
        return param;
    }
}
