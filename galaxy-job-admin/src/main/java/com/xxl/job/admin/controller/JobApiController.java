package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.HtmlUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Created by xuxueli on 17/5/10.
 */
@Controller
@RequestMapping("/api")
public class JobApiController {

    @Resource
    private AdminBiz adminBiz;

    /**
     * api
     *
     * @param uri
     * @param data
     * @return
     */
    @RequestMapping(value = "/{uri}",method = RequestMethod.POST)
    @ResponseBody
    @PermissionLimit(limit = false)
    public ReturnT<String> api(HttpServletRequest request, @PathVariable("uri") String uri, @RequestBody(required = false) String data) {

        if (uri == null || uri.trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "uri-mapping empty.");
        }
        if (XxlJobAdminConfig.getAdminConfig().getAccessToken() != null
                && XxlJobAdminConfig.getAdminConfig().getAccessToken().trim().length() > 0
                && !XxlJobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // services mapping
        if ("callback".equals(uri)) {
            List<HandleCallbackParam> callbackParamList = GsonTool.fromJson(data, List.class, HandleCallbackParam.class);
            for (int i = 0; i < callbackParamList.size(); i++) {
                HandleCallbackParam handleCallbackParam = callbackParamList.get(i);
                ReturnT<String> executeResult = handleCallbackParam.getExecuteResult();
                if(executeResult!=null&&executeResult.getContent()!=null){
                    executeResult.setContent(HtmlUtils.htmlEscape(executeResult.getContent()));
                }
                handleCallbackParam.setExecuteResult(executeResult);
                callbackParamList.set(i,handleCallbackParam);
            }
            return adminBiz.callback(callbackParamList);
        } else if ("registry".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            return adminBiz.registry(htmlEscape(registryParam));
        } else if ("registryRemove".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            return adminBiz.registryRemove(htmlEscape(registryParam));
        } else {
            return new ReturnT<>(ReturnT.FAIL_CODE, "unkonw uri");
        }

    }

    private RegistryParam htmlEscape(RegistryParam registryParam) {
        registryParam.setRegistryGroup(HtmlUtils.htmlEscape(registryParam.getRegistryGroup(), "UTF-8"));
        registryParam.setRegistryKey(HtmlUtils.htmlEscape(registryParam.getRegistryKey()));
        registryParam.setRegistryValue(HtmlUtils.htmlEscape(registryParam.getRegistryValue()));
        return registryParam;
    }
}
