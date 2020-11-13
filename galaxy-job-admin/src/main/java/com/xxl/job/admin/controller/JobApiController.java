package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import org.apache.commons.lang.CharEncoding;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
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
    @RequestMapping("/{uri}")
    @ResponseBody
    @PermissionLimit(limit = false)
    public ReturnT<String> api(HttpServletRequest request, @PathVariable("uri") String uri, @RequestBody(required = false) String data) {
        // valid
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
        }
        if (HtmlUtils.htmlEscape(uri, CharEncoding.UTF_8) == null || HtmlUtils.htmlEscape(uri, CharEncoding.UTF_8).trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
        }
        if (XxlJobAdminConfig.getAdminConfig().getAccessToken() != null
                && XxlJobAdminConfig.getAdminConfig().getAccessToken().trim().length() > 0
                && !XxlJobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // services mapping
        if ("callback".equals(HtmlUtils.htmlEscape(uri, CharEncoding.UTF_8))) {
            List<HandleCallbackParam> callbackParamList = GsonTool.fromJson(HtmlUtils.htmlEscape(data, CharEncoding.UTF_8), List.class, HandleCallbackParam.class);
            return adminBiz.callback(callbackParamList);
        } else if ("registry".equals(HtmlUtils.htmlUnescape(uri))) {
            RegistryParam registryParam = GsonTool.fromJson(HtmlUtils.htmlEscape(data, CharEncoding.UTF_8), RegistryParam.class);
            return adminBiz.registry(registryParam);
        } else if ("registryRemove".equals(HtmlUtils.htmlEscape(uri, CharEncoding.UTF_8))) {
            RegistryParam registryParam = GsonTool.fromJson(HtmlUtils.htmlEscape(data, CharEncoding.UTF_8), RegistryParam.class);
            return adminBiz.registryRemove(registryParam);
        } else {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + HtmlUtils.htmlEscape(uri, CharEncoding.UTF_8) + ") not found.");
        }

    }

}
