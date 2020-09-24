package com.xxl.job.admin.controller;

import com.xxl.job.admin.controller.annotation.PermissionLimit;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Date: 2020-09-24 18:11
 * @Author : liuyongqiang
 * @ClassName : TestCaseController
 * @Description : 测试案例接口
 */
@RestController
@RequestMapping("testCase")
public class TestCaseController {

    private static Logger logger = LoggerFactory.getLogger(TestCaseController.class);

    @PermissionLimit(limit = false)
    @GetMapping("logPolicyTest")
    public ReturnT<String> logPolicyTest() {
        logger.info("logPolicyTest execute>>>");
        return ReturnT.SUCCESS;
    }

}
