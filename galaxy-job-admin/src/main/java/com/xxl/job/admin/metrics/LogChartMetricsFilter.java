package com.xxl.job.admin.metrics;

import com.xxl.job.admin.service.XxlJobService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.util.DateUtil;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import javax.annotation.Resource;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
public class LogChartMetricsFilter implements Filter {

    private static Tag commonTag = Tag.of("jobcustom", "dashboardInfo");

    private MeterRegistry registry;

    public LogChartMetricsFilter(MeterRegistry registry) {
        this.registry = registry;
    }

    @Resource
    XxlJobService xxlJobService;


    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) servletRequest;
        filterChain.doFilter(servletRequest, servletResponse);
        if (req.getRequestURI().contains("/prometheus")) {
            registryDashboardInfo();
            registryChartInfo();
        }
    }

    /**
     * 更新info指标
     */
    private void registryDashboardInfo() {
        //去除容器中值防止影响
        Map<String, Object> map = xxlJobService.dashboardInfo();
        registryMetrics(map);
    }

    /**
     * 更新曲线图表指标
     */
    private void registryChartInfo() {
        //只查询今天的数据
        ReturnT<Map<String, Object>> mapReturnT = xxlJobService.chartInfo(DateUtil.parseDate(DateUtil.formatDate(new Date())), new Date());
        Map<String, Object> content = mapReturnT.getContent();
        //删除不需要的指标
        content.remove("triggerDayList");
        registryMetrics(content);
    }

    private void registryMetrics(Map<String, Object> map) {
        if (!ObjectUtils.isEmpty(map)) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                //去除容器中值防止影响
                while (true) {
                    Meter meter = registry.find(entry.getKey()).meter();
                    if (ObjectUtils.isEmpty(meter)) {
                        break;
                    }
                    registry.remove(meter);
                }
                int value = 0;
                if (!ObjectUtils.isEmpty(entry.getValue())) {
                    if (entry.getValue() instanceof List) {
                        value = Integer.parseInt(String.valueOf(((List) entry.getValue()).get(0)));
                    } else {
                        value = Integer.parseInt(String.valueOf(entry.getValue()));
                    }
                }
                registry.gauge(entry.getKey(), Tags.of(commonTag, Tag.of("info", entry.getKey())), value);
            }
        }
    }

}