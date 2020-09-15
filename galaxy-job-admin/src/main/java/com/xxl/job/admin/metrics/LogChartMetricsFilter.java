package com.xxl.job.admin.metrics;

import com.xxl.job.admin.service.XxlJobService;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class LogChartMetricsFilter implements Filter {

    private static Tag commonTag = Tag.of("jobcustom", "dashboardInfo");
    private static List<String> chartKeys = Arrays.asList("jobInfoCount", "jobLogCount", "jobLogSuccessCount", "executorCount");

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
        registryMetrics(req);
    }

    /**
     * 更新指标
     * @param req
     */
    private void registryMetrics(HttpServletRequest req) {
        if (req.getRequestURI().contains("/prometheus")) {
            //去除容器中值防止影响
            Map<String, Object> map = xxlJobService.dashboardInfo();
            for (String chartKey : chartKeys) {
                while (true) {
                    Meter meter = registry.find(chartKey).meter();
                    if (!ObjectUtils.isEmpty(meter)) {
                        registry.remove(meter);
                    } else {
                        break;
                    }
                }
                Tag tag = Tag.of("info", chartKey);
                registry.gauge(chartKey, Tags.of(commonTag, tag), Integer.parseInt(String.valueOf(map.get(chartKey))));
            }
        }
    }
}