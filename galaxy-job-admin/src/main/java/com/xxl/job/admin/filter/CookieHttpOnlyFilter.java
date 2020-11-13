package com.xxl.job.admin.filter;


import com.xxl.job.admin.service.impl.AdminBizImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;


/**
 * @Date: 2020-11-13 12:05
 * @Author : liuyongqiang
 * @ClassName : CookieHttpOnlyFilter
 * @Description : 解决检测到会话cookie中缺少HttpOnly属性
 */
@Component
public class CookieHttpOnlyFilter implements Filter {

    private static Logger logger = LoggerFactory.getLogger(CookieHttpOnlyFilter.class);

    @Override
    public void init(FilterConfig filterConfig) {
        logger.info("Initializing CookieHttpOnlyFilter");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        Cookie[] cookies = req.getCookies();

        if (cookies != null) {
            for (Cookie cookie : cookies) {
                String value = cookie.getValue();
                StringBuilder builder = new StringBuilder();
                builder.append("JSESSIONID=" + value + "; ");
                builder.append("Secure; ");
                builder.append("HttpOnly; ");

                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.HOUR, 1);

                Date date = cal.getTime();
                Locale locale = Locale.CHINA;
                SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss",
                        locale);
                builder.append("Expires=" + sdf.format(date));
                resp.setHeader("Set-Cookie", builder.toString());
            }
            filterChain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {

    }
}
