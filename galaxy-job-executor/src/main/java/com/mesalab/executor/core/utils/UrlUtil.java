package com.mesalab.executor.core.utils;

import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

/**
 * @description:
 * @author: zhq
 * @create: 2021-02-08
 **/
public class UrlUtil {

    public static String getUrl(String url) {
        return UrlBuilder.ofHttp(url, StandardCharsets.UTF_8).toString();
    }

    public static String getUrl(String... url) {
        return UrlBuilder.ofHttp(StringUtils.join(url), StandardCharsets.UTF_8).toString();
    }
    public static String getUrl(Object url) {
        return UrlBuilder.ofHttp(StrUtil.toString(url), StandardCharsets.UTF_8).toString();
    }

}
