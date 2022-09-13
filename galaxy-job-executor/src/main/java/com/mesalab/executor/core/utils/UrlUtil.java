package com.mesalab.executor.core.utils;

import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static String getParamByUrl(String url, String name) {

        url += "&";
        String pattern = "(\\?|&){1}#{0,1}" + name + "=[a-zA-Z0-9]*(&{1})";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(url);

        if (m.find()) {
            return m.group(0).split("=")[1].replace("&", "");
        } else {
            return null;
        }
    }

    public static String appendUrl(String url, Map data) {

        String newUrl = url;

        StringBuffer param = new StringBuffer();

        for (Object key : data.keySet()) {
            param.append(key + "=" + data.get(key).toString() + "&");
        }
        String paramStr = param.toString();
        paramStr = paramStr.substring(0, paramStr.length() - 1);

        if (newUrl.indexOf("?") >= 0) {

            newUrl += "&" + paramStr;

        } else {
            newUrl += "?" + paramStr;
        }

        return newUrl;
    }

}
