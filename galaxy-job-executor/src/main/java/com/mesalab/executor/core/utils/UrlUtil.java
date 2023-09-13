package com.mesalab.executor.core.utils;

import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

//    public static String getParamByUrl(String url, String name) {
//
//        url += "&";
//        String pattern = "(\\?|&){1}#{0,1}" + name + "=[a-zA-Z0-9]*(&{1})";
//        Pattern r = Pattern.compile(pattern);
//        Matcher m = r.matcher(url);
//
//        if (m.find()) {
//            return m.group(0).split("=")[1].replace("&", "");
//        } else {
//            return null;
//        }
//    }

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

    public static String removeParamFromUrl(String url, String keyToRemove) {
        try {
            URI uri = new URI(url);
            String newQuery = uri.getQuery();
            if (newQuery != null && !newQuery.isEmpty()) {
                newQuery = Arrays.stream(newQuery.split("&"))
                        .map(param -> param.split("="))
                        .filter(keyValue -> keyValue.length == 2 && !keyValue[0].equals(keyToRemove))
                        .map(keyValue -> keyValue[0] + "=" + keyValue[1])
                        .collect(Collectors.joining("&"));

                URI newUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), newQuery, uri.getFragment());
                return newUri.toString();
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return url;
    }
    public static String removeParamFromUrl(String url, List<String> keysToRemove) {
        try {
            URI uri = new URI(url);
            String newQuery = uri.getQuery();
            if (newQuery != null && !newQuery.isEmpty()) {
                newQuery = Arrays.stream(newQuery.split("&"))
                        .map(param -> param.split("="))
                        .filter(keyValue -> keyValue.length == 2 && !keysToRemove.contains(keyValue[0]))
                        .map(keyValue -> keyValue[0] + "=" + keyValue[1])
                        .collect(Collectors.joining("&"));

                URI newUri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), newQuery, uri.getFragment());
                return newUri.toString();
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return url;
    }
    public static String getParamByUrl(String urlString,String keyStr) {
        Map<String, String> params = new HashMap<>();
        try {
            URL url = new URL(urlString);
            String query = url.getQuery();
            if (query != null) {
                String[] pairs = query.split("&");
                for (String pair : pairs) {
                    String[] keyValue = pair.split("=");
                    if (keyValue.length == 2) {
                        String key = URLDecoder.decode(keyValue[0], Constant.UTF_8);
                        String value = URLDecoder.decode(keyValue[1], Constant.UTF_8);
                        params.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  params.get(keyStr);
    }
}
