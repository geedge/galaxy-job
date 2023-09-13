package com.mesalab.executor.core.utils;

import org.apache.log4j.Logger;
import sun.net.util.IPAddressUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidationUtils {
    private static final Logger LOG = Logger.getLogger(ValidationUtils.class);

    public static List<String> getCheckedFqdns(List<String> fqdns) {
        List<String> res = new ArrayList<>();
        for (String fqdn : fqdns) {
            //去端口号
            fqdn = fqdn.split(":")[0];
            // 去重 & 校验
            if (isValidDomain(fqdn) && !res.contains(fqdn)) {
                res.add(fqdn.toLowerCase());
            } else {
                LOG.debug("Bad or duplicated fqdn:" + fqdn);
            }
        }
        return res;
    }

    public static List<String> getCheckedIps(List<String> ipList) {
        List<String> res = new ArrayList<>();
        for (String ip : ipList) {
            //去端口号
            ip = ip.split(":")[0];
            // 去重 & 校验
            if (isValidIp(ip) && !res.contains(ip)) {
                res.add(ip.toLowerCase());
            } else {
                LOG.debug("Bad or duplicated fqdn:" + ip);
            }
        }
        return res;
    }

    public static boolean isValidIp(String ip) {
        boolean iPv4LiteralAddress = IPAddressUtil.isIPv4LiteralAddress(ip);
        boolean iPv6LiteralAddress = IPAddressUtil.isIPv6LiteralAddress(ip);
        return iPv4LiteralAddress || iPv6LiteralAddress;
    }

    private static boolean isValidDomain(String str) {
        String regex = "^((?!-)[A-Za-z0-9-_]"
                + "{1,63}(?<!-)\\.)"
                + "+[A-Za-z]{2,6}";
        Pattern p = Pattern.compile(regex);

        if (str == null) {
            return false;
        }
        Matcher m = p.matcher(str);
        return m.matches();
    }


}
