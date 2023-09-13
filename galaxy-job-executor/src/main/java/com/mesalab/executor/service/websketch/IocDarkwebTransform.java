package com.mesalab.executor.service.websketch;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.TypeUtils;
import com.mesalab.executor.core.utils.ValidationUtils;
import com.mesalab.executor.pojo.IocDarkWeb;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class IocDarkwebTransform {

    private static Log logger = Log.get();

    /**
     * tor
     * i2p
     * mtproxy
     * obfs4
     * snowflake
     * generalbridge
     *
     * @param sourceData
     */
    public static List<Map> builderIocDarkweb(Map sourceData, String apiType) {
        List<Map> list = new ArrayList<>();
        try {
            if(ObjectUtils.isEmpty(sourceData)){
                return list;
            }
            Map result = (Map) sourceData.get("result");
            List<Map> nodeList = (List<Map>) result.get("node_list");
            JobUtil.infoLog(apiType + " record size: " + nodeList.size());
            for (Map node : nodeList) {
                String ipv6s = getIp(node, "ipv6");
                //location
                Map detail = (Map) node.get("detail");
                Map location = (Map) detail.get("location");

                String isp = TypeUtils.castToString(location.get("isp"));
                String asn = TypeUtils.castToString(location.get("as_number"));
                String country = TypeUtils.castToString(location.get("country"));

                String city = TypeUtils.castToString(location.get("city"));
                String latitude = TypeUtils.castToString(location.get("latitude"));
                String longtitude = TypeUtils.castToString(location.get("longtitude"));

                Long firstAppearTime = TypeUtils.castToLong(node.get("first_appear_time")) / 1000;
                Long lastAppearTime = TypeUtils.castToLong(node.get("last_appear_time")) / 1000;

                IocDarkWeb iocDarkWeb = IocDarkWeb.builder()
                        .nodeType(apiType)
                        .ipv6(ipv6s)
                        .isp(isp)
                        .asn(asn)
                        .country(country)
                        .city(city)
                        .latitude(latitude)
                        .longtitude(longtitude)
                        .firstAppearTime(firstAppearTime)
                        .lastAppearTime(lastAppearTime)
                        .createTime(DateUtil.currentSeconds())
                        .updateTime(DateUtil.currentSeconds())
                        .iocType("ip").build();

                switch (apiType) {
                    case "tor":
                        //tor
                        String ip = getIp(node, "ip");
                        iocDarkWeb.setIocValue(ObjectUtil.isEmpty(ip) ? ipv6s : ip);
                        torDetail(node, detail, iocDarkWeb);
                        break;
                    case "i2p":
                        ip = getIp(node, "ipv4");
                        iocDarkWeb.setIocValue(ObjectUtil.isEmpty(ip) ? ipv6s : ip);
                        i2pDetail(detail, iocDarkWeb);
                        break;
                    case "mtproxy":
                        //特殊处理，mtproxy节点中server字段支持域名和ip两种形式
                        String server = TypeUtils.castToString(node.get("server"));
                        iocDarkWeb.setIocType(ValidationUtils.isValidIp(server) ? "ip" : "domain");
                        iocDarkWeb.setIocValue(server);
                        mtproxyDetail(detail, iocDarkWeb);
                        break;
                    case "obfs4":
                        ip = getIp(node, "ipv4");
                        iocDarkWeb.setIocValue(ObjectUtil.isEmpty(ip) ? ipv6s : ip);
                        obfs4Detail(detail, iocDarkWeb);
                        break;
                    case "snowflake":
                        ip = getIp(node, "ipv4");
                        iocDarkWeb.setIocValue(ObjectUtil.isEmpty(ip) ? ipv6s : ip);
                        snowflakeDetail(detail, iocDarkWeb);
                        break;
                }

                list.add(BeanUtil.beanToMap(iocDarkWeb, true, false));
            }

            logger.info("builder iocDarkweb success");
        } catch (Exception e) {
            logger.error(e);
        }
        return list;
    }

    private static String getIp(Map node, String key) {
        Object ip = node.get(key);
        if (ip instanceof List) {
            return String.join(",", TypeUtils.castToJavaBean(ip, List.class));
        } else {
            return TypeUtils.castToString(Optional.ofNullable(ip).orElse(""));
        }
    }

    private static void snowflakeDetail(Map detail, IocDarkWeb iocDarkWeb) {
        setIntegerProperty(detail, "port", iocDarkWeb::setSnowflakePort);
    }

    private static void obfs4Detail(Map detail, IocDarkWeb iocDarkWeb) {
        setIntegerProperty(detail, "port", iocDarkWeb::setObfs4Port);
        setStringProperty(detail, "fingerprint", iocDarkWeb::setObfs4Fingerprint);
        setStringProperty(detail, "cert", iocDarkWeb::setObfs4Cert);
        setIntegerProperty(detail, "iatMode", iocDarkWeb::setObfs4IatMode);
    }

    private static void mtproxyDetail(Map detail, IocDarkWeb iocDarkWeb) {
        setIntegerProperty(detail, "port", iocDarkWeb::setMtproxyPort);
        setStringProperty(detail, "secret", iocDarkWeb::setMtproxySecret);
    }

    private static void i2pDetail(Map detail, IocDarkWeb iocDarkWeb) {
        setStringProperty(detail, "hash", iocDarkWeb::setI2pHash);
        setStringProperty(detail, "method", iocDarkWeb::setI2pMethod);
        setStringProperty(detail, "version", iocDarkWeb::setI2pVersion);
        setBooleanProperty(detail, "isFloodfill", iocDarkWeb::setI2pIsFloodfill);
        setStringProperty(detail, "width", iocDarkWeb::setI2pBandwidth);
        setBooleanProperty(detail, "isHidden", iocDarkWeb::setI2pIsHidden);
        setBooleanProperty(detail, "isReachable", iocDarkWeb::setI2pIsReachable);
        setStringProperty(detail, "publish", iocDarkWeb::setI2pPublishTime);
    }

    private static void torDetail(Map node, Map detail, IocDarkWeb iocDarkWeb) {
        setStringProperty(node, "fingerprint", iocDarkWeb::setTorFingerprint);
        setIntegerProperty(node, "dir_port", iocDarkWeb::setTorDirPort);
        setIntegerProperty(node, "or_port", iocDarkWeb::setTorOrPort);
        setStringListProperty(detail, "flags", iocDarkWeb::setTorFlags);
        setStringProperty(detail, "nickname", iocDarkWeb::setTorNickname);
        setStringProperty(detail, "exit_policy", iocDarkWeb::setTorExitPolicy);
        setStringProperty(detail, "host_system", iocDarkWeb::setOperatingSystem);
        setStringProperty(detail, "tor_virsion", iocDarkWeb::setTorVersion);
    }

    public static void setStringProperty(Map<String, Object> map, String key, Consumer<String> setter) {
        String value = TypeUtils.castToString(map.get(key));
        if (value != null) {
            setter.accept(value);
        }
    }

    public static void setIntegerProperty(Map<String, Object> map, String key, Consumer<Integer> setter) {
        Integer value = TypeUtils.castToInt(map.get(key));
        if (value != null) {
            setter.accept(value);
        }
    }

    public static void setBooleanProperty(Map<String, Object> map, String key, Consumer<Boolean> setter) {
        Boolean value = TypeUtils.castToBoolean(map.get(key));
        if (value != null) {
            setter.accept(value);
        }
    }

    public static void setStringListProperty(Map<String, Object> map, String key, Consumer<String> setter) {
        List<String> valueList = TypeUtils.castToJavaBean(map.get(key), List.class);
        if (valueList != null && !valueList.isEmpty()) {
            setter.accept(String.join(",", valueList));
        }
    }

}
