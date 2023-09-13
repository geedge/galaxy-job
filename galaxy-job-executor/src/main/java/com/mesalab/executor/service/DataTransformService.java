package com.mesalab.executor.service;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.core.utils.UrlUtil;
import com.mesalab.executor.core.utils.async.HttpKit;
import com.mesalab.executor.enums.DataTypeEnum;
import com.mesalab.executor.service.websketch.IocDarkwebTransform;
import com.xxl.job.core.log.XxlJobLogger;
import org.apache.commons.collections4.ListUtils;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @description:
 * @author: zhq
 * @create: 2022-03-30
 **/
@Service
public class DataTransformService {

    private Log logger = Log.get();

    private static final String SOFTWARE = "software";
    private static final String ASSOCIATED_GROUPS = "associated groups";
    private static final String TECHNIQUES_USED = "techniques used";
    private static final String SOURCE_NAME = "source name";
    private static final String SOURCE_ID = "source ID";
    private static final String TARGET_NAME = "target name";
    private static final String TARGET_ID = "target ID";

    /**
     * 业务转换
     *
     * @param params
     * @return Map<String, List>
     */
    public Map<String, Object> adapt(Map<String, String> params, Object sourceResult) throws Exception {
        Map<String, Object> transformResult = new HashMap<>();
        if (ObjectUtil.isEmpty(params.get("type"))) {
            return transformResult;
        }
        switch (params.get("type")) {
            case "websketch":
                transformResult.put("websketch", webSketch(sourceResult));
                break;
            case "websketch-fqdn-accessible":
                transformResult.put("websketch-fqdn-accessible", isFqdnAccessible(sourceResult, params));
                break;
            case "websketch-iocdarkweb":
                transformResult.put("websketch-iocdarkweb", convertToDarkweb(sourceResult, params));
                break;
            default:
                break;
        }
        return transformResult;
    }

    private List<Map> convertToDarkweb(Object sourceResult, Map<String, String> params) throws Exception {
        Map<String, Map> source = JSONUtil.toBean(JSON.toJSONString(sourceResult, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullStringAsEmpty), Map.class);
        Map sourceData = source.get(DataTypeEnum.HTTP.getName());
        List<Map> transformResult = IocDarkwebTransform.builderIocDarkweb(sourceData, params.get("transform_table"));
        return transformResult;

    }

    /**
     * 探测域名是否可访问
     *
     * @param params
     * @param sourceResult
     * @return
     */
    private List<Map> isFqdnAccessible(Object sourceResult, Map<String, String> params) throws Exception {
        Map<String, List> source = JSONUtil.toBean(JSON.toJSONString(sourceResult, SerializerFeature.WriteNullStringAsEmpty), Map.class);
        List<Map> mariadbResult = source.get(DataTypeEnum.MARIADB.getName());
        Vector<Map> transformResult = new Vector<>();
        //判断是否可达
        long start = System.currentTimeMillis();
        int threadNum = Integer.valueOf(StrUtil.toString(params.get("parallelism")));//线程数
        int partitionSize = (mariadbResult.size() / threadNum) > 0 ? mariadbResult.size() / threadNum : 1;
        List<List<Map>> split = ListUtil.split(mariadbResult, partitionSize);
        int t = split.size();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(t);
        logger.info("当前线程数为：" + t);
        // 用于计数线程是否执行完成
        CountDownLatch countDownLatch = new CountDownLatch(t);
        //1.已经探测过不用重复探测 2.探测结果与数据库相同不用更新
        Map uniqueFqdns = new ConcurrentHashMap();
        final String STATUS_CODE = "status_code";
        XxlJobLogger.log("fqdn accessible task starting ");
        logger.info("fqdn accessible task starting ");
        for (List<Map> mapList : split) {
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    for (Map map : mapList) {
                        int code = 0;
                        if (uniqueFqdns.containsKey(map.get("fqdn"))) {
                            code = Integer.valueOf(String.valueOf(uniqueFqdns.get("fqdn")));
                        } else {
                            code = HttpKit.getStatus(UrlUtil.getUrl(map.get("fqdn")));
                        }
                        uniqueFqdns.put(map.get("fqdn"), code);
                        if (ObjectUtil.isEmpty(map.get(STATUS_CODE))||Integer.valueOf(String.valueOf(map.get(STATUS_CODE))) != code) {
                            Map m = new ConcurrentHashMap();
                            m.put(STATUS_CODE, code);
                            m.put("id", map.get("id"));
                            transformResult.add(m);
                        }
                    }
                    countDownLatch.countDown();
                }
            });
        }
        try {
            logger.info("websketch-fqdn-accessible transform running...");
            XxlJobLogger.log("websketch-fqdn-accessibe transform running");
            countDownLatch.await();
            fixedThreadPool.shutdown();
        } catch (InterruptedException e) {
            logger.error(e);
        }
        //去掉重复无效
        logger.info("websketch-fqdn-accessible transform data {}, spend time {} ms", transformResult.size(), System.currentTimeMillis() - start);
        XxlJobLogger.log("websketch-fqdn-accessible transform data {}, spend time {} ms", transformResult.size(), System.currentTimeMillis() - start);
        return transformResult;

    }
    public List<Map> isFqdnAccessible(List<Map> mariadbResult , Map<String, String> params) throws Exception {
        Vector<Map> transformResult = new Vector<>();
        //判断是否可达
        long start = System.currentTimeMillis();
        int threadNum = Integer.valueOf(StrUtil.toString(Optional.ofNullable(params.get("parallelism")).orElse("5")));//线程数
        int partitionSize = (mariadbResult.size() / threadNum) > 0 ? mariadbResult.size() / threadNum : 1;
        List<List<Map>> split = ListUtil.split(mariadbResult, partitionSize);
        int t = split.size();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(t);
        logger.info("threads total count is ：" + t);
        // 用于计数线程是否执行完成
        CountDownLatch countDownLatch = new CountDownLatch(t);
        //1.已经探测过不用重复探测 2.探测结果与数据库相同不用更新
        Map uniqueFqdns = new ConcurrentHashMap();
        final String STATUS_CODE = "status_code";
        XxlJobLogger.log("fqdn accessible task starting ");
        logger.info("fqdn accessible task starting ");
        for (List<Map> mapList : split) {
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    for (Map map : mapList) {
                        int code = 0;
                        if (uniqueFqdns.containsKey(map.get("fqdn"))) {
                            code = Integer.valueOf(String.valueOf(uniqueFqdns.get("fqdn")));
                        } else {
//                            code = HttpKit.getStatus(UrlUtil.getUrl(map.get("fqdn")));
                            code = HttpClientUtils.get(UrlUtil.getUrl(map.get("fqdn")));
                        }
                        uniqueFqdns.put(map.get("fqdn"), code);
                        if (ObjectUtil.isEmpty(map.get(STATUS_CODE))||Integer.valueOf(String.valueOf(map.get(STATUS_CODE))) != code) {
                            Map m = new ConcurrentHashMap();
                            m.put(STATUS_CODE, code);
                            m.put("id", map.get("id"));
                            transformResult.add(m);
                        }
                    }
                    countDownLatch.countDown();
                }
            });
        }
        try {
            logger.info("websketch-fqdn-accessible transform running...");
            XxlJobLogger.log("websketch-fqdn-accessibe transform running");
            countDownLatch.await();
            fixedThreadPool.shutdown();
        } catch (InterruptedException e) {
            logger.error(e);
            fixedThreadPool.shutdownNow();
        }
        //去掉重复无效
        logger.info("websketch-fqdn-accessible transform data {}, spend time {} ms", transformResult.size(), System.currentTimeMillis() - start);
        XxlJobLogger.log("websketch-fqdn-accessible transform data {}, spend time {} ms", transformResult.size(), System.currentTimeMillis() - start);
        return transformResult;

    }
    /**
     * @param sourceResult ioc_value
     *                     threat_type 字段相同则不入库
     * @return
     */
    public List<Map> webSketch(Object sourceResult) throws Exception {
        Map<String, List> source = JSONUtil.toBean(JSONUtil.toJsonStr(sourceResult), Map.class);
        List<Map> json = source.get("json");
        List<Map> malwareData = source.get("xlsx");
        Map<String, Map<String, List<String>>> malware = getMalwareByExcel(malwareData);
        Map<String, List<String>> soft = malware.get(SOFTWARE);
        Map<String, List<String>> associatedGroups = malware.get(ASSOCIATED_GROUPS);
        Map<String, List<String>> techniquesUsed = malware.get(TECHNIQUES_USED);

        //规则
        final List<String> iocTypeFilter = Arrays.asList("ip:port", "domain", "url");
        final String SOURCE = "abush.ch";// 文件来源
        final String IP = "ip";
        final String IP_PORT = "ip:port";// ioc_type改为port
        final String CAC = "command and control";// ioc_type改为port
        final String PE = "payload delivery";// ioc_type改为port
        final String FIELDS = "ioc_type,threat_type,malware_alias,confidence_level,reference";

        long now = System.currentTimeMillis();
        //处理   ioc=iov_value
        List<Map> transformResult = new ArrayList<>();
        for (Map map : json) {
            List<Map> data = (List<Map>) map.get("data");
            for (Map datum : data) {
                Object ioc_type = datum.get("ioc_type");
                if (iocTypeFilter.contains(ioc_type)) {
                    Map extMap = JobUtil.extractMap(datum, FIELDS);
                    String iocValue = String.valueOf(Optional.ofNullable(datum.get("ioc_value")).orElse(datum.get("ioc")));
                    extMap.put("ioc_value", iocValue);

                    Object first = Optional.ofNullable(datum.get("first_seen_utc")).orElse(datum.get("first_seen"));
                    Object last = Optional.ofNullable(datum.get("last_seen_utc")).orElse(datum.get("last_seen"));
                    extMap.put("first_report_time", JobUtil.nullToDef(first, ""));
                    extMap.put("last_seen_time", JobUtil.nullToDef(last, ""));

                    if (IP_PORT.equals(ioc_type)) {
                        extMap.put("ioc_type", IP);
                        extMap.put("ioc_value", iocValue.contains(":") ? iocValue.substring(0, iocValue.lastIndexOf(":")) : iocValue);
                    }
                    String threat_type = String.valueOf(datum.get("threat_type"));
                    if ("botnet_cc".equals(threat_type)) {
                        extMap.put("threat_type", CAC);
                    } else if ("payload".equals(threat_type) || "payload_delivery".equals(threat_type)) {
                        extMap.put("threat_type", PE);
                    }

                    //关联malware
                    String malwareName = String.valueOf(datum.get("malware_printable"));
                    if (soft.containsKey(malwareName)) {
                        extMap.put("mitre_attack_platforms", soft.get(malwareName).get(0));
                        extMap.put("mitre_attack_description", soft.get(malwareName).get(1));
                    } else {
                        extMap.put("mitre_attack_platforms", "");
                        extMap.put("mitre_attack_description", "");
                    }
                    if (associatedGroups.containsKey(malwareName)) {
                        extMap.put("mitre_attack_groups", JSONUtil.toJsonStr(associatedGroups.get(malwareName)));
                    } else {
                        extMap.put("mitre_attack_groups", "");
                    }
                    if (techniquesUsed.containsKey(malwareName)) {
                        extMap.put("mitre_attack_techniques", JSONUtil.toJsonStr(techniquesUsed.get(malwareName)));
                    } else {
                        extMap.put("mitre_attack_techniques", "");
                    }

                    extMap.put("source", SOURCE);
                    extMap.put("malware_name", malwareName);

                    extMap.put("create_time", now / 1000);
                    extMap.put("update_time", now / 1000);
                    transformResult.add(extMap);
                }
            }
        }
        logger.info("websketch transform data {}, spend time {} ms", transformResult.size(), System.currentTimeMillis() - now);
        XxlJobLogger.log("websketch transform data {}, spend time {} ms", transformResult.size(), System.currentTimeMillis() - now);
        return transformResult;
    }

    /**
     * {key, value}
     *
     * @param malwareData
     * @return
     */

    public Map<String, Map<String, List<String>>> getMalwareByExcel(List<Map> malwareData) throws Exception {

        Map<String, List<String>> newS = new HashMap();
        Map<String, List<String>> newAG = new HashMap();
        Map<String, List<String>> newTU = new HashMap();
        for (Map malwareDatum : malwareData) {
            List<Map<String, String>> software = (List<Map<String, String>>) malwareDatum.get(SOFTWARE);
            List<Map<String, String>> associatedGroups = (List<Map<String, String>>) malwareDatum.get(ASSOCIATED_GROUPS);
            List<Map<String, String>> techniquesUsed = (List<Map<String, String>>) malwareDatum.get(TECHNIQUES_USED);
            for (Map<String, String> map : software) {
                newS.put(map.get("name"), Arrays.asList(map.get("platforms"), map.get("description")));
            }//associated groups表格中target name字段，取(多行)source name和source id字段拼接
            for (Map<String, String> map : associatedGroups) {
                String sourceName = map.get(TARGET_NAME);
                List<String> value = Arrays.asList(StrUtil.join("", map.get(SOURCE_NAME), "(", map.get(SOURCE_ID), ")"));
                if (newAG.containsKey(sourceName)) {
                    newAG.put(sourceName, ListUtils.union(newAG.get(sourceName), value));
                } else {
                    newAG.put(sourceName, value);
                }
            }//techniques used表格中source name字段，取(多行)target name和target id字段拼接
            for (Map<String, String> map : techniquesUsed) {
                String targetName = map.get(SOURCE_NAME);
                List<String> value = Arrays.asList(StrUtil.join("", map.get(TARGET_NAME), "(", map.get(TARGET_ID), ")"));
                if (newTU.containsKey(targetName)) {
                    newTU.put(targetName, ListUtils.union(newTU.get(targetName), value));
                } else {
                    newTU.put(targetName, value);
                }
            }
        }
        Map<String, Map<String, List<String>>> malwareMap = new HashMap<>();
        malwareMap.put(SOFTWARE, newS);
        malwareMap.put(ASSOCIATED_GROUPS, newAG);
        malwareMap.put(TECHNIQUES_USED, newTU);
        return malwareMap;
    }


}
