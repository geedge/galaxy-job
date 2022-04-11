package com.mesalab.executor.service;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.mesalab.executor.core.utils.JobUtil;
import org.apache.commons.collections4.ListUtils;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @description:
 * @author: zhq
 * @create: 2022-03-30
 **/
@Service
public class DataTransformService {

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
            default:
                break;
        }
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
        Map<String, Map<String, List<String>>> malware = getMalware(malwareData);
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

        long now = System.currentTimeMillis() / 1000;
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

                    extMap.put("create_time", now);
                    extMap.put("update_time", now);

                    transformResult.add(extMap);
                }
            }
        }
        return transformResult;
    }

    /**
     * {key, value}
     *
     * @param malwareData
     * @return
     */

    public Map<String, Map<String, List<String>>> getMalware(List<Map> malwareData) throws Exception {

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
