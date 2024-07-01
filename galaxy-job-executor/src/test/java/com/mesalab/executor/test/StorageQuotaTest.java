package com.mesalab.executor.test;

import com.geedgenetworks.utils.DateUtils;
import com.geedgenetworks.utils.JsonMapper;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.utils.Constant;
import com.mesalab.executor.core.utils.KafkaUtils;
import com.mesalab.executor.jobhandler.LogStorageQuotaJob;
import com.mesalab.executor.pojo.JDBCParam;
import com.mesalab.executor.service.StorageQuotaService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {GalaxyExecutorApplication.class})
@Slf4j
public class StorageQuotaTest {

    @Autowired
    LogStorageQuotaJob logStorageQuotaJob;

    /**
     * 文件清库 慎
     */
    @Test
    public void delteAllFilesTest() {

//		ReturnT<String> deleteAllFiles = logStorageQuotaJob.deleteAllFiles("{\"maxdays\":365}");
//		Assert.isTrue(200==(deleteAllFiles.getCode()),"success");
    }


    @Test
    public void testS() {
        String filesHosServer = "Nur-sultan|192.168.40.223:9098,Aktau|192.168.1.2:9098,Aktubinsk|,Almaty|,Atyrau|,Karaganda|,Kokshetau|,Kostanay|,Kyzylorda|,Pavlodar|,Semey|,Shymkent|,Taldykurgan|,Taraz|,Uralsk|,Ust-Kamenogorsk|,Zhezkazgan|";

//		Splitter.on(",").splitToList(a).stream().map(x ->  x.substring(x.indexOf("|")));
        String b = "Nur-sultan|192.168.40.223:9098";
//		String collect = Splitter.on(",").splitToList(a).stream().map(x -> x.substring(x.indexOf("|") + 1)).filter(x-> StringUtil.isNotEmpty(x)).collect(Collectors.joining(","));
        List collect = Splitter.on(",").splitToList(filesHosServer).stream().map(x -> {
            List<String> dc = Splitter.on("|").splitToList(x);
            if (dc.size() == 2 && StringUtil.isNotBlank(dc.get(1))) {
                return ImmutableMap.of(dc.get(0), dc.get(1));
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        log.info(JsonMapper.toJsonString(collect));
    }

    @Test
    public void test() {
        String date = DateUtils.getDateOfYesterday("yyyyMMdd");
        long yyyyMMdd = DateUtils.convertStringToTimestamp(date, "yyyyMMdd");
        log.info(String.valueOf(yyyyMMdd));
    }

    @Test
    public void testStorage() {
        String msgc = "{\"log_type\":\"Files\",\"aggregate_size\":0,\"data_center\":\"Nur-sultan\",\"time\":1630569900,\"last_storage\":1622299518,\"used_size\":1217383982783,\"max_size\":25169958203392}";
        new KafkaUtils().sendMessage("SYS-STORAGE-LOG", msgc);
    }


    @Resource
    StorageQuotaService storageQuotaService;

    @Test
    public void testStorage2() {
        Map<String, String> sourceParams = new HashMap<>();
        sourceParams.put("ip", "192.168.44.12");
        sourceParams.put("type", "mariadb");
        sourceParams.put("database", "tsg_olap");
        sourceParams.put("table", "sys_storage_event");
        sourceParams.put("username", "root");
        sourceParams.put("pin", "galaxy2019");
        JDBCParam jdbcParam = new JDBCParam(sourceParams);
        storageQuotaService.getAndSaveStorageQuotaInfo(new String[]{Constant.TRAFFIC_LOGS}, jdbcParam, true);
        storageQuotaService.getAndSaveStorageQuotaInfo(new String[]{Constant.METRICS}, jdbcParam, true);
        storageQuotaService.getAndSaveStorageQuotaInfo(new String[]{Constant.FILES}, jdbcParam, true);

    }
}
