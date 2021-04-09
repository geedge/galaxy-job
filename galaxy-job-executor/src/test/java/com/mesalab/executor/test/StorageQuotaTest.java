package com.mesalab.executor.test;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.jobhandler.LogStorageQuotaJob;
import com.mesalab.executor.service.StorageQuotaService;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { GalaxyExecutorApplication.class })
@Slf4j
public class StorageQuotaTest {

	  @Autowired StorageQuotaService storageQuotaService;

	  @Test public void storageQuotaServiceTest() {

	  Map diskJson = storageQuotaService.getDiskJson(false);
	  Assert.notEmpty(diskJson, "存储配额获取失败");

	  }

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

	/**
	 * 设置文件存储策略
	 */
//	@Test
//	public void deleteFilesTest() {
//		ReturnT<String> deleteFiles = logStorageQuotaJob.deleteFiles("{\"maxdays\":365}");
//		Assert.isTrue(200 == (deleteFiles.getCode()), "success");
//	}

	@Test
	public void zookeeperTest() {
		Long lastStorage = storageQuotaService.getLastStorage("Files");
		Assert.notNull(lastStorage, "获取标准时间失败");
	}
	@Test
	public void testS() {
		String filesHosServer ="Nur-sultan|192.168.40.223:9098,Aktau|192.168.1.2:9098,Aktubinsk|,Almaty|,Atyrau|,Karaganda|,Kokshetau|,Kostanay|,Kyzylorda|,Pavlodar|,Semey|,Shymkent|,Taldykurgan|,Taraz|,Uralsk|,Ust-Kamenogorsk|,Zhezkazgan|";

//		Splitter.on(",").splitToList(a).stream().map(x ->  x.substring(x.indexOf("|")));
		String b ="Nur-sultan|192.168.40.223:9098";
//		String collect = Splitter.on(",").splitToList(a).stream().map(x -> x.substring(x.indexOf("|") + 1)).filter(x-> StringUtil.isNotEmpty(x)).collect(Collectors.joining(","));
		List collect = Splitter.on(",").splitToList(filesHosServer).stream().map(x->{
			List<String> dc = Splitter.on("|").splitToList(x);
			if (dc.size()==2&&StringUtil.isNotBlank(dc.get(1))){
				return ImmutableMap.of(dc.get(0),dc.get(1));
			}
			return null;
		}).filter(Objects::nonNull).collect(Collectors.toList());
		log.info(JsonMapper.toJsonString(collect));
	}

	@Test
	public void test(){
		String date = DateUtils.getDateOfYesterday("yyyyMMdd");
		long yyyyMMdd = DateUtils.convertStringToTimestamp(date, "yyyyMMdd");
		log.info(String.valueOf(yyyyMMdd));
	}
}
