package com.mesalab.executor.test;

import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.jobhandler.LogStorageQuotaJob;
import com.mesalab.executor.service.StorageQuotaService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { GalaxyExecutorApplication.class })
public class StorageQuotaTest {

//	  @Autowired StorageQuotaService storageQuotaService;
//	  
//	  @Test public void storageQuotaServiceTest() {
//	  
//	  Map dayJson = storageQuotaService.getDayJson(); Assert.notEmpty(dayJson,
//	  "clickhouse 一天的为空"); Map diskJson = storageQuotaService.getDiskJson();
//	  Assert.notEmpty(diskJson, "存储配额获取失败");
//	  
//	  }

	@Autowired
    LogStorageQuotaJob logStorageQuotaJob;
	
	@Autowired
    StorageQuotaService storageQuotaService;

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
	
}
