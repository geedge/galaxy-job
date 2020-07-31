package com.xxl.job.executor.test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.core.utils.HttpClientUtils;
import com.xxl.job.executor.core.utils.XmlUtil;
import com.xxl.job.executor.jobhandler.LogStorageQuotaJob;
import com.xxl.job.executor.pojo.ListAllMyBucketsResult;
import com.xxl.job.executor.pojo.ListAllMyBucketsResult.Buckets.Bucket;
import com.xxl.job.executor.service.StorageQuotaService;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.TimeConstants;
import com.zdjizhi.utils.ZooKeeperLock;
import com.zdjizhi.utils.ZookeeperUtils;

import ch.qos.logback.core.joran.spi.XMLUtil;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { XxlJobExecutorApplication.class })
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
