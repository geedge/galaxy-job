package com.mesalab.executor.core.config;

import com.google.common.base.Splitter;
import com.zdjizhi.utils.StringUtil;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Configuration
public class StorgeConfig {

	@Value("${storge.traffic.server}")
	private String trafficServer;// clickhouse要查看的ip

	@Value("${storge.traffic.username}")
	private String trafficUsername;// clickhouse要查看的username
	
	@Value("${storge.traffic.password}")
	private String trafficPassword;// clickhouse要查看的password
	
	@Value("${storge.traffic.datasource}")
	private String trafficDatasource;// clickhouse要查看的datasource
	
	@Value("${storge.traffic.system.parts}")
	private String systemParts ;

	@Value("${storge.traffic.system.partsclusters}")
	private String systemPartsCluster ;

	@Value("${storge.traffic.system.disks}")
	private String systemDisks ;
	
	@Value("${storge.traffic.system.clusters}")
	private String systemClusters ;

	@Value("${storge.traffic.system.tables}")
	private String systemTables ;

	@Value("${storge.analytic.server}")
	private String analyticServer;// druid 节点ip和port

	@Value("${storge.files.hos-server}")
	private String filesHosServer;// 清库删除文件服务器 ip:port

	@Value("${storge.files.token}")
	private String filesToken; //登录验证

	public String getFilesHosServer() {
		return getServer();
	}

	private String getServer() {
		return getServer(filesHosServer);
	}

	public String getTrafficServer() {
		return getServer(trafficServer);
	}
	public String getAnalyticServer() {
		return getServer(analyticServer);
	}

	public Map<String,String> getFilesDataCenter() {
		return getDCMap(filesHosServer);
	}

	public Map<String,String> getTrafficDataCenter() {
		return getDCMap(trafficServer);
	}
	public Map<String,String> getAnalyticDataCenter() {
		return getDCMap(analyticServer);
	}
	private String getServer(String dcStr) {
		return Splitter.on(",").trimResults().splitToList(dcStr).stream().map(x -> x.substring(x.indexOf("|") + 1)).filter(x -> StringUtil.isNotEmpty(x)).collect(Collectors.joining(","));
	}

	private Map<String, String> getDCMap(String dcStr) {
		Map<String, String> map = new HashMap();
		Splitter.on(",").trimResults().splitToList(dcStr).stream().forEach(x -> {
			List<String> dc = Splitter.on("|").trimResults().splitToList(x);
			if (dc.size() == 2 && StringUtil.isNotBlank(dc.get(1))) {
				map.put(dc.get(0), dc.get(1));
			}
		});
		return map;
	}
}
