package com.xxl.job.executor.pojo;

import lombok.Data;

@Data
public class DeleteDataParam {

	private String server;
	private String datasource;
	private String username;
	private String password;
	private String tablename;
	// maxDays：范围 [1-2000]
	private Integer maxdays;

}
