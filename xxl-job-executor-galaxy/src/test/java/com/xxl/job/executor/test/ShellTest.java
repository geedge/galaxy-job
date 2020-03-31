package com.xxl.job.executor.test;
import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.xxl.job.executor.XxlJobExecutorApplication;
import com.xxl.job.executor.core.config.StorgeConfig;
import com.zdjizhi.file.FileApplication;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { XxlJobExecutorApplication.class })
public class ShellTest {

	
	
	@Test
	public void shellTest() {
        
    	//脚本默认存放目录
//		String remoteDirectory = "/home/admin/test/";
		String scriptName = "shell.sh";
		try {
			String localFile = new File("").getCanonicalPath()+"/config/script/" + "storge.zip";
//			File file = new File(localFile);
//			File file2 = new File(new File("").getCanonicalPath()+"/config/"+scriptName);
//			Runtime runtime = Runtime.getRuntime();
//			runtime.exec("mv "+localFile +" "+remoteDirectory);
			boolean unZipFiles = FileApplication.unZipFiles(localFile, new File("").getCanonicalPath()+"/config/");
//			FileUtils.copyFileToDirectory(file, file2);
			String s = StorgeConfig.getStorgeConfigCommand("home");
			System.err.println(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
