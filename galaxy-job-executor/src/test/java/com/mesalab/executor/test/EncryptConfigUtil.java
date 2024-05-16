package com.mesalab.executor.test;

import cn.hutool.core.io.FileUtil;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.JobUtil;
import org.jasypt.util.text.BasicTextEncryptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EncryptConfigUtil {

    @Test
    public  void encryptTest() {

        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        //加密所需的salt
        textEncryptor.setPassword("galaxy");
        //要加密的数据（数据库的用户名或密码）
//        String user = textEncryptor.encrypt("admin");
//        String pin = textEncryptor.decrypt("galaxy2019");
//        System.err.println("user: "+user);
//        System.err.println("pin: "+pin);
        System.err.println("pin: "+textEncryptor.decrypt("z3gVlaa7Q4IMDqtUgvhwoJzKAkH1Uznc"));
        System.err.println("pin: "+textEncryptor.decrypt("LDEb2OekU7iZWiFw6pUYBSozVKP27r1y"));
    }

    @Resource
    StorgeConfig storgeConfig;

    @Test
    public  void encryptFileTest() throws Exception {
        JobUtil.encrypt("86cf0e2ffba3f541a6c6761313e5cc7e", FileUtil.file("C:\\test\\test1.csv"),FileUtil.file("C:\\test\\test.csv.enc"));
    }
    @Test
    public  void decryptFileTest() throws Exception {
        JobUtil.decrypt("86cf0e2ffba3f541a6c6761313e5cc7e", FileUtil.file("C:\\test\\9_latest"),FileUtil.file("C:\\test\\test3.csv"));
    }

//    public static byte[] readBytesFromFile(String filePath) throws Exception {
//        FileInputStream fis = new FileInputStream(filePath);
//
//        byte[] data = fis.readAllBytes();
//        fis.close();
//        return data;
//    }
//
//    public static void writeBytesToFile(byte[] data, String filePath) throws Exception {
//        FileOutputStream fos = new FileOutputStream(filePath);
//        fos.write(data);
//        fos.close();
//    }

}