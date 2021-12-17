package com.xxl.job.admin.util;

import org.jasypt.util.text.BasicTextEncryptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EncryptConfigUtil {

    @Test
    public  void encryptTest() {

        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        //加密所需的salt
        textEncryptor.setPassword("galaxy");
        //要加密的数据（数据库的用户名或密码）
        String user = textEncryptor.encrypt("root");
        String pin = textEncryptor.encrypt("111111");
        System.err.println("user: "+user);
        System.err.println("pin: "+pin);
    }
}