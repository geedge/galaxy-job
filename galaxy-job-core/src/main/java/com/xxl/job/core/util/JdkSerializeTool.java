package com.xxl.job.core.util;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author xuxueli 2020-04-12 0:14:00
 */
public class JdkSerializeTool {
    private static Logger logger = LoggerFactory.getLogger(JdkSerializeTool.class);


    // ------------------------ serialize and unserialize ------------------------

    /**
     * 将对象-->byte[] (由于jedis中不支持直接存储object所以转换成byte[]存入)
     *
     * @param object
     * @return
     */
    public static byte[] serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            // 序列化
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                oos.close();
                baos.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return null;
    }


    /**
     * 将byte[] -->Object
     *
     * @param bytes
     * @return
     */
    public static Object deserialize(byte[] bytes) {
        try {
            Object obj;
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            // Use ValidatingObjectInputStream instead of InputStream
            ValidatingObjectInputStream ois = new ValidatingObjectInputStream(bais);
            //HandleCallbackParam class
            ois.accept(HandleCallbackParam.class);
            obj = ois.readObject();
            return obj;
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
        }
        return null;
    }


}
