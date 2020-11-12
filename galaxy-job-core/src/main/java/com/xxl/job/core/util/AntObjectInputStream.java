package com.xxl.job.core.util;

import com.xxl.job.core.biz.model.HandleCallbackParam;

import java.io.*;

/**
 * @Date: 2020-11-12 14:21
 * @Author : liuyongqiang
 * @ClassName : AntObjectInputStream
 * @Description : 校验反序列化
 */
public class AntObjectInputStream extends ObjectInputStream {

    public AntObjectInputStream(InputStream inputStream) throws IOException {
        super(inputStream);
    }

    /**
     * 只允许反序列化HandleCallbackParam class
     */
    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        if (!desc.getName().equals(HandleCallbackParam.class.getName())) {
            throw new InvalidClassException("Unauthorized deserialization attempt", desc.getName());
        }
        return super.resolveClass(desc);
    }
}
