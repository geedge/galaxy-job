package com.mesalab.executor.service;

import java.util.Map;

/**
 * @description:
 * @author: zhq
 * @create: 2022-05-13
 **/

@FunctionalInterface
public interface DataExtractService<T> {

   public void adapt(Map transformResult, Map sinkParams) throws Exception;

}
