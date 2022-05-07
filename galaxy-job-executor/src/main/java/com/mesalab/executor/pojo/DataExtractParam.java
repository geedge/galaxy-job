package com.mesalab.executor.pojo;

import lombok.Data;
import org.apache.poi.ss.formula.functions.T;

import java.util.List;

/**
 * @description:
 * @author: zhq
 * @create: 2022-04-29
 **/

@Data
public class DataExtractParam {


    private List<T> dataExtractParam;


    public static class source {

        private String type;
        private T t;

    }

    public static class transform {

        private String type;

    }

    public static class sink {

        private String type;

    }
}
