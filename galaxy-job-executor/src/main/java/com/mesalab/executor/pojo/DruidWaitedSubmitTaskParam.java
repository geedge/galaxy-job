package com.mesalab.executor.pojo;

import com.alibaba.fastjson2.JSONObject;

public class DruidWaitedSubmitTaskParam implements Comparable<DruidWaitedSubmitTaskParam> {
    final public int order;
    final public int maxNumConcurrentSubTasks;
    final public JSONObject param;

    public DruidWaitedSubmitTaskParam(int order, int maxNumConcurrentSubTasks, JSONObject param) {
        this.order = order;
        this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
        this.param = param;
    }

    @Override
    public int compareTo(DruidWaitedSubmitTaskParam o) {
        int compare = Integer.compare(order, o.order);
        if (compare != 0) {
            return compare;
        }
        return Integer.compare(maxNumConcurrentSubTasks, o.maxNumConcurrentSubTasks);
    }

    @Override
    public String toString() {
        return "DruidWaitedSubmitTaskParam(" + order + ", " + maxNumConcurrentSubTasks + ", " + param.toString() + ")";
    }
}
