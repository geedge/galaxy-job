package com.mesalab.executor.jobhandler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.core.config.HbaseConfig;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;

@Component
public class HosTtlJob {
    private Log logger = Log.get();

    @Autowired
    private Connection hbaseConnection;
    @Autowired
    private ExecutorService threadPool;
    @Autowired
    private HbaseConfig hbaseConfig;

    private static final String max_days = "maxDays";
    private int deleteCount;
    private int deleteErrorCount;

    /**
     * 删除过期数据
     *
     * @param params{"maxdays":30}
     */
    @XxlJob("deleteHosFilesJobHandler")
    public ReturnT<String> deleteExpiredFile(String params) {
        try {
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Date date = DateUtil.date();
            XxlJobLogger.log("date: " + date);
            ArrayList<String> bucketList = getTableList();
            XxlJobLogger.log("bucket list: " + bucketList);
            deleteCount = 0;
            deleteErrorCount = 0;
            if (bucketList.size() > 0) {
                for (String bucket : bucketList) {
                    deleteBucketFile(bucket, date, Integer.parseInt(String.valueOf(paramsMap.get(max_days))));
                }
                XxlJobLogger.log("delete file count: " + deleteCount + "; delete error count : " + deleteErrorCount);
            }
        } catch (RuntimeException be) {
            logger.error(be.getMessage());
            XxlJobLogger.log(be.getMessage());
            return ReturnT.FAIL;
        }
        return ReturnT.SUCCESS;
    }

    private ArrayList<String> getTableList() {
        ArrayList<String> tableList = new ArrayList<>();
        Table table = null;
        try {
            table = hbaseConnection.getTable(TableName.valueOf(hbaseConfig.getSystem_bucket_meta()));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result rs : scanner) {
                tableList.add(Bytes.toString(rs.getRow()));
            }
        } catch (Exception be) {
            logger.error("get bucket list error. " + be.getMessage());
            XxlJobLogger.log("get bucket list error. " + be.getMessage());
        } finally {
            IoUtil.close(table);
        }
        return tableList;
    }

    private void deleteBucketFile(String tableName, Date date, int maxDays) {
        Table table = null;
        Table index_time_table = null;
        try {
            table = hbaseConnection.getTable(TableName.valueOf(tableName));
            index_time_table = hbaseConnection.getTable(TableName.valueOf(hbaseConfig.getTime_index_table_prefix() + tableName));
            for (String prefix : hbaseConfig.getFilename_head()) {
                String startRow = prefix;
                while (true) {
                    ResultScanner scanner = null;
                    Set<Get> fileGets = new HashSet<>();
                    try {
                        Scan scan = new Scan();
                        scan.setRowPrefixFilter((prefix + "|").getBytes());
                        scan.withStartRow(startRow.getBytes(), false);
                        scan.withStopRow((prefix + "|" + dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString())).getBytes());
                        scan.setCaching(hbaseConfig.getScanner_caching());
                        scan.setLimit(hbaseConfig.getScan_batch());
                        scanner = index_time_table.getScanner(scan);
                        ArrayList<Row> rows = new ArrayList<>();
                        for (Result rs : scanner) {
                            if (!rs.isEmpty()) {
                                startRow = Bytes.toString(rs.getRow());
                                Get get = new Get(Bytes.toString(rs.getRow()).split("\\|")[2].getBytes());
                                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("isparent"));
                                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("filenamekey"));
                                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("timekey"));
                                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("partname"));
                                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("filesize"));
                                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("filename"));
                                Increment increment = new Increment(rs.getRow());
                                increment.addColumn("meta".getBytes(), "scancount".getBytes(), 1);
                                rows.add(increment);
                                fileGets.add(get);
                            }
                        }
                        index_time_table.batch(rows, null);
                    } catch (Exception be) {
                        logger.error("scan index_time_" + tableName + " data error, start row is " + startRow + ". " + be.getMessage());
                        XxlJobLogger.log("scan index_time_table error, start row is " + startRow + ". " + be.getMessage());
                    } finally {
                        IoUtil.close(scanner);
                    }

                    if (fileGets.size() <= 0) {
                        break;
                    } else {
                        Result[] results = table.get(new ArrayList<>(fileGets));
                        for (Result result : results) {
                            threadPool.submit(new Runnable() {
                                @Override
                                public void run() {
                                    deleteData(tableName, date, maxDays, result);
                                }
                            });
                        }
                    }
                }
            }
        } catch (Exception be) {
            logger.error("delete table " + tableName + " data error. " + be.getMessage());
            XxlJobLogger.log("delete table " + tableName + " data error. " + be.getMessage());
        } finally {
            IoUtil.close(table);
            IoUtil.close(index_time_table);
        }
    }

    private void deleteData(String tableName, Date date, int maxDays, Result result) {
        String fileName = "";
        Table table = null;
        Table index_time_table = null;
        Table index_filename_table = null;
        try {
            table = hbaseConnection.getTable(TableName.valueOf(tableName));
            index_time_table = hbaseConnection.getTable(TableName.valueOf(hbaseConfig.getTime_index_table_prefix() + tableName));
            index_filename_table = hbaseConnection.getTable(TableName.valueOf(hbaseConfig.getFilename_index_table_prefix() + tableName));
            if (!result.isEmpty() && result.containsColumn("meta".getBytes(), "isparent".getBytes())) {
                int isparent = Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell("meta".getBytes(), "isparent".getBytes())));
                String filenamekey = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("filenamekey"))));
                String timekey = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("timekey"))));
                fileName = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("filename"))));
                if (isparent == 0 || isparent == 2) {
                    Cell filesize = result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("filesize"));
                    if (filesize.getTimestamp() < dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString())) {
                        index_filename_table.delete(new Delete(filenamekey.getBytes()));
                        table.delete(new Delete(result.getRow()));
                        index_time_table.delete(new Delete(timekey.getBytes()));
                    }
                } else if (isparent == 1) {
                    Cell partNameCell = result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("partname"));
                    if (partNameCell.getTimestamp() < dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString())) {
                        String[] filenamekeys = filenamekey.split(",");
                        for (String fk : filenamekeys) {
                            if (!"".equals(fk)) {
                                index_filename_table.delete(new Delete(fk.getBytes()));
                            }
                        }
                        String[] partNames = Bytes.toString(CellUtil.cloneValue(partNameCell)).split(",");
                        ArrayList<Delete> partDeletes = new ArrayList<>();
                        for (String part : partNames) {
                            partDeletes.add(new Delete(getRowKey(fileName + "|" + part).getBytes()));
                        }
                        partDeletes.add(new Delete(result.getRow()));
                        table.delete(partDeletes);
                        String[] timekeys = timekey.split(",");
                        for (String tk : timekeys) {
                            if (!"".equals(tk)) {
                                index_time_table.delete(new Delete(tk.getBytes()));
                            }
                        }
                    }
                } else if (isparent == 3) {
                    int update = 0;
                    ArrayList<Delete> partDeletes = new ArrayList<>();
                    ArrayList<Get> partGets = new ArrayList<>();
                    for (String partHead : hbaseConfig.getPart_head()) {
                        Get getPart = new Get((partHead + "|" + Bytes.toString(result.getRow())).getBytes());
                        getPart.addFamily("meta".getBytes());
                        getPart.setTimeRange(dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString()), dateToTimestamp(DateUtil.beginOfDay(date).toString()));
                        getPart.setMaxResultsPerColumnFamily(1);
                        partGets.add(getPart);
                        partDeletes.add(new Delete((partHead + "|" + Bytes.toString(result.getRow())).getBytes()));
                    }
                    for (Result part : table.get(partGets)) {
                        if (!part.isEmpty()) {
                            update += 1;
                            break;
                        }
                    }
                    if (update == 0) {
                        String[] filenamekeys = filenamekey.split(",");
                        for (String fk : filenamekeys) {
                            if (!"".equals(fk)) {
                                index_filename_table.delete(new Delete(fk.getBytes()));
                            }
                        }
                        partDeletes.add(new Delete(result.getRow()));
                        table.delete(partDeletes);
                        String[] timekeys = timekey.split(",");
                        for (String tk : timekeys) {
                            if (!"".equals(tk)) {
                                index_time_table.delete(new Delete(tk.getBytes()));
                            }
                        }
                    }
                }
                synchronized (this) {
                    deleteCount++;
                }
            }
        } catch (Exception be) {
            logger.error("delete " + tableName + " " + fileName + " error. " + be.getMessage());
            XxlJobLogger.log("delete " + tableName + " " + fileName + " error. " + be.getMessage());
            synchronized (this) {
                deleteErrorCount++;
            }
        } finally {
            IoUtil.close(table);
            IoUtil.close(index_filename_table);
            IoUtil.close(index_time_table);
        }
    }

    private Map<String, Object> validParams(String params) {
        logger.info("params{}", params);
        if (StringUtil.isBlank(params)) {
            XxlJobLogger.log("params is Empty !");
            return null;
        }
        Map<String, Object> paramMap = (Map) JsonMapper.fromJsonString(params, Map.class);
        if (paramMap == null) {
            XxlJobLogger.log("params error !");
            return null;
        }
        return paramMap;
    }

    private long dateToTimestamp(String date) {
        return DateUtil.parse(date).getTime();
    }

    private String getRowKey(String filename) {
        String md5str = DigestUtil.md5Hex(filename);
        md5str = md5str.substring(8, 24);
        return md5str;
    }
}
