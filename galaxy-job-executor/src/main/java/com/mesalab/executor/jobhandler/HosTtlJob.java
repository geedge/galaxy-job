package com.mesalab.executor.jobhandler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.core.config.HbaseConfig;
import com.mesalab.executor.core.utils.HbaseUtil;
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

@Component
public class HosTtlJob {
    private Log logger = Log.get();

    @Autowired
    private Connection hbaseConnection;
    @Autowired
    private HbaseConfig hbaseConfig;

    private static final String max_days = "maxDays";
    private List<Delete> dataDeletes = null;
    private List<Delete> timeDeletes = null;
    private List<Delete> filenameDeletes = null;
    private ArrayList<Get> fileGets = null;
    private Set<String> expiredFiles = null;
    private Table table = null;
    private Table index_time_table = null;
    private Table index_filename_table = null;
    private String startRow = "";
    private int deleteCount = 0;

    /**
     * 删除过期数据
     *
     * @param params{"maxdays":30}
     */
    @XxlJob("deleteFilesJobHandler")
    public ReturnT<String> deleteExpiredFile(String params) {
        try {
            Map<String, Object> paramsMap = validParams(params);
            if (ObjectUtils.isEmpty(paramsMap)) {
                logger.error("params parser error , params is {}", params);
                return IJobHandler.FAIL;
            }
            Date date = DateUtil.date();
            long timestamp = HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(date).toString());
            XxlJobLogger.log("date: " + date);
            ArrayList<String> bucketList = getTableList();
            XxlJobLogger.log("bucket list: " + bucketList);
            if (bucketList.size() > 0) {
                for (String bucket : bucketList) {
                    deleteCount = 0;
                    if (System.currentTimeMillis() - timestamp >= hbaseConfig.getExecuteTime()) {
                        XxlJobLogger.log("job stop, the execution time exceeds " + hbaseConfig.getExecuteTime() + "ms.");
                        return ReturnT.SUCCESS;
                    }
                    deleteTableData(bucket, date, Integer.parseInt(String.valueOf(paramsMap.get(max_days))));
                    XxlJobLogger.log(bucket + " delete file count: " + deleteCount);
                    if (deleteCount > 0) {
                        compactTable(bucket);
                    }
                }
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

    private void deleteTableData(String tableName, Date date, int maxDays) {
        ResultScanner scanner = null;
        try {
            dataDeletes = new ArrayList<>();
            timeDeletes = new ArrayList<>();
            filenameDeletes = new ArrayList<>();
            fileGets = new ArrayList<>();
            expiredFiles = new HashSet<>();
            table = hbaseConnection.getTable(TableName.valueOf(tableName));
            index_time_table = hbaseConnection.getTable(TableName.valueOf(hbaseConfig.getTime_index_table_prefix() + tableName));
            index_filename_table = hbaseConnection.getTable(TableName.valueOf(hbaseConfig.getFilename_index_table_prefix() + tableName));
            for (String prefix : hbaseConfig.getFilename_head()) {
                if (System.currentTimeMillis() - HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(date).toString()) >= hbaseConfig.getExecuteTime()) {
                    XxlJobLogger.log("job stop, the execution time exceeds 1 day. ");
                    break;
                }
                startRow = prefix;
                while (true) {
                    if (System.currentTimeMillis() - HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(date).toString()) >= hbaseConfig.getExecuteTime()) {
                        XxlJobLogger.log("job stop, the execution time exceeds 1 day. ");
                        break;
                    }
                    boolean isEmpty = getExpiredFiles(tableName, prefix, date, maxDays);
                    if (isEmpty) {
                        break;
                    } else {
                        if (expiredFiles.size() >= hbaseConfig.getGet_batch()) {
                            deleteData(tableName, date, maxDays);
                        }
                    }
                }
            }
            if (expiredFiles.size() > 0) {
                deleteData(tableName, date, maxDays);
            }
        } catch (Exception be) {
            logger.error("delete table " + tableName + " data error. " + be.getMessage());
            XxlJobLogger.log("delete table " + tableName + " data error. " + be.getMessage());
        } finally {
            IoUtil.close(scanner);
            IoUtil.close(table);
            IoUtil.close(index_filename_table);
            IoUtil.close(index_time_table);
        }
    }

    private boolean getExpiredFiles(String tableName, String prefix, Date date, int maxDays) {
        boolean isEmpty = false;
        ResultScanner scanner = null;
        try {
            Scan scan = new Scan();
            scan.setCaching(hbaseConfig.getScanner_caching());
            scan.setRowPrefixFilter((prefix + "|").getBytes());
            scan.withStartRow(startRow.getBytes(), false);
            scan.withStopRow((prefix + "|" + HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString())).getBytes());
            scan.setLimit(hbaseConfig.getScan_batch());
            scanner = index_time_table.getScanner(scan);
            Result firstResult = scanner.next();
            if (firstResult == null) {
                isEmpty = true;
            } else {
                expiredFiles.add(Bytes.toString(firstResult.getRow()).split("\\|")[2]);
                startRow = Bytes.toString(firstResult.getRow());
                for (Result rs : scanner) {
                    if (!rs.isEmpty()) {
                        expiredFiles.add(Bytes.toString(rs.getRow()).split("\\|")[2]);
                        startRow = Bytes.toString(rs.getRow());
                    }
                }
            }
        } catch (Exception be) {
            logger.error("scan index_time_" + tableName + " data error, region is " + prefix + ". " + be.getMessage());
            XxlJobLogger.log("scan index_time_table error, region is " + prefix + ". " + be.getMessage());
        } finally {
            IoUtil.close(scanner);
        }
        return isEmpty;
    }

    private void deleteData(String tableName, Date date, int maxDays) {
        try {
            for (String file : expiredFiles) {
                Get get = new Get(file.getBytes());
                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("isparent"));
                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("filenamekey"));
                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("timekey"));
                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("partname"));
                get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("filesize"));
                fileGets.add(get);
            }
            Result[] results = table.get(fileGets);
            for (Result result : results) {
                if (!result.isEmpty() && result.containsColumn("meta".getBytes(), "isparent".getBytes())) {
                    int isparent = Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell("meta".getBytes(), "isparent".getBytes())));
                    String filenamekey = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("filenamekey"))));
                    String timekey = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("timekey"))));
                    if (isparent == 0 || isparent == 2) {
                        Cell filesize = result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("filesize"));
                        if (filesize.getTimestamp() < HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString())) {
                            dataDeletes.add(new Delete(result.getRow()));
                            filenameDeletes.add(new Delete(filenamekey.getBytes()));
                            timeDeletes.add(new Delete(timekey.getBytes()));
                        }
                    } else if (isparent == 1) {
                        Cell partnameCell = result.getColumnLatestCell(Bytes.toBytes("meta"), Bytes.toBytes("partname"));
                        String partname = Bytes.toString(CellUtil.cloneValue(partnameCell));
                        if (partnameCell.getTimestamp() < HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString())) {
                            dataDeletes.add(new Delete(result.getRow()));
                            String[] filenamekeys = filenamekey.split(",");
                            String filename = "";
                            for (String fk : filenamekeys) {
                                if (!"".equals(fk)) {
                                    filename = fk.split("\\|")[1];
                                    filenameDeletes.add(new Delete(fk.getBytes()));
                                }
                            }
                            String[] timekeys = timekey.split(",");
                            for (String tk : timekeys) {
                                if (!"".equals(tk)) {
                                    timeDeletes.add(new Delete(tk.getBytes()));
                                }
                            }
                            String[] partnames = partname.split(",");
                            for (String part : partnames) {
                                dataDeletes.add(new Delete(HbaseUtil.getRowKey(filename + "|" + part).getBytes()));
                            }
                        }
                    } else if (isparent == 3) {
                        int update = 0;
                        ArrayList<Delete> partDeletes = new ArrayList<>();
                        ArrayList<Get> partGets = new ArrayList<>();
                        for (String partHead : hbaseConfig.getPart_head()) {
                            Get getPart = new Get((partHead + "|" + Bytes.toString(result.getRow())).getBytes());
                            getPart.addFamily("meta".getBytes());
                            getPart.setTimeRange(HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(DateUtil.offsetDay(date, -maxDays)).toString()), HbaseUtil.dateToTimestamp(DateUtil.beginOfDay(date).toString()));
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
                            dataDeletes.addAll(partDeletes);
                            dataDeletes.add(new Delete(result.getRow()));
                            String[] filenamekeys = filenamekey.split(",");
                            for (String fk : filenamekeys) {
                                if (!"".equals(fk)) {
                                    filenameDeletes.add(new Delete(fk.getBytes()));
                                }
                            }
                            String[] timekeys = timekey.split(",");
                            for (String tk : timekeys) {
                                if (!"".equals(tk)) {
                                    timeDeletes.add(new Delete(tk.getBytes()));
                                }
                            }
                        }
                    }
                }
                if (dataDeletes.size() >= hbaseConfig.getDelete_batch()) {
                    table.delete(dataDeletes);
                    dataDeletes.clear();
                }
                if (timeDeletes.size() >= hbaseConfig.getDelete_batch()) {
                    index_time_table.delete(timeDeletes);
                    timeDeletes.clear();
                }
                if (filenameDeletes.size() >= hbaseConfig.getDelete_batch()) {
                    index_filename_table.delete(filenameDeletes);
                    filenameDeletes.clear();
                }
            }
            if (dataDeletes.size() > 0) {
                table.delete(dataDeletes);
                dataDeletes.clear();
            }
            if (timeDeletes.size() > 0) {
                index_time_table.delete(timeDeletes);
                timeDeletes.clear();
            }
            if (filenameDeletes.size() > 0) {
                index_filename_table.delete(filenameDeletes);
                filenameDeletes.clear();
            }
            deleteCount += expiredFiles.size();
            fileGets.clear();
            expiredFiles.clear();
        } catch (Exception be) {
            logger.error("delete " + tableName + " data error. " + be.getMessage());
            XxlJobLogger.log("delete " + tableName + " data error. " + be.getMessage());
        }
    }

    private void compactTable(String tableName) {
        Admin admin = null;
        try {
            admin = hbaseConnection.getAdmin();
            admin.majorCompact(TableName.valueOf(tableName), "meta".getBytes(), CompactType.NORMAL);
            admin.majorCompact(TableName.valueOf(tableName), "data".getBytes(), CompactType.MOB);
            admin.majorCompact(TableName.valueOf(hbaseConfig.getTime_index_table_prefix() + tableName), "meta".getBytes(), CompactType.NORMAL);
            admin.majorCompact(TableName.valueOf(hbaseConfig.getFilename_index_table_prefix() + tableName), "meta".getBytes(), CompactType.NORMAL);
        } catch (Exception be) {
            logger.error("compact " + tableName + " error. " + be.getMessage());
            XxlJobLogger.log("compact " + tableName + " error. " + be.getMessage());
        } finally {
            IoUtil.close(admin);
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
}
