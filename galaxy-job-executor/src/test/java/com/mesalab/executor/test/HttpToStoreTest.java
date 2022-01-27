package com.mesalab.executor.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.net.url.UrlBuilder;
import cn.hutool.core.text.csv.*;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.log.Log;
import com.mesalab.executor.GalaxyExecutorApplication;
import com.mesalab.executor.core.config.StorgeConfig;
import com.mesalab.executor.core.utils.Constant;
import com.mesalab.executor.core.utils.HttpClientUtils;
import com.mesalab.executor.core.utils.JobUtil;
import com.mesalab.executor.pojo.HttpToKafkaParam;
import com.xxl.job.core.log.XxlJobLogger;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * @description:
 * @author: zhq
 * @create: 2022-01-21
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {GalaxyExecutorApplication.class})
public class HttpToStoreTest {
    private Log logger = Log.get();
//    private static StorgeConfig ckConfig =  (StorgeConfig) SpringContextUtil.getBean("storgeConfig");

    @Autowired
    StorgeConfig ckConfig;

    private byte[] compress(String str) {
        if (str == null || str.length() <= 0) {
            return null;
        }
        byte[] zip_bytes = null; // 声明压缩数据的字节数组
        // 先构建字节数组输出流，再据此构建压缩输出流
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gos = new GZIPOutputStream(baos);) {
            gos.write(str.getBytes()); // 往压缩输出流写入字节数组
            gos.finish(); // 结束写入操作
            zip_bytes = baos.toByteArray(); // 从字节数组输出流中获取字节数组信息
        } catch (Exception e) {
            e.printStackTrace();
        }
        return zip_bytes;
    }

    // 往文件写入压缩后的数据
    @Test
    public void writeZipFile() {
        String str = "白日依山尽，黄河入海流。\n欲穷千里目，更上一层楼。";
        // 根据指定文件路径构建文件输出流对象
        try (FileOutputStream fos = new FileOutputStream("D:\\tmpfile01.zip")) {
            // 从字符串获得压缩后的字节数组
            byte[] zip_bytes = compress(str);
            fos.write(zip_bytes); // 把字节数组写入文件输出流
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 通过地址读
     *
     * @param filePath
     */
    public static void readCSV(String filePath) {
        CsvReader reader = CsvUtil.getReader();
        //从文件中读取CSV数据
        CsvData data = reader.read(FileUtil.file(filePath));
        List<CsvRow> rows = data.getRows();
        //遍历行
        for (CsvRow csvRow : rows) {
            //getRawList返回一个List列表，列表的每一项为CSV中的一个单元格（既逗号分隔部分）
            System.out.println(csvRow.getRawList());
        }
    }


    /**
     * 导出excle
     *
     * @param filePath
     * @param lines
     */
    public void writeCSV(String filePath, Collection<String[]> lines) {
        //指定路径和编码
        CsvWriter writer = null;
        try {
            writer = CsvUtil.getWriter(filePath + ".csv", CharsetUtil.CHARSET_UTF_8);
            //按行写出
            writer.write(lines);
        } catch (IORuntimeException e) {
            e.printStackTrace();
        } finally {
            assert writer != null;
            writer.close();
        }
    }

    private void writeRow(List<Object> row, BufferedWriter csvWriter) throws IOException {
        // 写入文件头部
        for (Object data : row) {
            String rowStr = "\"" + data + "\",";
            csvWriter.write(rowStr);

        }
        csvWriter.newLine();
    }

    @Test
    public void writeTest() {
//        String[] fields = {"name", "type"};
//        String str = "[{\"common_recv_time\":1,\"type\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"},{\"common_recv_time\":\"2\",\"type\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"}]";

//        List<Map> results = JSONUtil.toList(str, Map.class);
        List<Map> results = (List<Map>) queryTest();
        File csvFile = JobUtil.createCsvFile(results, "web-sketch-db-weekly-assessment-all.csv");
        File csvFile1 = JobUtil.createCsvFile(results, "web-sketch-db-weekly-assessment-uncategorized.csv");
        String randomString = RandomUtil.randomString(6);
        String currentDate = DateUtils.getCurrentDate("yyyyMMdd");
        String zipName = "web-sketch-db-weekly-assement-" + currentDate + "-" + randomString + ".zip";
        File zipFile = ZipUtil.zip(FileUtil.file("D:\\logs\\" + zipName), false, csvFile, csvFile1);

        CloseableHttpResponse response = null;
        //创建HttpClient
        String url = "http://192.168.44.12:9098/hos/assessment_hos_bucket/" + zipName;
        Header header = new BasicHeader(Constant.TOKEN, "c21f969b5f03d33d43e04f8f136e7682");
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(zipFile));
            String res = HttpClientUtils.httpPutStream(url, in, header);
            logger.info("send file to hos result :" + res);
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>> " + res);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            IoUtil.close(response);
        }
        //元数据存入库
        String querySql = "http://192.168.44.12:9999/?query=SELECT http_domain AS name,arrayStringConcat(groupUniqArray(common_l7_protocol),',') AS protocol, count(*) AS sessions FROM tsg_galaxy_v3.session_record WHERE common_recv_time >= toStartOfTenMinutes(now()- INTERVAL 24 HOUR) AND common_recv_time < now() AND http_domain != '' GROUP BY name ORDER BY sessions DESC LIMIT 100;";


        long length = zipFile.length();
        String sha256Hex = DigestUtil.sha256Hex(zipFile);

        String insertSql = "INSERT INTO tsg_galaxy_v3.assessment_event " +
                "(common_log_id, common_recv_time, assessment_date, lot_number, flie_name, assessment_file, assessment_type, features, `size`, file_checksum_sha)" +
                "VALUES(0, 0, 0, '', '', '', '', '', " + length + ", '" + sha256Hex + "');";
        Map<String, Object> ckParamMap = ckConfig.getDeleteSource();

        ckParamMap.put("query", insertSql);

        UrlBuilder urlBuilder = UrlBuilder.ofHttp(ckConfig.getTrafficServer(), StandardCharsets.UTF_8);
        logger.info(" url info " + urlBuilder.toString());
        HttpClientUtils.httpPost(HttpClientUtils.getUrlWithParams(urlBuilder.toString(), ckParamMap), "");
        XxlJobLogger.log("save meta to clickhouse");

            /*RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000).build();
            HttpPut httpPut = new HttpPut(url);
            httpPut.setConfig(requestConfig);
            httpPut.setHeader(header);
            httpPut.setEntity(new InputStreamEntity(in));
            response = httpClient.execute(httpPut);

            if (response.getStatusLine().getStatusCode() == 200) {
                String responseEntity = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONObject jsonObj = (JSONObject) JSON.parse(responseEntity);
                if (jsonObj != null) {
                    if (responseEntity.contains("\"code\":200")) {
//                        PutFileResponse postFileResponse = JSON.toJavaObject(jsonObj, PostFileResponse.class);
                        logger.info("post file success. current url: {}, msg: {}", url, responseEntity);
                    } else {
                        logger.error("post file error. current url: {}, msg: {}", url, responseEntity);
                    }
                } else {
                    logger.error("post file error, response body error. current url: {}", url);
                }
            } else if (response.getStatusLine().getStatusCode() == 500) {
                logger.error("post file error. Oss service error.current url: {}, code: 500, msg: {}", url, response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity(), "UTF-8"));
            } else {
                logger.error("post file error. current url: {}, code: {}, msg: {}", url, response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity(), "UTF-8"));
            }*/

    }

    public byte[] fileToBinArray(File file) {
        try {
            InputStream fis = new FileInputStream(file);
            byte[] bytes = FileCopyUtils.copyToByteArray(fis);
            return bytes;
        } catch (Exception ex) {
            throw new RuntimeException("transform file into bin Array 出错", ex);
        }
    }

    public Object queryTest() {
        String params = "{" +
                "    \"url\": \"http://192.168.44.252:9999/?query=SELECT http_domain AS name,arrayStringConcat(groupUniqArray(common_l7_protocol),',') AS protocol, count(*) AS sessions FROM tsg_galaxy_v3.session_record WHERE  common_recv_time < now()  GROUP BY name ORDER BY sessions DESC LIMIT 500000;\"," +
//                "    \"url\": \"http://192.168.44.12:9999/?query=SELECT http_domain AS name,arrayStringConcat(groupUniqArray(common_l7_protocol),',') AS protocol, count(*) AS sessions FROM tsg_galaxy_v3.session_record WHERE common_recv_time >= toStartOfTenMinutes(now()- INTERVAL 24 HOUR) AND common_recv_time < now() AND http_domain != '' GROUP BY name ORDER BY sessions DESC LIMIT 500000;\"," +
                "    \"method\": \"get\"," +
                "    \"requestBody\": {}," +
                "    \"resultKey\": \"data\"," +
                "    \"store\": \"hos.web-sketch\"" +
                "}";
        String resultMessage = null;
        HttpToKafkaParam httpParams = (HttpToKafkaParam) JsonMapper.fromJsonString(params, HttpToKafkaParam.class);
        if (HttpMethod.POST.name().equalsIgnoreCase(httpParams.getMethod())) {
            resultMessage = HttpClientUtils.httpPost(httpParams.getUrl(), JsonMapper.toJsonString(httpParams.getRequestBody()));
        } else if (HttpMethod.GET.name().equalsIgnoreCase(httpParams.getMethod())) {
            resultMessage = HttpClientUtils.httpGet(httpParams.getUrl());
        } /*else {
            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Not support Method:" + httpParams.getMethod());
        }*/

//        if (resultMessage.equals("-1")) {
//            return new ReturnT<String>(IJobHandler.FAIL.getCode(), "Http Client connection Error!");
//
//        }

        Object resultObj = JsonMapper.fromJsonString(resultMessage, Object.class);

        // 获取字段
        /*List<Object> fields = new ArrayList<>();
        if(((Map) resultObj).get("meta")!=null){
            Object meta = ((Map) resultObj).get("meta");
            ((List<Map>)meta).forEach(x->fields.add(x.get("name")));
        }*/

        if (StringUtil.isNotBlank(httpParams.getResultKey())) {
            resultObj = ((Map) resultObj).get(httpParams.getResultKey());
        }
//        System.err.println(resultObj);

        return resultObj;
    }


    public File createCsvFile(List<Map> maps, String[] fields, String filePath) {
        File file = FileUtil.newFile(filePath);
        try (CsvWriter writer = CsvUtil.getWriter(file, CharsetUtil.CHARSET_UTF_8)) {
            List result = new ArrayList<String[]>();
            result.add(fields);
            maps.forEach(x -> result.add(x));
            writer.write(result);
            return file;
        } catch (IORuntimeException e) {
            logger.error(e.getMessage());
            XxlJobLogger.log(e.getMessage());
            return null;
        }
    }


    private static final String TAG = "uploadFile";
    private static final int TIME_OUT = 100 * 1000; // 超时时间
    private static final String CHARSET = "utf-8"; // 设置编码

    public String uploadFile(File file, String RequestURL) {
        String result = null;
        String BOUNDARY = "heima"; // 边界标识 随机生成
        String PREFIX = "--", LINE_END = "\r\n";
        String CONTENT_TYPE = "multipart/form-data"; // 内容类型

        try {
            URL url = new URL(RequestURL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setReadTimeout(TIME_OUT);
            conn.setConnectTimeout(TIME_OUT);
            conn.setDoInput(true); // 允许输入流
            conn.setDoOutput(true); // 允许输出流
            conn.setUseCaches(false); // 不允许使用缓存
            conn.setRequestMethod("POST"); // 请求方式
            conn.setRequestProperty("Charset", CHARSET); // 设置编码
            conn.setRequestProperty(Constant.TOKEN, "c21f969b5f03d33d43e04f8f136e7682");
            conn.setRequestProperty("connection", "keep-alive");
            conn.setRequestProperty("Content-Type", CONTENT_TYPE + ";boundary=" + BOUNDARY);
            if (file != null) {
                /**
                 * 当文件不为空，把文件包装并且上传
                 */
                DataOutputStream dos = new DataOutputStream(conn.getOutputStream());
                StringBuffer sb = new StringBuffer();
                sb.append(PREFIX);
                sb.append(BOUNDARY);
                sb.append(LINE_END);
                /**
                 * 这里重点注意： name里面的值为服务器端需要key 只有这个key 才可以得到对应的文件
                 * filename是文件的名字，包含后缀名的 比如:abc.png
                 */
                sb.append("Content-Disposition: form-data; name=\"file\"; filename=\""
                        + file.getName() + "\"" + LINE_END);
                sb.append("Content-Type: application/zip" + LINE_END);
                sb.append(LINE_END);
                dos.write(sb.toString().getBytes());
                InputStream is = new FileInputStream(file);
                byte[] bytes = new byte[1024 * 1024];
                int len = 0;
                while ((len = is.read(bytes)) != -1) {
                    dos.write(bytes, 0, len);
                }
                is.close();
                dos.write(LINE_END.getBytes());
                byte[] end_data = (PREFIX + BOUNDARY + PREFIX + LINE_END).getBytes();
                dos.write(end_data);
                dos.flush();
                /**
                 * 获取响应码 200=成功 当响应成功，获取响应的流
                 */
                int res = conn.getResponseCode();
                logger.info(TAG, "response code:" + res);
                // if(res==200)
                // {
                logger.info(TAG, "request success");
                InputStream input = conn.getInputStream();
                StringBuffer sb1 = new StringBuffer();
                int ss;
                while ((ss = input.read()) != -1) {
                    sb1.append((char) ss);
                }
                result = sb1.toString();
                result = new String(result.getBytes("iso8859-1"), "utf-8");
                logger.info(TAG, "result : " + result);
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return result;
    }

    @Test
    public void downFile() {
        String buildUrl = UrlBuilder.create()
                .setScheme("http") //http方式
                .setHost("192.168.44.12") //ip
                .setPort(9098)
                .addPath("/hos/assessment_hos_bucket/web-sketch-db-weekly-assement-20220127-gl6zlj.zip")
                .setCharset(Charset.forName(CharsetUtil.UTF_8))
                .build();


        long file = HttpUtil.downloadFile(buildUrl, "D:\\logs\\1.zip");
    }

}
