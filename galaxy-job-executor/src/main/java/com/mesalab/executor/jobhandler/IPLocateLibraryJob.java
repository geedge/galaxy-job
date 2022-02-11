package com.mesalab.executor.jobhandler;

import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import cn.hutool.log.Log;
import com.mesalab.executor.core.config.IPLocateLibraryConfig;
import com.mesalab.executor.exception.BusinessException;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import ch.ethz.ssh2.Connection;
/**
 * @ Author tiangaojie
 * @ Date 2022/1/14
 * @ Description 动态ip定位库生成
 * */

@Component
public class IPLocateLibraryJob {
    private Log logger = Log.get();

    @Autowired
    private IPLocateLibraryConfig config;
    private Connection conn;
    @XxlJob("IPLocateLibraryJobHandler")
    public ReturnT<String> timingCacheJob(String param){
        String result = exec(param);
        conn.close();
        return ReturnT.SUCCESS;
    }

    public String exec(String cmds) {
        InputStream in;
        String result = "";
        try {
            if (this.login()) {
                Session session = conn.openSession(); // 打开一个会话
                session.execCommand(cmds);
                in = session.getStdout();
                result = this.processStdout(in, "utf-8");
//              读取返回的数据
                InputStream out = new StreamGobbler(session.getStdout());
                BufferedReader reader = new BufferedReader(new InputStreamReader(out));
                session.close();
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return result;
    }
    public String processStdout(InputStream in, String charset) {

        byte[] buf = new byte[1024];
        StringBuffer sb = new StringBuffer();
        try {
            while (in.read(buf) != -1) {
                sb.append(new String(buf, charset));
            }
        } catch (IOException e) {
            logger.info("Parameter transfer failure");
        }
        return sb.toString();
    }
    public boolean login() throws IOException {
        conn = new Connection(config.getServer());
        conn.connect(); // 连接
        boolean root = conn.authenticateWithPassword(config.getUsername(), config.getPassword());
        if(root==false){
            throw new BusinessException("Server connection failure");
        }
        return root;
    }

}
