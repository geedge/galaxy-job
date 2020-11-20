package com.mesalab.executor.core.utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Splitter;

public class ZookeeperUtils implements Watcher {
	private static Logger logger = Logger.getLogger(ZookeeperUtils.class);

	private ZooKeeper zookeeper;

	private static final int SESSION_TIME_OUT = 10000;//ms

	private CountDownLatch countDownLatch = new CountDownLatch(1);

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected) {
			countDownLatch.countDown();
		}
	}

	/**
	 * 修改节点信息
	 *
	 * @param path 节点路径
	 */
	public void modifyNode(String path, String data, String zookeeperIp) {
		createNode(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, zookeeperIp);
		try {
			connectZookeeper(zookeeperIp);
			Stat stat = zookeeper.exists(path, true);
			if (stat != null) {
				zookeeper.setData(path, data.getBytes(), stat.getVersion());
			} else {
				logger.error("Node does not exist!,Can't modify");
			}
		} catch (KeeperException | InterruptedException e) {

		} finally {
			closeConn();
		}
	}

	/**
	 * 连接zookeeper
	 *
	 * @param host 地址
	 */
	public void connectZookeeper(String host) {
		try {
			zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
			countDownLatch.await();
		} catch (IOException | InterruptedException e) {

		}
	}

	/**
	 * 关闭连接
	 */
	public void closeConn() {
		try {
			if (zookeeper != null) {
				zookeeper.close();
			}
		} catch (InterruptedException e) {

		}
	}

	/**
	 * 获取节点内容
	 *
	 * @param path 节点路径
	 * @return 内容/异常null
	 */
	public String getNodeData(String path, String zookeeperIp) {
		String result = null;
		Stat stat = new Stat();
		connectZookeeper(zookeeperIp);
		try {
			byte[] resByte = zookeeper.getData(path, true, stat);
			result = new String(resByte);
		} catch (KeeperException | InterruptedException e) {
			logger.error("Get node information exception");

			return null;
		} finally {
			closeConn();
		}
		return result;
	}

	/**
	 * @param path 节点创建的路径
	 * @param data 节点所存储的数据的byte[]
	 * @param acls 控制权限策略
	 */
	public void createNode(String path, byte[] data, List<ACL> acls, String zookeeperIp) {
		try {
			connectZookeeper(zookeeperIp);
			List<String> list = Splitter.on("/").omitEmptyStrings().splitToList(path);
			StringBuffer sb= new StringBuffer();
			for (String dir : list) {
				sb.append("/").append(dir);
				Stat exists = zookeeper.exists(sb.toString(), true);
				if (exists == null) {
					zookeeper.create(sb.toString(), null, acls, CreateMode.PERSISTENT);
				}
			}
			Stat exists = zookeeper.exists(path, true);
			if (exists == null) {
				zookeeper.create(path, data, acls, CreateMode.PERSISTENT);
			} else {
				logger.warn("Node already exists ! Don't need to create");
			}
		} catch (KeeperException | InterruptedException e) {

		} finally {
			closeConn();
		}
	}

}
