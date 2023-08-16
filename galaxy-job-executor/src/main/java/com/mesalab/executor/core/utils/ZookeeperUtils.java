package com.mesalab.executor.core.utils;

import cn.hutool.log.Log;
import com.google.common.base.Splitter;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperUtils implements Watcher {
	private Log logger = Log.get();
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
			logger.error(e);
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
			logger.error(e);
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
			logger.error(e);
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
			Stat exists = zookeeper.exists(path, true);
			if (exists == null) {
				return null;
			}
			byte[] resByte = zookeeper.getData(path, true, stat);
			result = new String(resByte, 0, resByte.length);
		} catch (Exception e) {
			logger.error("Get node information exception");
			logger.error(e);
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
			logger.error(e);
		} finally {
			closeConn();
		}
	}

	public List<String> getChildren(String path, String zookeeperIp) {
		try {
			connectZookeeper(zookeeperIp);
			Stat exists = zookeeper.exists(path, true);
			if (exists == null) {
				return null;
			}
			List<String> children = zookeeper.getChildren(path, false);
			return children;
		} catch (Exception e) {
			logger.error("Get node information exception ,{}",e.getMessage());
			return null;
		} finally {
			closeConn();
		}
	}
	public boolean deleteNode(String path, String zookeeperIp) {
		try {
			connectZookeeper(zookeeperIp);
			Stat exists = zookeeper.exists(path, false);
			if (exists == null) {
				logger.error("{} node not exist", path);
				return false;
			}
			zookeeper.delete(path, -1); // -1表示忽略版本号检查
			return true;
		} catch (Exception e) {
			logger.error("Delete node information exception ,{}",e.getMessage());
			return false;
		} finally {
			closeConn();
		}
	}
}
