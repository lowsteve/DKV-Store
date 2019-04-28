package watchers;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class nodeWatcher implements Watcher, Runnable {

  private static Logger logger = Logger.getRootLogger();
  public static Queue<queueMessage> sharedQueue = new ConcurrentLinkedQueue<queueMessage>();
  private String zkPath = null;
  private String hostCredentials = null;

  ZooKeeper zk = null;
  byte[] zData = null;
  private String watchType = null;

  public nodeWatcher(String path, String hostCredentials, String type) {
    this.zkPath = path;
    this.hostCredentials = hostCredentials;
    this.watchType = type;

    try {
      new LogSetup("logs/watch/" + hostCredentials + "_" + path + "_"
          + type + ".log", Level.INFO);
      zk = new ZooKeeper(this.hostCredentials, 2000, this);
      this.setInitWatch();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public nodeWatcher() {

  }

  public Queue<queueMessage> getSharedQueue() {
    return sharedQueue;
  }

  public String getHostCredentials() {
    return hostCredentials;
  }

  public void setHostCredentials(String hostCredentials) {
    this.hostCredentials = hostCredentials;
  }

  public String getZkPath() {
    return zkPath;
  }

  public void setZkPath(String zkPath) {
    this.zkPath = zkPath;
  }

  @Override
  public void process(WatchedEvent event) {
    // TODO depending on the event, process the data differently

    try {
      if (this.watchType == "NODE_DATA_CHANGED"
          && (event.getType() == Event.EventType.NodeDataChanged)) {
        handleData();
      } else if (this.watchType == "NODE_CHILDREN_CHANGED"
          && (event.getType() == Event.EventType.NodeChildrenChanged)) {
        handleChildren();
      } else if (this.watchType == "NODE_CREATED"
          && (event.getType() == Event.EventType.NodeCreated)) {
        handleExistence(this.watchType);
      } else if (this.watchType == "NODE_DELETED"
          && (event.getType() == Event.EventType.NodeDeleted)) {
        handleExistence(this.watchType);
      } else {

      }
    } catch (InterruptedException | KeeperException e) {
      e.printStackTrace();
    }
  }

  public void handleData() throws InterruptedException, KeeperException {
    // TODO decide what to do with the data
    try {
      zData = zk.getData(this.zkPath, this, null);
      String dataString = new String(zData);
      System.out.println("handleData: " + dataString);
      logger.debug("handleData: " + dataString);
      addToQueue(dataString, this.watchType, this.zkPath);
    } catch (Exception e) {
      throw e;
    }
  }

  public void handleChildren() throws InterruptedException, KeeperException {
    // TODO decide what to do with the data
    try {
      List<String> zkChildren = zk.getChildren(zkPath, this);

      System.out.println("handleChildren: " + zkChildren);
      addToQueue(zkChildren, this.watchType, this.zkPath);

      logger.debug("handleChildren: " + zkChildren);
    } catch (Exception e) {
      throw e;
    }

  }

  public void handleExistence(String type) throws InterruptedException, KeeperException {
    // TODO decide what to do with the data
    try {
      Stat zkStat = zk.exists(zkPath, this);


      System.out.println("handleExistence -" + type + ": " + this.watchType + this.zkPath);
      addToQueue((String) null, this.watchType, this.zkPath);


    } catch (Exception e) {
      throw e;
    }
  }

  public synchronized void addToQueue(List<String> zkData, String zkType, String zkPath) {
    sharedQueue.add(new queueMessage(zkType, zkData, zkPath));
  }

  public synchronized void addToQueue(String zkData, String zkType, String zkPath) {
    sharedQueue.add(new queueMessage(zkType, zkData, zkPath));
  }


  public void setInitWatch() throws Exception {
    try {
      if (this.watchType == "NODE_DATA_CHANGED") {
        zk.getData(this.zkPath, this, null);
      } else if (this.watchType == "NODE_CHILDREN_CHANGED") {
        zk.getChildren(zkPath, this);
      } else if (this.watchType == "NODE_CREATED"
          || this.watchType == "NODE_DELETED") {
        zk.exists(zkPath, this);
      } else {

      }
    } catch (KeeperException | InterruptedException e) {
      throw e;
    }
    logger.debug("setInitWatch");
  }

  public void run() {
    // stay idle if there is no watch being triggered
    try {
      synchronized (this) {

        while (true) {
          wait();
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }
}
