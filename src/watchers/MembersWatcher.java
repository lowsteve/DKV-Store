package watchers;

import common.HashRing;
import common.ZkAdmin;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MembersWatcher implements Watcher, Runnable {

  private static Logger logger = Logger.getRootLogger();
  private static final String MEMBERS_STARTED_PATH = "/members/started";
  private static final String MEMBERS_STARTED_CHILD = "/members/started/";

  private ZooKeeper zk = null;
  private HashRing hashRing;

  public String zkHostname = null;
  public int zkPort = -1;
  
  public MembersWatcher(HashRing hashRing, String startingServer, String zkHostname, int zkPort) {

    this.hashRing = hashRing;
    this.zkHostname = zkHostname;
    this.zkPort = zkPort;
    try {
      new LogSetup("logs/watch/" + startingServer + "_MembersWatcher" +
          ".log", Level.OFF);

      zk = ZkAdmin.connectZookeeper(zkHostname, zkPort, logger);

      if (zk == null) {
        throw new NullPointerException("MembersWatcher: Connect to ZooKeeper failed.");
      }
      zk.getChildren(MEMBERS_STARTED_PATH, this);
    } catch (InterruptedException e) {
      logger.info("InterruptedException in MembersWatcher");
      e.printStackTrace();
    } catch (KeeperException e) {
      logger.info("KeeperException in MembersWatcher");
      e.printStackTrace();
    } catch (IOException e) {
      logger.info("Problem setting up the logger.");
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    // stay idle if there is no watch being triggered
    try {
      synchronized (this) {
        
        logger.info("MemebersWatcher waiting for member changes");
        while (true) {
          wait();
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    try {
      if (event.getType() == Event.EventType.NodeChildrenChanged) {
        handleChildren();
      }
    } catch (InterruptedException | KeeperException e) {
//      e.printStackTrace();
    }
  }

  public void handleChildren() throws InterruptedException, KeeperException {
    logger.info("Processing NodeChildrenChanged event to update hashRing");
    try {
      List<String> memberNames = zk.getChildren(MEMBERS_STARTED_PATH, this);
      List<String> memberZData = new ArrayList<>();
      logger.info("handle Children member names: " + memberNames);
      if (memberNames != null && !memberNames.isEmpty()) {
        for (String member : memberNames) {
          byte[] memberData = zk.getData(MEMBERS_STARTED_CHILD + member, null,
              null);
          memberZData.add(new String(memberData));
        }
        hashRing.update(memberZData);
        logger.info("MEMBERS WATCHER update hashRing to: " + hashRing.getServerNodes());
      } else {
        hashRing.update(null);
      }
    } catch (Exception e) {
      throw e;
    }
  }

}
