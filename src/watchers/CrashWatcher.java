package watchers;

import app_kvECS.ECSClient;
import common.ZkAdmin;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import ecs.ECSNode;
import java.io.IOException;
import java.util.List;

public class CrashWatcher implements Watcher{

  private static final String MEMBERS_STARTED_PATH = "/members/started";
  private static final String MEMBERS_STOPPED_PATH = "/members/stopped";
  private static final String tasksDirChild = "/tasks/";

  private static Logger logger = Logger.getRootLogger();
  private ECSClient ecs = null;
  private ZooKeeper zk = null;

  public String zkHostname = null;
  public int zkPort = -1;
  
  public Object lock = new Object();


  public CrashWatcher(ECSClient ecs, String zkHostname, int zkPort) {


    try {
      this.ecs = ecs;
      this.zkHostname = zkHostname;
      this.zkPort = zkPort;
      new LogSetup("logs/watch/CrashWatcher" + ".log", Level.OFF);

      zk = ZkAdmin.connectZookeeper(zkHostname, zkPort, logger);

      if (zk == null) {
        throw new NullPointerException(
            "CrashWatcher: Connect to ZooKeeper failed.");
      }
      zk.getChildren(MEMBERS_STARTED_PATH, this);
      zk.getChildren(MEMBERS_STOPPED_PATH, this);
    } catch (InterruptedException e) {
      logger.info("InterruptedException in CrashWatcher");
      e.printStackTrace();
    } catch (KeeperException e) {
      logger.info("KeeperException in CrashWatcher");
      e.printStackTrace();
    } catch (IOException e) {
      logger.info("Problem setting up the logger.");
      e.printStackTrace();
    }
  }

 
  @Override
  public void process(WatchedEvent event) {

    logger.info("CRASHWATCHER: Process Event: " + event.getType());

    CrashDispatcher cd = new CrashDispatcher(event);
    new Thread(cd).start();

    try {
      setWatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void setWatch() throws KeeperException, InterruptedException {
    try {
      zk.getChildren(MEMBERS_STARTED_PATH, this);

    } catch (KeeperException.NoNodeException nne) {
      if (!ecs.getActiveNodes().isEmpty() && !ecs.isWantsToQuit()) {
        nne.printStackTrace();

        logger.info("Creating \"" + MEMBERS_STARTED_PATH + "\"...");

        // create members_started_path if does not exist
        zk.create(MEMBERS_STARTED_PATH, "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        logger
            .debug("exception has detected that there are no nodes, " +
            		   "but active nodes is empty so this is expected");
      }
    } catch (Exception e) {
      if (!ecs.getActiveNodes().isEmpty()) {
        logger.error("Failed to create path: " + MEMBERS_STARTED_PATH + "...",
            e);
        e.printStackTrace();
      }
    }

    try {
      zk.getChildren(MEMBERS_STOPPED_PATH, this);

    } catch (KeeperException.NoNodeException nne) {
      if (!ecs.getActiveNodes().isEmpty() && !ecs.isWantsToQuit()) {
        nne.printStackTrace();

        logger.info("Creating \"" + MEMBERS_STOPPED_PATH + "\"...");

        // create members_stopped_path if does not exist
        zk.create(MEMBERS_STOPPED_PATH, "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        logger
            .debug("exception has detected that there are no nodes," +
            		   "but active nodes is empty so this is expected");
      }
    } catch (Exception e) {
      if (!ecs.getActiveNodes().isEmpty()) {
        logger.error("Failed to create path: " + MEMBERS_STOPPED_PATH + "...",
            e);
        e.printStackTrace();
      }
    }
  }

  private class CrashDispatcher implements Runnable {

    WatchedEvent event = null;

    public CrashDispatcher(WatchedEvent event) {
      this.event = event;
    }

    public void run() {
      synchronized (lock) {
        if (event.getType() == Event.EventType.NodeChildrenChanged && !ecs.isAddingNodes()) {
          logger.info("CrashDispatcher Processing NodeChildrenChanged event");
          try {
            
            // get the list of all the children in the started and stopped dir
            List<String> memberStartedNames = zk.getChildren(
                MEMBERS_STARTED_PATH, null);
            List<String> memberStoppedNames = zk.getChildren(
                MEMBERS_STOPPED_PATH, null);
            List<ECSNode> activeMembers = ecs.getActiveNodes();
            List<ECSNode> inactiveMembers = ecs.getInactiveNodes();
            
            for (ECSNode node : activeMembers) {
              String name = node.getServerName();

              if (!memberStartedNames.contains(name) && 
                  !memberStoppedNames.contains(name)) {
                // node is not present on zk system, therefore need to remove
                // the node from the activeMembers
                ZKUtil.deleteRecursive(zk, tasksDirChild + name);
                inactiveMembers.add(node);
                activeMembers.remove(node);
              }
            }

            if (activeMembers.isEmpty() && ecs.isWantsToQuit()) {
              ecs.kill();
            }
          } catch (Exception e) {
            logger.error("TaskDispatcher: Exception: ", e);
          }
        }
      }
    }
  }
}
