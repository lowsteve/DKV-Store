package common;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public final class ZkAdmin {

  /* Utility class. We don't want instances. */
  private ZkAdmin() {

  }


  public static ZooKeeper connectZookeeper(String zkHostname, int zkPort, Logger logger) {
    try {
      final CountDownLatch connectedSignal = new CountDownLatch(1);
      String zkCredentials = zkHostname+":"+zkPort;
      ZooKeeper zk = new ZooKeeper(zkCredentials, 3000, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
          }
        }
      });

      logger.info("Waiting for ZooKeeper connection...");
      connectedSignal.await();
      logger.info("Connection to ZooKeeper successful");
      return zk;
    } catch (InterruptedException ie) {
      logger.info("Thread interrupted while setting up zookeeper");
    } catch (IOException ioe) {
      logger.info("Interrupted exception in connectZookeeper");
    }
    return null;
  }
}
