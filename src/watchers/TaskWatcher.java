package watchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import com.google.gson.Gson;

import app_kvServer.KVServer;

import common.KVAdminMsg;
import common.KVAdminMsg.CommandType;

public class TaskWatcher implements Watcher {
  private static Logger logger = Logger.getRootLogger();
  
  private ZooKeeper zk = null;
  private KVServer kvs = null;
  private String kvs_task_dir_path = null;    //Format: "/tasks/server1"
  public  Object lock = new Object();
  public  List<String>prevChildZnodeSet = new ArrayList<String>();
  
  public TaskWatcher(
      KVServer kvs, 
      ZooKeeper zk, 
      String kvs_task_dir_path) {
    try{
      new LogSetup("logs/watch/Task_Watcher/" + kvs_task_dir_path + ".log", Level.INFO);
      
      this.zk = zk;
      this.kvs_task_dir_path = kvs_task_dir_path;
      this.kvs = kvs;
      setWatch();
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("TASKWATCHER Exception: ", e);
    }
  }

  /**
   * Starts new TaskDispatcher for every new event for 
   * separate handling of events so as to be able to 
   * reset the watch on the DistributedTaskQueue as 
   * quickly as possible so as not to miss any events.
   */
  public void process(WatchedEvent event) {
    logger.info("TASKWATCHER: Process Event: " + event.getType());
    
    TaskDispatcher td = new TaskDispatcher(event, kvs_task_dir_path);
    new Thread(td).start();
    
    try {
      setWatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * set child watch on "/tasks/serverx"
   */
  private void setWatch() throws KeeperException, InterruptedException {
    try {
      zk.getChildren(kvs_task_dir_path, this);
    } catch (KeeperException.NoNodeException nne) {
      if(kvs.isRunning()){
        nne.printStackTrace();
        
        logger.info("TASKWATCHER: Creating \"" + kvs_task_dir_path + "\"...");
        
        //create "/tasks/serverx" if does not exist
        zk.create(
            kvs_task_dir_path, 
            "".getBytes(), 
            ZooDefs.Ids.OPEN_ACL_UNSAFE, 
            CreateMode.PERSISTENT);
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Failed to create path: " + kvs_task_dir_path + "...", e);
    }
  }
  
  
  /**
   * A new TaskDispatcher is created upon a watch trigger event. Done 
   * so as to minimize time to reset watch so that we don't miss a watch.
   * 
   * Performs the following:
   * 1) Get the list of children
   * 2) Read KVAdminMsg off of each of the task Znodes
   * 3) Dispatch each new task in a new thread
   * 4) Delete each task Znodes
   */
  private class TaskDispatcher implements Runnable {
    
    WatchedEvent event = null;
    String kvs_task_dir_path = null;
    
    public TaskDispatcher(WatchedEvent event, String kvs_task_dir_path) {
      this.event = event;
      this.kvs_task_dir_path = kvs_task_dir_path;
    }
    
    public void run() {      
      synchronized(prevChildZnodeSet) {
        logger.info("TASKDISPATCHER: Run...");
        
        //read child nodes from "/tasks/serverx"
        try {
          if( !event.getType().equals(Event.EventType.NodeChildrenChanged) ) {
            logger.error("Unhandled event occured: " + event.getType());
            throw new IllegalArgumentException();
          }
          
          /* 1 */
          List<String> currChildZnodeSet = zk.getChildren(kvs_task_dir_path, null);
          
          currChildZnodeSet.removeAll(prevChildZnodeSet); //disregard old tasks
          Collections.sort(currChildZnodeSet);  //sort current tasks
          
          //add new tasks to prev list
          prevChildZnodeSet.addAll(currChildZnodeSet);
          
//          //DEBUG: check if there were any duplicates that were added to the list
//          Set<String> set = new HashSet<String>(prevChildZnodeSet);
//          if(set.size() < prevChildZnodeSet.size()){
//              logger.error("TASKDISPATCHER: Duplicates Detected!");
//          }

          Gson gson = new Gson();
          
          for(String task_znode_name : currChildZnodeSet) {
            logger.info("PROCESSING: " + task_znode_name);
            
            /* 2 */
            String full_path_task_znode = kvs_task_dir_path + "/" + task_znode_name;
            byte[] arr = zk.getData(full_path_task_znode, false, 
                zk.exists(full_path_task_znode, false));
            String msg = new String(arr, "UTF-8");
            logger.info(msg);
            KVAdminMsg kvMsg = gson.fromJson(msg, KVAdminMsg.class);
            
            /* 3 */
            Task task = new Task(kvs, kvMsg);
            new Thread(task).start();            
          }
        } catch (Exception e) {
          e.printStackTrace();
          logger.error("TaskDispatcher: Exception: ", e);
        }
      }

      
      // IMPLEMENTATION THAT DELETES THE TASKS ZNODE AFTER DONE PROCESSING (DO NOT DELETE)
      // process tasks currently present before resetting watch
      // Done so as not to trigger watch due to deletion of task znodes
//      synchronized(lock) {
//
//        //read child nodes from "/tasks/serverx"
//        try {
//          if( !event.getType().equals(Event.EventType.NodeChildrenChanged) ) {
//            System.out.println("Unhandled event occured...");
//            throw new IllegalArgumentException();
//          }
//          
//          /* 1 */
//          List<String> zkChildren = zk.getChildren(kvs_task_dir_path, null);
//          Collections.sort(zkChildren);
//          
//          logger.info("TASKDISPATCHER: " + zkChildren + " Size: " + zkChildren.size());
//          
//          Gson gson = new Gson();
//          
//          for(String task_znode_name : zkChildren) {
//            /* 2 */
//            String full_path_task_znode = kvs_task_dir_path + "/" + task_znode_name;
//            byte[] arr = zk.getData(full_path_task_znode, false, 
//                zk.exists(full_path_task_znode, false));
//            String msg = new String(arr, "UTF-8");
//            logger.info(msg);
//            KVAdminMsg kvMsg = gson.fromJson(msg, KVAdminMsg.class);
//            
//            /* 3 */
//            Task task = new Task(kvs, kvMsg);
//            new Thread(task).start();
//            
//            /* 4 */
//            zk.delete(full_path_task_znode, -1);
//          }
//        } catch (Exception e) {
//          e.printStackTrace();
//          logger.error("TaskDispatcher: Exception: ", e);
//        } finally {
//
//          //reset watch once done with handling event
//          //ideally should be done right after fired watch event so as not to miss any watch events
//          try {
//            setWatch();
//          } catch (Exception e) {
//            e.printStackTrace();
//          }
//        }
//      }

    }
  }
  
  
  /**
   * Performs a single task in a new thread
   */
  private class Task implements Runnable {

    private CommandType command = null;
    private KVServer kvs = null;
    private KVAdminMsg msg = null;
    
    public Task(KVServer kvs, KVAdminMsg msg) {
      this.command = msg.getCmd();
      this.kvs = kvs;
      this.msg = msg;
    }
    
    public void run() {
      logger.info("TASK: Command: " + command);
      
      switch (command) {
        case MOVE_DATA:
          String[] hashRange = this.msg.getArg1();
          String destName = this.msg.getArg2();
          try {
            kvs.moveData(hashRange, destName);
          } catch (Exception e) {
            e.printStackTrace();
          }
          break;
        case LISTEN_DATA:
          String serverAddress = this.msg.getArg1()[0];
          String port = this.msg.getArg2();
          kvs.listenData(serverAddress, port);
          break;
        case WRITE_LOCK:
          kvs.lockWrite();
          break;
        case RELEASE_LOCK:
          kvs.unlockWrite();
          break;
        case START:
          kvs.start();
          break;
        case STOP:
          kvs.stop();
          break;
        case UPDATE:
          kvs.update();
          break;
        case SHUTDOWN:
          kvs.close();
          break;
        default:
          break;
      }
      return;
    }
  }
}
