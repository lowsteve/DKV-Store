package testing;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import common.KVAdminMsg;
import common.ZkAdmin;

import app_kvECS.ECSClient;
import client.KVStore;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import common.KVMessage;
import ecs.IECSNode;


public class ECSTests {
  
  private KVStore kvClient;
  private static ECSClient ecsClient;
  Exception ex = null;
  private static ZooKeeper zk = null;
  private Logger logger = Logger.getRootLogger();
  private String zkHostname = "localhost";
  private int zkPort = 1920;
  
  @Before
  public void setUp() {
    kvClient = new KVStore("localhost", 21231);
    ecsClient = AllTests.ecsClient;
    zk = ZkAdmin.connectZookeeper(zkHostname, zkPort, logger);
    try {
      kvClient.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }    
  }  
  
  @Test
  public void testServerStopped() {
     
    try {
      ecsClient.start(); 
      TimeUnit.SECONDS.sleep(4); 
      ecsClient.stop();   
      assertEquals(KVMessage.StatusType.SERVER_STOPPED,
          kvClient.put("ece", "419").getStatus());
    } catch (Exception e) {
      ex = e;
    }   
      
    assertNull(ex);
    ex = null;
  }

  @Test
  public void testWriteLocked() {
    //for each of the ECS nodes, write lock
    IECSNode nodeToLock = ecsClient.getNodeByKey("testString123");    
   
    try {      
      TimeUnit.SECONDS.sleep(4); 
      ecsClient.start();    
      ecsClient.handleNodeTask(nodeToLock.getNodeName(), KVAdminMsg.CommandType.WRITE_LOCK); 
      assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK,
          kvClient.put("testString123", "testValue123").getStatus());
      
    } catch (Exception e) {
      ex = e;
    }   
      
    try {
      ecsClient.handleNodeTask(nodeToLock.getNodeName(), KVAdminMsg.CommandType.RELEASE_LOCK);
    } catch (Exception e) {
      ex = e;
    } 
    
    assertNull(ex);    
    ex = null;
  }
    
  @Test
  public void testMoveData() {
    
    String[] keys = {"t3","a","ils","omg","87g"};
    IECSNode oldNode = ecsClient.getNodeByKey(keys[0]);
    
    PrintWriter writer = null;
    try {
      TimeUnit.SECONDS.sleep(6); 
      File fio = new File("KVServer_Storage/server1");
      fio.delete();
      writer = new PrintWriter(fio);
      for(String key : keys) {
        String value = "temp";
        writer.println(key+" "+value);
      }
    } catch (Exception e1) {
    } finally {
      if(writer != null) {
        writer.close();
      }
    }
    
    //issue move data
    List<String> nodeName = new ArrayList<>();
    nodeName.add("server1");
    ecsClient.removeNodes(nodeName);
    
    //determine responsible server    
    try {
      IECSNode newNode = ecsClient.getNodeByKey(keys[0]);
      KVStore newKVClient = new KVStore("localhost",newNode.getNodePort());
      boolean namesEqual = oldNode.equals(newNode);
      assertEquals(namesEqual,false);
      newKVClient.connect();
      
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          newKVClient.get(keys[0]).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          newKVClient.get(keys[1]).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          newKVClient.get(keys[2]).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          newKVClient.get(keys[3]).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          newKVClient.get(keys[4]).getStatus());
      
    } catch (Exception e) {
      ex = e;
    }
    assertNull(ex);   
    ex = null;
  }
    
  @Test
  public void testAddSurplusNodes() {
    ecsClient.shutdown();
    
    try {      //exp , act
      TimeUnit.SECONDS.sleep(4); 
      assertEquals(ecsClient.addNodes(1000, "FIFO", 10).size(),
         ecsClient.getActiveNodes().size());
    } catch (Exception e) {
      ex = e;
    }   
      
    assertNull(ex);   
    ex = null;
  }
  
  @Test
  public void testShutDown() {

    try {
      TimeUnit.SECONDS.sleep(10); 
      ecsClient.shutdown(); 
       TimeUnit.SECONDS.sleep(10);      
       List<String> childrenStopped = zk.getChildren("/members/stopped", null);
       List<String> childrenStarted = zk.getChildren("/members/started", null);
      
       assertEquals(0,childrenStopped.size());
       assertEquals(0,childrenStarted.size());
    } catch (Exception e) {
      ex = e;
    }   
      
    assertNull(ex);
    ex = null;
  }
  
  @Test
  public void testStartNoNodes() {
   
    try {
      assertEquals(ecsClient.start(), false);
    } catch (Exception e) {
      ex = e;
    }   
      
    assertNull(ex);
    ecsClient.start();
    ex = null;
  }
  
  
}

