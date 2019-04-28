package common;

import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HashRing {
  private List<ECSNode> serverNodes;
  private final Object hashRingMutex = new Object();
  private static Logger logger = Logger.getRootLogger();

  public HashRing() {
    this.serverNodes = new ArrayList<>();
    try {
      new LogSetup("logs/watch/" + "hashRingWatch" + ".log", Level.OFF);
      logger.info("hash ring object made");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public ECSNode getResponsibleServer(String key) {
    logger.info("in get Responsible Server");
    synchronized (hashRingMutex) {
      try {
        if (this.serverNodes != null && !this.serverNodes.isEmpty()) {
          MessageDigest md5 = MessageDigest.getInstance("MD5");
          byte[] digestArr = md5.digest(key.getBytes());
          BigInteger keyHash = new BigInteger(1, digestArr);
          logger.info("serverNodes value is " + serverNodes.toString());
          for (ECSNode node : serverNodes) {
            boolean isWrapAround = node.getHashStart().compareTo(
                node.getHashEnd()) > 0;
            if (isWrapAround) {
              if (keyHash.compareTo(node.getHashStart()) > 0) {
                return node;
              } else if (keyHash.compareTo(node.getHashEnd()) <= 0) {
                return node;
              }
            } else if (keyHash.compareTo(node.getHashStart()) > 0
                && keyHash.compareTo(node.getHashEnd()) <= 0) {
              return node;
            }
          }
        }
      } catch (NoSuchAlgorithmException nsae) {
        nsae.printStackTrace();
      }
    }
    logger.info("no responsible server found!");
    return null;
  }

  public void update(List<String> nodeList) {
    synchronized (hashRingMutex) {
      logger.info("update: nodeList " + nodeList);
      if (nodeList != null && !nodeList.isEmpty()) {
        
        List<ECSNode> newRing = new ArrayList<>();
        String[] credentials = null;
        // cred[0]: HOST
        // cred[1]: PORT
        // cred[2]: NAME
        
        for (String node : nodeList) {
          credentials = node.split(":");
          ECSNode newNode = new ECSNode(credentials[2], null,
              Integer.parseInt(credentials[1]), credentials[0], null, null);
          newRing.add(newNode);
        }
        calcHashes(newRing);       
        
        this.serverNodes = newRing;
      } else {
        logger.info("update server nodes to null");
        this.serverNodes = null;
      }
    }
  }

  public ECSNode getNodeOfServerName(String serverName, int portNum) {
    synchronized (hashRingMutex) {
      if(this.serverNodes != null && !this.serverNodes.isEmpty()){
        for (ECSNode i : this.serverNodes) {
          
          logger.debug("EXPECTED > " + serverName + " "
              + Integer.toString(portNum) + "ACTUAL> " + i.getServerName() + " "
              + Integer.toString(i.getNodePort()));
          
          if (i != null && i.getServerName().equals(serverName)
              && i.getNodePort() == portNum) {
            return i;
          }
        }
      }
    }
    logger.info("no node found from server name + port num!");
    return null;
  }

  public List<ECSNode> getServerNodes() {
    synchronized (hashRingMutex) {
      return serverNodes;
    }
  }

  public ECSNode getNodeOfCredentials(String hostName, int portNum) {
    synchronized (hashRingMutex) {
      for (ECSNode i : this.serverNodes) {
        if (i != null
            && i.getNodeHost() == hostName
            && i.getNodePort() == portNum) {
          return i;
        }
      }
    }
    logger.info("no node found from host name + port num!");
    return null;
  }

  private void calcHashes(List<ECSNode> newRing) {
    /* WARN: Do not synchronize this. It will deadlock b/c it is called within a mutex. */
    calcEndHashes(newRing);
    calcStartHashes(newRing);
  }

  private void calcEndHashes(List<ECSNode> newRing) {
    /* WARN: Do not synchronize this. It will deadlock b/c it is called within a mutex. */
    for (ECSNode node : newRing) {
      String ipPort = node.getNodeIP() + ":" + node.getNodePort();
      try {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] digestArr = md5.digest(ipPort.getBytes());
        node.setHashEnd(new BigInteger(1, digestArr));
      } catch (NoSuchAlgorithmException nsae) {
        nsae.printStackTrace();
      }
    }
  }
    
  private void calcStartHashes(List<ECSNode> newRing) {
    /* WARN: Do not synchronize this. It will deadlock b/c it is called within a mutex. */

    // Corner case for single node
    if(newRing.size() == 1) {
      newRing.get(0).setHashStart(newRing.get(0).getHashEnd().add(BigInteger.ONE));
      return;
    }
    Collections.sort(newRing, ECSNode.hashValEndComp);
    newRing.get(0).setHashStart(
        newRing.get(newRing.size() - 1).getHashEnd().add(BigInteger.ONE));
    for (int i = 1; i < newRing.size(); i++) {
      newRing.get(i).setHashStart(
          newRing.get(i - 1).getHashEnd().add(BigInteger.ONE));
    }
  }


}
