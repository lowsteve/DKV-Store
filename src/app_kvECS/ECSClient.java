package app_kvECS;

import com.google.gson.Gson;
import common.HashRing;
import common.KVAdminMsg;
import common.KVAdminMsg.CommandType;
import common.ZkAdmin;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import watchers.CrashWatcher;
import watchers.MembersWatcher;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ECSClient implements IECSClient {

  private Logger logger = Logger.getRootLogger();
  private static final String STARTED_DIR = "/members/started";
  private static final String MEMBERS_DIR = "/members";
  private static final String STOPPED_DIR = "/members/stopped";
  private static final String TASK_DIR = "/tasks";
  public static final String persistentFilename = "kvpairfile";
  private static final String PROMPT = "ECSClient> ";
  
  private static final int TIMEOUT_VALUE = 10000;
  
  private boolean isRunning = false;
  private boolean wantsToQuit = false;
  private List<ECSNode> inactiveNodes;
  private List<ECSNode> activeNodes;


  public void setActiveNodes(List<ECSNode> activeNodes) {
    this.activeNodes = activeNodes;
  }

  private static Gson gson = new Gson();
  private static ZooKeeper zk = null;

  private boolean addingNodes = false;
  private String zkHostname = null;
  private int zkPort = -1;
  
  public boolean isAddingNodes() {
    return addingNodes;
  }
  
  private HashRing hashRing;

  public HashRing getHashRing() {
    return hashRing;
  }

  public ECSClient(String config_file, String zkHostname, int zkPort) {
    this.zkHostname = zkHostname;
    this.zkPort = zkPort;
    this.wantsToQuit = false;
    this.hashRing = new HashRing();
    activeNodes = new ArrayList<>();
    inactiveNodes = new ArrayList<>();

    zk = ZkAdmin.connectZookeeper(zkHostname, zkPort, logger);

    loadConfig(config_file);
    initECSClient();
    loadExistingServiceState();
    if (zk == null) {
      logger.info("Couldn't connect to ZooKeeper");
    }
  }

  private void initECSClient() {
    createTaskAndAcksZnode();
    setupMemberNodes();
    addingNodes = false;
    // place a watcher under the started dir for the hash ring.
    Stat fileStat;
    try {
      fileStat = zk.exists(STARTED_DIR, false);
      if (fileStat != null) {

        MembersWatcher membersWatcher = 
            new MembersWatcher(hashRing, "ECS_CLIENT", this.zkHostname, this.zkPort);
        new Thread(membersWatcher).start();
        new CrashWatcher(this, this.zkHostname, this.zkPort);

      }
    } catch (KeeperException | InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  private void loadExistingServiceState() {

    String[] dirList = { STARTED_DIR, STOPPED_DIR };
    try {
      for (String dir: dirList) {
        Stat dirStat =  zk.exists(dir, false);
        if (dirStat != null) {
          List<String> serverList = zk.getChildren(dir, false);
          if (serverList != null && !serverList.isEmpty()) {
            for (String server : serverList) {
              String serverData = new String(zk.getData(dir + "/" + server, null, null));
              String serverName = serverData.split(":")[2];
              List<ECSNode> tempList = new ArrayList<>();
              for (ECSNode node : inactiveNodes) {
                if (node.getNodeName().split(" ")[0].equals(serverName)) {
                  tempList.add(node);
                }
              }
              activeNodes.addAll(tempList);
              inactiveNodes.removeAll(tempList);
            }
          }
        }
      }
    } catch (KeeperException e) {
      logger.info("loadExistingServiceState: KeeperException");
      e.printStackTrace();
    } catch (InterruptedException e) {
      logger.info("loadExistingServiceState: Interrupted Exception");
      e.printStackTrace();
    }

  }

  /**
   * Load the config file into the array of inactive nodes. Start hash values
   * will be set to null and must be updated when they are placed into the hash
   * ring.
   */
  private void loadConfig(String config_file) {
    FileReader file = null;
    BufferedReader reader = null;
            
    try {
      file = new FileReader(config_file);
      reader = new BufferedReader(file);
      String line;
      while ((line = reader.readLine()) != null) {
        String[] configLine = line.split(" ", 3);
                
        String serverName = configLine[0];
        String serverIP = configLine[1];
        int serverPort = Integer.parseInt(configLine[2]);
        String serverHost = null;
        String hostName = null;        
        
        if(serverIP.equals("127.0.0.1")){
          serverHost = InetAddress.getLocalHost().getHostName();
        } else {        
          InetAddress addr = InetAddress.getByName(serverIP);
          hostName = addr.getHostName();
          String[] remoteHostFQDN = hostName.split("\\.",4);
          serverHost = remoteHostFQDN[0];         
        }
               
        // Calculate hash of <IP>:<Port>***/
        String credentials = serverIP + ":" + serverPort;
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] digestArr = md5.digest(credentials.getBytes());
        
        ECSNode node = new ECSNode(serverName, serverIP, serverPort, serverHost,
            null, new BigInteger(1, digestArr));

        inactiveNodes.add(node);
      }
    } catch (FileNotFoundException fnfe) {
      logger.info(String.format("Error file %s does not exist!", config_file));
    } catch (IOException ioe) {
      logger.info("Error reading config file");
    } catch (NoSuchAlgorithmException nsae) {
      logger.info("No MD5 algorithm exception");
    } finally {
      if (file != null) {
        try {
          file.close();
        } catch (Exception e) {
          logger.info("ERR: File close failed");
        }
      }

      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          logger.info("ERR: Reader close failed");
        }
      }
    }
  }

  private void createTaskAndAcksZnode() {
    try {
      Stat tasksNode = zk.exists("/tasks", false);
      if (tasksNode == null) {

        zk.create("/tasks", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      }

      Stat tasksNodeAck = zk.exists("/acks", false);
      if (tasksNodeAck == null) {

        zk.create("/acks", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      }

    } catch (KeeperException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private ECSNode getECSNodeFromServerName(String server_name) {
    for (ECSNode node : activeNodes) {
      if (node.getServerName().equals(server_name) == true) {
        return node;
      }
    }
    return null;
  }

  private ECSNode getSuccessorNode(ECSNode curr_node) {
    for (int i = 0; i < activeNodes.size(); i++) {
      if (activeNodes.get(i).equals(curr_node)) {
        if (i + 1 == activeNodes.size()) {
          return activeNodes.get(0);
        } else {
          return activeNodes.get(i + 1);
        }
      }
    }

    logger.error("ERR: Successor node calculation incorrect...");
    return null;
  }

  public void issueMoveDataCommands(List<String> toBeStartedNodes) {
    try {
      for (int i = 0; i < toBeStartedNodes.size(); i++) {
        String server_name = toBeStartedNodes.get(i);
        ECSNode server_node = getECSNodeFromServerName(server_name);
        ECSNode successor_node = getSuccessorNode(server_node);

        if (successor_node == null || successor_node == server_node) {
          continue;
        }

        // WRITE_LOCK
        KVAdminMsg cmd_write_lock = new KVAdminMsg(
            KVAdminMsg.CommandType.WRITE_LOCK, null, null);
        String msg_write_lock = gson.toJson(cmd_write_lock);

        zk.create("/tasks/" + server_name + "/task", msg_write_lock.getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        // MOVE_DATA
        KVAdminMsg cmd_move_data = new KVAdminMsg(
            KVAdminMsg.CommandType.MOVE_DATA, server_node.getNodeHashRange(),
            server_node.getNodeName());
        String msg_move_data = gson.toJson(cmd_move_data);

        // create a task on the successor server's ZNode
        zk.create("/tasks/" + successor_node.getServerName() + "/task",
            msg_move_data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_SEQUENTIAL);

        // wait for acks before starting sending "start" and delete acks when
        // done
        boolean success = false;
        long startTime = System.currentTimeMillis();
        List<String> children = null;
        while (System.currentTimeMillis() - startTime < 5000) {
          children = zk.getChildren("/acks", null);
          if (children != null && children.size() >= 1) {
            logger.info("issueMoveDataCommands: Found: " + children);
            zk.delete("/acks/" + children.get(0), -1);
            success = true;
            break;
          }
        }

        if (!success) {
          logger.error("issueMoveDataCommands: Acknowledgement timed out for: "
              + server_name);
        }

        // RELEASE_LOCK
        KVAdminMsg cmd_release_lock = new KVAdminMsg(
            KVAdminMsg.CommandType.RELEASE_LOCK, null, null);
        String msg_release_lock = gson.toJson(cmd_release_lock);

        zk.create("/tasks/" + server_name + "/task",
            msg_release_lock.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_SEQUENTIAL);
      }
    } catch (Exception e) {
      logger.error("issueMoveDataCommands: Exception: ", e);
    }
  }

  private List<String> getListOfNodesToStart() {
    List<String> startedNodes = null;
    List<String> toBeStartedNodes = null;

    try {
      startedNodes = zk.getChildren("/members/started", false);
      toBeStartedNodes = new ArrayList<String>();

      // construct the list of node names of all active nodes
      for (ECSNode node : activeNodes) {
        toBeStartedNodes.add(node.getServerName());
      }

      // exclude the servers that have already been started
      toBeStartedNodes.removeAll(startedNodes);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return toBeStartedNodes;
  }

  /**
   * 1) Parse commands from ecs.config 2)
   * <p>
   * Launches instances of KVServers through SSH calls
   * <p>
   * Calls start() on each KVServer instance, i.e.
   * "Starts the KVServer, all client requests and all ECS requests are processed."
   */
  @Override
  public boolean start() {

    if(!activeNodes.isEmpty()) {

      //get list of znode names for the server

      List<String> list_of_server_names = getListOfNodesToStart();

      
      if(list_of_server_names.size() >= 1) {
        issueMoveDataCommands(list_of_server_names);
      }
      
      //Only send START to the servers that are not in the start state
      for (int i = 0 ; i < list_of_server_names.size() ; i++){
        //System.out.println("SENDING START TO: " + list_of_server_names.get(i));

        try {
          handleNodeTask(list_of_server_names.get(i),
              KVAdminMsg.CommandType.START);
        } catch (Exception e) {
          logger.error("start: Exception: ", e);

          return false;
        }
      }
    } else {
      System.out.println(PROMPT + "error: no nodes added, so cant start");
      return false;
    }

    System.out.println(PROMPT + "all servers have been started");
    return true;

  }

  @Override
  public boolean stop() {

    if ((activeNodes != null) && (activeNodes.size() != 0)) {
      logger.debug("Handling stop");

      for (ECSNode node : activeNodes) {
        try {
          handleNodeTask(node.getServerName(), KVAdminMsg.CommandType.STOP);
        } catch (Exception e) {

          logger.error("error in stop()", e);

          return false;
        }
      }
    } else {
      return false;
    }

    System.out.println(PROMPT + "all servers have been stopped");

    return true;
  }

  @Override
  public boolean shutdown() {

    if((!activeNodes.isEmpty())){

      logger.info("Handling shutDown");

      if(!activeNodes.isEmpty()){
        for (ECSNode node : activeNodes) {
          //System.out.println("SENDING SHUTDOWN: " + node.getServerName());
          try {
            
            handleNodeTask(node.getServerName(), KVAdminMsg.CommandType.SHUTDOWN);
          } catch (Exception e) {
            logger.error("error in shutdown",e);
            return false;
          }
        }
      }
      try {
        while (true) {
          List<String> tasks = zk.getChildren(TASK_DIR, null, null);
          if (tasks.size() == 0) { break; }
        } 
      } catch (KeeperException e) {
        System.out.println("ECSClient.shutdown() - KeeperException");
      } catch (InterruptedException e) {
        System.out.println("ECSClient.shutdown() - InterruptedException");
      }
    }

    // all nodes are shutdown, so remove everything from the active nodelist
    inactiveNodes.addAll(activeNodes);
    activeNodes.clear();

    this.wantsToQuit = true;
    System.out.println(PROMPT + "all servers have been shutdown");
    return true;
  }

  public void handleNodeTask(String znodeName, KVAdminMsg.CommandType cmd)
      throws Exception {
    try {
      String jsonMsg = gson.toJson(new KVAdminMsg(cmd, null, null));
      zk.create("/tasks/" + znodeName + "/task", jsonMsg.getBytes(),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (Exception e) {
      throw e;
    }
  }

  
  private void sshStartServer(String host, int port, int cacheSize, String cacheStrat, 
      String serverName, String zkHostname, int zkPort) {

    Process proc;
    String script = "scripts/ssh-start-server.sh";
        
    String[] cmdArray = { script,
                          host,   //TODO, find the host name of the KVserver based on the IP & port 
                          String.valueOf(port),
                          String.valueOf(cacheSize),
                          cacheStrat,
                          serverName,
                          zkHostname,   
                          String.valueOf(zkPort),                          
                          "OFF" };


    Runtime run = Runtime.getRuntime();
    try {
      proc = run.exec(cmdArray);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private ECSNode removeAndReturn(List<ECSNode> list, String nodeName) {
    // NOTE: careful here, I modify the list so size changes
    // Assumption is that list size checks have been done elsewhere
    for (int i = 0; i < list.size(); i++) {
      ECSNode node = list.get(i);
      if (node.getNodeName().equals(nodeName)) {
        list.remove(i);
        return node;
      }
    }
    return null;
  }

  @Override
  public IECSNode addNode(String cacheStrategy, int cacheSize) {
    logger.info("Handling addNode");

    if (inactiveNodes.size() == 0) {
      logger.info("addNode failed: No more nodes available");
      return null;
    }

    IECSNode selectedNode = new ArrayList<>(setupNodes(1, cacheStrategy, cacheSize)).get(0);
    ECSNode node = removeAndReturn(inactiveNodes, selectedNode.getNodeName());
    activeNodes.add(node);

    // Sort the nodes that are going into the hash ring
    Collections.sort(activeNodes, ECSNode.hashValEndComp);
    activeNodes.get(0).setHashStart(
        activeNodes.get(activeNodes.size() - 1).getHashEnd()
            .add(BigInteger.ONE));
    for (int i = 1; i < activeNodes.size(); i++) {
      activeNodes.get(i).setHashStart(
          activeNodes.get(i - 1).getHashEnd().add(BigInteger.ONE));
    }

    logger.info("Starting: " + node.getNodeName() +  " on port: "
        + node.getNodePort());
    String serverName = node.getNodeName().split(" ")[0];
    sshStartServer(node.getNodeHost(), node.getNodePort(), cacheSize,
        cacheStrategy, serverName, zkHostname, zkPort);

    try {
      awaitNodes(1, TIMEOUT_VALUE);
    } catch (Exception e) {
      logger.info("awaitNodes exception in addNode.");
      e.printStackTrace();
    }

    return node;
  }

  @Override
  public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
    addingNodes = true;
    logger.info(String.format("Handling addNodes(%d, %d, %s)",
        count, cacheSize, cacheStrategy));



    if (count <= 0) {
      logger
          .info("please enter a valid amount of nodes to add (must be greater than 0)");
      return null;
    }
    if (count > inactiveNodes.size()) { // Limit the count to add
      count = inactiveNodes.size();
    }

    List<IECSNode> nodeList = new ArrayList<>(setupNodes(count, cacheStrategy, cacheSize));
    for (IECSNode node : nodeList) {
      ECSNode current = removeAndReturn(inactiveNodes, node.getNodeName());
      activeNodes.add(current);
    }

    // Sort the nodes that are going into the hash ring
    Collections.sort(activeNodes, ECSNode.hashValEndComp);
    activeNodes.get(0).setHashStart(
        activeNodes.get(activeNodes.size() - 1).getHashEnd()
            .add(BigInteger.ONE));
    for (int i = 1; i < activeNodes.size(); i++) {
      activeNodes.get(i).setHashStart(
          activeNodes.get(i - 1).getHashEnd().add(BigInteger.ONE));
    }

    for (IECSNode node : nodeList) {

      logger.info("Starting: " + node.getNodeName() + " on port: "
          + node.getNodePort());

      String serverName = node.getNodeName().split(" ")[0];
      sshStartServer(node.getNodeHost(), node.getNodePort(), cacheSize,
          cacheStrategy, serverName, zkHostname, zkPort);
    }

    try {

      boolean rc = awaitNodes(nodeList.size(), nodeList.size() * TIMEOUT_VALUE);

      addingNodes = false;

    } catch (Exception e) {
      e.printStackTrace();
    }

    return nodeList;
  }

  public List<ECSNode> getActiveNodes() {
    return activeNodes;
  }
  
  public List<ECSNode> getInactiveNodes() {
    return inactiveNodes;
  }

  public void setInactiveNodes(List<ECSNode> inactiveNodes) {
    this.inactiveNodes = inactiveNodes;
  }  

  private void setupMemberNodes() {
    try {
      String jsonMsg = gson.toJson(activeNodes);
      Stat fileStat = null;
      String fileNames[] = { "/members", "/members/stopped", "/members/started" };
      for (int i = 0; i < 3; i++) {
        fileStat = zk.exists(fileNames[i], false);
        if (fileStat == null) {
          zk.create(fileNames[i], jsonMsg.getBytes(),
              ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          if (i == 0) {
            TimeUnit.SECONDS.sleep(2);
          }
        }
      }
    } catch (KeeperException ke) {
      logger.info("KeeperException");
      ke.printStackTrace();
    } catch (InterruptedException ie) {
      logger.info("InterruptedException");
      ie.printStackTrace();
    }
  }

  /**
   * Sets up `count` servers with the ECS (in this case Zookeeper)
   * <p>
   * Note: Since this is just a hook for the TA autotester, we're just going to
   * return a fake list of which servers we actually plan to add to the service.
   * To me (Steven) it doesn't make sense to add the metadata to zookeeper until
   * the servers are actually added to the hash ring, which we will do in
   * addNodes(). We can change this after the release of the autotester if need
   * be.
   */
  @Override
  public Collection<IECSNode> setupNodes(int count, String cacheStrategy,
      int cacheSize) {
    int added = 0;
    List<IECSNode> nodeList = new ArrayList<>();
    List<ECSNode> inactiveCpy = new ArrayList<>(inactiveNodes);

    // @179
    if (count <= 0) {
      return null;
    }
    // sanitize count
    if (count > inactiveNodes.size()) {
      count = inactiveNodes.size();
    }

    // ALWAYS put the first server into the list many tests
    // rely on using this server to connect
    if (activeNodes.size() == 0) {
      ECSNode node = inactiveCpy.get(0);
      inactiveCpy.remove(0);
      nodeList.add(node);
      added++;
    }

    Random random = new Random();
    for (; added < count; added++) {
      int index = random.nextInt(inactiveCpy.size());
      nodeList.add(inactiveCpy.get(index));
      inactiveCpy.remove(index);
    }

    return nodeList;
  }

  @Override
  public boolean awaitNodes(int count, int timeout) throws Exception {
    long startTime = System.currentTimeMillis();
    long timeout_val = timeout + startTime;
    while (System.currentTimeMillis() < timeout_val) {
      List<String> children = zk.getChildren(STOPPED_DIR, null);
      if (children.size() == count) {
        System.out.println(PROMPT + Integer.toString(children.size())
            + " nodes successfully added!");
        return true;
      }
    }
    // timeout has failed. make active nodes equal to what is running
    int addedCount = 0;
    List<String> stoppedNodes = zk.getChildren(STOPPED_DIR, null);
    List<String> startedNodes = zk.getChildren(STARTED_DIR, null);
    List<String> liveNodes = new ArrayList<>();
    if (stoppedNodes != null && !liveNodes.isEmpty()) {
      addedCount = stoppedNodes.size();
      liveNodes.addAll(stoppedNodes);
    }
    if (startedNodes != null && !startedNodes.isEmpty()) {
      liveNodes.addAll(startedNodes);
    }

    List<ECSNode> runningNodes = new ArrayList<>();
    for (String name : liveNodes) {
      runningNodes.add(getECSNodeFromServerName(name));
    }

    inactiveNodes.addAll(activeNodes);
    activeNodes.addAll(runningNodes);

    System.out.println(PROMPT + "await nodes timeout! Was able to add "
        + Integer.toString(addedCount) + " out of "
        + Integer.toString(count) + " nodes!");
    return false;
  }

  @Override
  public boolean removeNodes(Collection<String> nodeNames) {
    logger.info("Handling removeNodes");

    boolean success = true;

    if (nodeNames == null || nodeNames.size() < 1) {
      return false;
    }

    for (String servername : nodeNames) {
      if (!handleRemoveNode(servername)) {
        success = false;
      }
    }

    return success;
  }

  @Override
  public Map<String, IECSNode> getNodes() {
    List<ECSNode> nodelist = hashRing.getServerNodes();
    Map<String, IECSNode> nodemap = new HashMap<String, IECSNode>();
    for (ECSNode node : nodelist) {
      nodemap.put(node.getNodeName(), node);
    }
    if (nodemap.isEmpty()) {
      return null;
    } else {
      return nodemap;
    }
  }

  @Override
  public IECSNode getNodeByKey(String key) {
    return hashRing.getResponsibleServer(key);
  }

  public void quit() {
    System.out.println("Goodbye!");
    System.exit(0);
  }

  public boolean isWantsToQuit() {
    return wantsToQuit;
  }

  public void kill(){
    try {
      ZKUtil.deleteRecursive(zk, "/members");
      ZKUtil.deleteRecursive(zk, "/tasks");
      ZKUtil.deleteRecursive(zk, "/acks");
      logger.info("deleting recursive directories - quit ECS");
    } catch (InterruptedException | KeeperException e) {
      logger.error("error in kill",e);
    } 
    
  }

  private boolean validateCacheStrategy(String cacheStrategy) {
    return cacheStrategy.equals("FIFO") || cacheStrategy.equals("LRU")
        || cacheStrategy.equals("LFU");
  }

  private void handleAddNodes(String[] args) {
    if (args.length != 3) {
      System.out.println(PROMPT + "Invalid command");
      return;
    }
    try {
      int numNodes = Integer.parseInt(args[0]);
      int cacheSize = Integer.parseInt(args[1]);
      String cacheStrategy = args[2];
      if (!validateCacheStrategy(cacheStrategy)) {
        System.out.println(PROMPT
            + "error: cacheStrategy must be one of <FIFO, LRU, LFU>");
        return;
      }
     
      addNodes(numNodes, cacheStrategy, cacheSize);
    
    } catch (NumberFormatException nfe) {
      logger.info("Error! Invalid arguments for addNodes!");
    }
  }

  private void handleAddNode(String[] args) {
    if (args.length != 2) {
      System.out.println("Invalid command");
      return;
    }
    try {
      int cacheSize = Integer.parseInt(args[0]);
      String cacheStrategy = args[1];
      if (!validateCacheStrategy(cacheStrategy)) {
        System.out.println(PROMPT
            + "cacheStrategy must be one of <FIFO, LRU, LFU>");
        return;
      }
     
      addNode(cacheStrategy, cacheSize);

    } catch (NumberFormatException nfe) {
      logger.info("Error! Invalid arguments for addNode()");
    }
  }

  private boolean performDataTransfer(ECSNode curr_node, ECSNode successor_node) {

    try {
      // 0 or only 1 server in the hash ring, no need to move data
      if (successor_node == null || successor_node == curr_node) {
        return true;
      }

      // WRITE_LOCK
      handleNodeTask(curr_node.getServerName(),
          KVAdminMsg.CommandType.WRITE_LOCK);

      // really big range to send over all the KV pairs
      String[] hashRange = { "0", "999999999999999999999999999999999999999999" };

      // MOVE_DATA
      KVAdminMsg cmd_move_data = new KVAdminMsg(
          KVAdminMsg.CommandType.MOVE_DATA, hashRange,
          successor_node.getNodeName()); // to the successor_node
      String msg_move_data = gson.toJson(cmd_move_data);

      // place this task for the current node
      zk.create("/tasks/" + curr_node.getServerName() + "/task",
          msg_move_data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL);

      // wait for acks before moving any further
      boolean success = false;
      long startTime = System.currentTimeMillis();
      List<String> children = null;
      while (System.currentTimeMillis() - startTime < 5000) {
        children = zk.getChildren("/acks", null);
        if (children != null && children.size() >= 1) {
          logger.info("performDataTransfer: Found: " + children);
          zk.delete("/acks/" + children.get(0), -1);
          success = true;
          break;
        }
      }

      if (!success) {
        logger.error("performDataTransfer: Acknowledgement timed out for: "
            + curr_node.getServerName());
      }

      // RELEASE_LOCK
      handleNodeTask(curr_node.getServerName(), CommandType.RELEASE_LOCK);
    } catch (Exception e) {
      logger.error("performDataTransfer: Exception: ", e);
      return false;
    }

    // TODO: Proper return values
    return true;
  }

  private boolean handleRemoveNode(String servername) {

    try {
      if(servername == null || getECSNodeFromServerName(servername) == null) {
        return false;
      }

      ECSNode curr_node = getECSNodeFromServerName(servername);
      ECSNode successor_node = getSuccessorNode(curr_node);

      // delete node from hash ring list
      activeNodes.remove(curr_node);

      // sort the list
      Collections.sort(activeNodes, ECSNode.hashValEndComp);
      activeNodes.get(0).setHashStart(
          activeNodes.get(activeNodes.size() - 1).getHashEnd()
              .add(BigInteger.ONE));
      for (int i = 1; i < activeNodes.size(); i++) {
        activeNodes.get(i).setHashStart(
            activeNodes.get(i - 1).getHashEnd().add(BigInteger.ONE));
      }

      performDataTransfer(curr_node, successor_node);

      // Shutdown a specific server
      handleNodeTask(servername, KVAdminMsg.CommandType.SHUTDOWN);
    } catch (Exception e) {
      logger.error("handleRemoveNode: Exception: ", e);
      return false;
    }

    return true;
  }

  private boolean handleRemoveNodes(String[] args) {
    boolean success = true;

    if (args == null || args.length < 1) {
      return false;
    }

    for(int i = 0 ; i < args.length ; i++) {
      if(!handleRemoveNode(args[i])) {
        System.out.println(PROMPT + "node has either crashed or is already deleted");

        success = false;
      }
    }
    return success;
  }

  public void handleUpdate(){
    if(!activeNodes.isEmpty()){
      logger.info("Handling update");
      for (ECSNode node : activeNodes) {
        try {
          String jsonMsg = gson.toJson(new KVAdminMsg(
              KVAdminMsg.CommandType.UPDATE, null, null));
          zk.create("/tasks/" + node.getServerName() + "/task",
              jsonMsg.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
          logger.error("error in update", e);
        }
      }
      System.out.println(PROMPT + "all nodes have been updated!");
    }
  }

  private void handleCommand(String cmdLine) {

    cmdLine = cmdLine.replaceAll("[\n\r\\s]", "");
    
    if(cmdLine.equals("")) {
      return;
    }
    
    String fnCallRegex = "([a-zA-Z0-9]+)\\(([,a-zA-Z0-9]*)\\)";
    Pattern funcPatter = Pattern.compile(fnCallRegex);
    Matcher m = funcPatter.matcher(cmdLine);
    if (!m.matches()) {
      System.out.println(PROMPT + "Invalid command");
      return;
    }

    String cmd = m.group(1);
    String[] args = m.group(2).split(",");
    switch (cmd) {
    case "addNodes":
      handleAddNodes(args);
      break;
    case "start":
      if (!args[0].equals("")) {
        System.out.println(PROMPT + "Invalid command!");
        break;
      }
      start();
      break;
    case "stop":
      if (!args[0].equals("")) {
        System.out.println(PROMPT + "Invalid command!");
        break;
      }
      stop();
      break;
    case "shutDown":
      if (!args[0].equals("")) {
        System.out.println(PROMPT + "Invalid command!");
        break;
      }
      shutdown();
      break;
    case "addNode":
      handleAddNode(args);
      break;
    case "removeNode":
      handleRemoveNodes(args);
      break;
    case "update":
      handleUpdate();
      break;
    case "clear":  
      System.out.print("\033[H\033[2J");  
      System.out.flush();
      break;
    case "quit":
      quit();
      break;
    case "clearZK":
      kill();
      break;
    default:
      System.out.println(PROMPT + "Invalid command!");
    }
  }

  private void runCLI() {
    isRunning = true;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

    while (isRunning) {
      System.out.print(PROMPT);
      try {
        String cmdLine = stdin.readLine();
        handleCommand(cmdLine);
      } catch (IOException ioe) {
        isRunning = false;
      }
    }
  }

  public static void main(String[] args) {

    
    if (args.length != 3) {
      System.out.println("Error! Invalid number of arguments");
      System.out.println("\tUsage: java -jar m2-ecs.jar <config_file.config> "+
          "<zk-Hostname> <zk-Port>");
      return;
    }
    try {
      new LogSetup("logs/ECS/ECS.log", Level.OFF);
    } catch (IOException e2) {
      e2.printStackTrace();
    }
    String config_file = args[0];
    String zkHostname = args[1];
    
    if (zkHostname.equals("localhost")) {
      try {
        zkHostname = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        System.out.println("sshStartServer: UnknownHostException");
      }
    }
    
    int zkPort = Integer.parseInt(args[2]);
    
    ECSClient app = new ECSClient(config_file, zkHostname, zkPort);

    // place a watcher under the started dir for the hash ring.
    Stat fileStat;
    try {
      fileStat = zk.exists(STARTED_DIR, false);
      if (fileStat != null) {

        MembersWatcher membersWatcher = 
            new MembersWatcher(app.hashRing, "ECS_CLIENT", zkHostname, zkPort);

        new Thread(membersWatcher).start();
      }
    } catch (KeeperException | InterruptedException e1) {

      e1.printStackTrace();
    }

    try {
      app.runCLI();
    } catch (Exception e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    } // catch (KeeperException | InterruptedException e) {
    //
    // e.printStackTrace();
  }
}
