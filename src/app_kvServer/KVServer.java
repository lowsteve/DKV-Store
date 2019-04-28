package app_kvServer;

import CommandTask.CommandTask;
import cache.FIFOCache;
import cache.ICache;
import cache.LFUCache;
import cache.LRUCache;
import com.google.gson.Gson;
import common.HashRing;
import common.KVAdminMsg;
import common.KVObject;
import common.ZkAdmin;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import watchers.MembersWatcher;
import watchers.TaskWatcher;
import watchers.nodeWatcher;
import watchers.queueMessage;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class KVServer implements IKVServer, Runnable {


  public enum serverState {
    SERVER_STOPPED, SERVER_STARTED, SERVER_WRITE_LOCK
  }

  public static String persistentFilename = null;
  private static final String storageFileDir = "KVServer_Storage/";
 
  private static final String stopDirChild = "/members/stopped/";
  private static final String startDir = "/members/started";
  private static final String startDirChild = "/members/started/";
  private static final String tasksDirChild = "/tasks/";
  private static final String acksDirChild = "/acks/";
  
  public static ZooKeeper zk = null;
  public String zkHostname = null;
  public int zkPort = -1;
  public String zkServerCred = null;
  
  private int port;
  private int cacheSize;
  private CacheStrategy cacheStrategy;
  private ServerSocket serverSocket;
  private boolean running;
  private String serverName = null;
  private HashRing hashRing;

  public HashRing getHashRing() {
    return hashRing;
  }

  // need to synchronize writes to server state
  private final Object serverStateLock = new Object();
  private serverState curServerState = null;
  private serverState prevServerState = null;

  private ICache<String, String> cache;

  public static Gson gson = new Gson();
  private static Logger logger = Logger.getRootLogger();
  
  // lock for synchronizing file accesses
  private final Object dataAcessLock = new Object();

  public String getServerName() {
    return serverName;
  }

  public void setCurServerState(serverState curServerState) {
    this.curServerState = curServerState;
  }
  
  public void setPort(int port) {
    this.port = port;
  }

  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  public void setCacheStrategy(CacheStrategy cacheStrategy) {
    this.cacheStrategy = cacheStrategy;
  }  

    
  /**
   * Start KV Server at given port
   *
   * @param port      given port for storage server to operate
   * @param cacheSize specifies how many key-value pairs the server is allowed to keep
   *                  in-memory
   * @param strategy  specifies the cache replacement strategy in case the cache is full
   *                  and there is a GET- or PUT-request on a key that is currently not
   *                  contained in the cache. Options are "FIFO", "LRU", and "LFU".
   */

  
  
  public void handlePortAndCacheArgs(int port, int cacheSize, String strategy)
      throws IllegalArgumentException {

    if ((port > 0 && port <= 1024) || port >= 65536) {
      // throw new IllegalArgumentException(
      // "Error! <port> must be between 1024 and 65536.");
      logger.error("port number out of range!");
    } else {
      this.port = port;      
    }

    this.cacheSize = cacheSize;

    switch (strategy) {
    case "FIFO":
      this.cacheStrategy = CacheStrategy.FIFO;
      this.cache = new FIFOCache<>(cacheSize);
      break;
    case "LRU":
      this.cacheStrategy = CacheStrategy.LRU;
      this.cache = new LRUCache<>(cacheSize);
      break;
    case "LFU":
      this.cacheStrategy = CacheStrategy.LFU;
      this.cache = new LFUCache<>(cacheSize);
      break;
    default:
      throw new IllegalArgumentException("Error! <cache-strategy> must be one "
          + "of 'FIFO', 'LRU', 'LFU'");
    }
  }

  public KVServer(String name, String zkHostname, int zkPort) {
    this.serverName = name;
    this.hashRing = new HashRing();
    this.curServerState = serverState.SERVER_STOPPED;
    if(!createPersistentStorageDir()) {
      logger.error("ERROR: Couldn't create the persistent storage dir!");
    }
    this.zkPort = zkPort;
    this.zkHostname = zkHostname;
    zk = ZkAdmin.connectZookeeper(zkHostname,zkPort,logger);

  }

  private boolean createPersistentStorageDir() {
    File storageDir = new File(storageFileDir.replace("/", ""));
    if (!storageDir.exists()) {
      try {
        return storageDir.mkdir();
      } catch (SecurityException se) {
        return false;
      }
    } else {
      return true;
    }
  }

  /**
   * Start the server.
   */
  @Override
  public void run() {
    running = initServer();
    logger.info("server running state: " + running);
    logger.debug("Server listening on port: " + port);
    if (serverSocket != null) {
      try {

        // start the task dispatcher thread
        // initTaskQueue(this);
        
        //start a new task watcher (watcher is started in Main-Event Thread)
        new TaskWatcher(this, zk, tasksDirChild+serverName);
        
        // then on this thread, listen to KVClients
        while (isRunning()) {

          Socket client = serverSocket.accept();
          ClientConnection connection = new ClientConnection(client, this);
          new Thread(connection).start();

          logger.info("Connected to "
              + client.getInetAddress().getHostAddress() + " on port "
              + client.getPort());
        }

      } catch (SocketException se) {
        //Socket disconnected, just exit
      }
      catch (Exception e) {
        logger.error("initServer: Exception: ", e);
      }
    }
    logger.error("Server stopped.");
  }

  /**
   * Tries to open a server socket at the specified port
   *
   * @return true on success, false on failure.
   */
  private boolean initServer() {
    try {
      serverSocket = new ServerSocket(port);
      // gets the port in case port 0 is passed (0 is a special port)
      this.port = serverSocket.getLocalPort();
      if (serverName == null) {
        logger.error("Error! Server wasn't given a name");
        return false;
      }

      // place a watcher under the started dir
      Stat fileStat = zk.exists(startDir, false);
      if (fileStat != null) {

        MembersWatcher membersWatcher = 
            new MembersWatcher(hashRing, serverName, this.zkHostname, this.zkPort);

        new Thread(membersWatcher).start();
      }

      // place a node under the stopped dir
      fileStat = zk.exists(stopDirChild + serverName, false);
      if (fileStat == null) {
        zk.create(stopDirChild + serverName, "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      }

      // place a node under the tasks dir
      fileStat = zk.exists(tasksDirChild + serverName, false);
      if (fileStat == null) {
        zk.create(tasksDirChild + serverName, "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      this.curServerState = serverState.SERVER_STOPPED;
      this.prevServerState = this.curServerState;

      return true;
    } catch (IOException e) {
      logger.error("Error! Cannot open server socket.", e);
      if (e instanceof BindException) {
        logger.error("Port " + port + " is already bound!", e);
      }
      return false;
    } catch (InterruptedException ie) {
      logger.error("Error! ZooKeeper thread was interrupted!", ie);
      return false;
    } catch (KeeperException ke) {
      logger.error("Keeper Exception!", ke);
      return false;
    }
  }

  /**
   * Check if the server is running
   *
   * @return true if the server is running, false otherwise.
   */
  public boolean isRunning() {
    
    return this.running;
  }

  /**
   * Get the port number of the server
   *
   * @return port number
   */
  @Override
  public int getPort() {
    //logger.debug("In method KVServer.getPort()");
    return this.port;
  }

  /**
   * Get the server hostname.
   *
   * @return hostname of server
   */
  @Override
  public String getHostname() {
    //logger.deubg("In method KVServer.getHostname()");
    try {
      InetAddress ip;
      ip = InetAddress.getLocalHost();
      return ip.getHostName();
    } catch (UnknownHostException e) {
      logger.info("Error! Unknown host!");
    }
    return null;
  }

  /**
   * Get the cache strategy of the server
   *
   * @return cache strategy
   */
  @Override
  public CacheStrategy getCacheStrategy() {
    //logger.debug("In method KVServer.getCacheStrategy()");
    return this.cacheStrategy;
  }

  /**
   * Get the cache size
   *
   * @return cache size
   */
  @Override
  public int getCacheSize() {
    //logger.debug("In method KVServer.getCacheSize()");
    return this.cacheSize;
  }

  /**
   * Generates a hash from the key. Can be used in conjunction with gen_filename
   * to create a unique file for each key-value pair
   *
   * @param key used to generate unique file name by generating a hash value
   *            through a cryptographic hash function
   * @return return the unique cryptographic hash value
   */
  private String gen_hash(String key) {
    //logger.debug("In method KVServer.gen_hash()");
    return Integer.toString(key.hashCode());
  }

  /**
   * Generates the file name to use for the persistent storage file
   *
   * @param key is passed in to allow for future extensions where we can have 1
   *            file per key-value pair in order to improve performance.
   * @return the file name to use for persistent storage file
   */
  private String gen_filename(String key) {
    //logger.debug("In method KVServer.get_filename()");
    return persistentFilename;
  }

  /**
   * Check if key is in storage.
   *
   * @param key The key to check
   * @return true if key in storage, false otherwise.
   */
  @Override
  public boolean inStorage(String key) {
    //logger.debug("In method KVServer.inStorage()");

    if (key == null || key.length() == 0 || key.length() > 20
        || key.contains(" ")) {
      return false;
    }

    synchronized (dataAcessLock) {
      try {
        FileReader file = new FileReader(gen_filename(key));
        BufferedReader reader = new BufferedReader(file);

        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split(" ", 2);
          if (tokens[0].equals(key)) {
            reader.close();
            file.close();
            return true;
          }
        }
        reader.close();
        file.close();
      } catch (FileNotFoundException e) {

      } catch (IOException e) {

      } catch (Exception e) {

      }
    }

    return false;
  }

  /**
   * Check if key is in storage.
   *
   * @param key The key to check for.
   * @return true if key in storage, false otherwise
   */
  @Override
  public boolean inCache(String key) {
    //logger.info("In method KVServer.inCache()");
    if (key == null || key.length() == 0 || key.length() > 20
        || key.contains(" ")) {
      return false;
    }

    return this.cache.get(key) != null;

  }

  /**
   * {@inheritDoc}
   * <p>
   * Checks to see if key-value entry specified by <key> is in cache.
   * <p>
   * If kv pair in cache, value is returned from cache itself.
   * <p>
   * If kv pair is not found in cache, value is fetched from disk, inserted into
   * the cache and returned.
   *
   * @throws Exception to indicate failure to get kv pair from disk
   */
  @Override
  public String getKV(String key) throws Exception {
    //logger.debug("In method KVServer.getKV()");
    String valuestring = null;

    if (key == null || key.length() == 0 || key.length() > 20
        || key.contains(" ")) {
      throw new IllegalArgumentException("Key \"" + key + "\" invalid");
    }

    synchronized (dataAcessLock) {
      // if in cache, return that
      valuestring = this.cache.get(key);
      if (valuestring != null) {
        return valuestring;
      }

      try {
        FileReader file = new FileReader(gen_filename(key));
        BufferedReader reader = new BufferedReader(file);

        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split(" ", 2);
          if (tokens[0].equals(key)) {
            reader.close();
            file.close();

            // found on disk, insert in cache and return
            valuestring = line.substring(key.length() + 1);
            this.cache.put(key, valuestring);

            return valuestring;
          }
        }
        reader.close();
        file.close();

      } catch (FileNotFoundException e) {
        throw new NoSuchElementException("KV pair for key \"" + key
            + "\" was not found");
      } catch (IOException e) {
        logger.error("Buffered reader failed", e);
      } catch (Exception e) {
        logger.error("General Exception thrown", e);
      }
    }

    // key not found
    throw new NoSuchElementException("KV pair for key \"" + key
        + "\" was not found");
  }

  /**
   * {@inheritDoc}
   * <p>
   * Adds key-value entry to the cache in addition to adding the key-value pair
   * in the persistent storage, Cache is write-through.
   *
   * @throws Exception to indicate failure to insert kv pair to disk
   */
  @Override
  public void putKV(String key, String value) throws Exception {
    //logger.debug("In method KVServer.putKV()");
    if (null == key || key.length() > 20 || key.length() == 0
        || key.contains(" ")
        || (value != null && value.length() > (120 * 1024))) {
      throw new IllegalArgumentException("Illegal key and/or value encountered");
    }

    synchronized (dataAcessLock) {
      if (inStorage(key)) {
        try {
          if ((value == null) || (value.equals("null")) || (value.equals(""))) {
            // KV deletion
            this.cache.removeElement(key);
            deleteKV(key);
            return;
          }

          // KV Update
          this.cache.put(key, value);
          updateKV(key, value);
          return;
        } catch (Exception e) {
          throw e;
        }
      } else if ( // trying to delete something that is not in storage
          value == null || value.equals("null") || value.equals("")) {
        throw new Exception("Trying to delete kv pair that doesn't exist");
      }

      // Regular Put <key,value>
      try {
        FileWriter file = new FileWriter(gen_filename(key), true);

        // use space as delimiter (disallowed in key)
        String kv = key + " " + value + "\n";
        file.write(kv);

        this.cache.put(key, value);

        file.close();
      } catch (IOException ioe) {
        System.err.println("IOException: " + ioe.getMessage());
      }
    }
  }

  /**
   * deleteKV
   * <p>
   * Deletes a KV pair from the persistent storage. Achieves this by copying all
   * entries to a temporary file excluding the kv pair for <key>. Renames
   * temporary file to original when done
   *
   * @param key specifies the key to delete
   * @throws Exception if delete did not succeed or if there was any other error
   */
  private void deleteKV(String key) throws Exception {
   // logger.debug("In method KVServer.deleteKV()");
    boolean success = false;

    try {
      String filename = gen_filename(key);

      File in = new File(filename);
      File out = new File(filename + "_temp");

      BufferedReader reader = new BufferedReader(new FileReader(in));
      BufferedWriter writer = new BufferedWriter(new FileWriter(out));

      String line;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split(" ", 2);
        if (tokens[0].equals(key)) {
          continue;
        }
        writer.write(line + "\n");
      }

      writer.close();
      reader.close();

      success = out.renameTo(in);
    } catch (Exception e) {
      throw new Exception("Delete KV pair failed");
    }

    if (!success) {
      throw new Exception("Failed to delete KV pair for key \"" + key + "\"");
    }
  }

  /**
   * updateKV
   * <p>
   * Updates value of KV pair specified by <key>. Achieves this by copying all
   * entries to a temporary file excluding the kv pair for <key>. Renames
   * temporary file to original when done.
   *
   * @param key specifies the key to update
   * @throws Exception if update did not succeed or if there was any other error
   */
  private void updateKV(String key, String value) throws Exception {
    //logger.debug("In method KVServer.updateKV()");
    boolean success = false;

    try {
      String hash = gen_filename(key);

      File in = new File(hash);
      File out = new File(hash + "_temp");

      BufferedReader reader = new BufferedReader(new FileReader(in));
      BufferedWriter writer = new BufferedWriter(new FileWriter(out));

      String line;
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split(" ", 2);
        if (tokens[0].equals(key)) {
          // write new value
          writer.write(key + " " + value + "\n");
          continue;
        }
        writer.write(line + "\n");
      }

      writer.close();
      reader.close();

      success = out.renameTo(in);
    } catch (Exception e) {
      throw new Exception("KV update for key \"" + key + "\" and value \""
          + value + "\" failed");
    }

    if (!success) {
      throw new Exception("Failed to update KV pair for key \"" + key
          + "\" and value \"" + value + "\"");
    }
  }

  /**
   * Clear the local cache of the server
   */
  @Override
  public void clearCache() {
    
    this.cache.clearCache();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Cleans the cache and persistent storage of all kv entries
   */
  @Override
  public void clearStorage() {
    //logger.debug("In method KVServer.clearStorage()");
    clearCache();

    try {
      File folder = new File(".");

      File[] files = folder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(final File dir, final String name) {
          return name.matches(persistentFilename);
        }
      });

      for (final File file : files) {
        if (!file.delete()) {
          System.err.println("Failed to delete \"" + file.getAbsolutePath()
              + "\"");
        }
      }
    } catch (Exception e) {
      logger.error("Failed to clear the storage",e);
    }
  }

  /**
   * Abruptly stop the server without any additional actions
   */
  @Override
  public void kill() {
    //logger.debug("In method KVServer.kill()");
    running = false;
    try {
      if (serverSocket != null) {
        serverSocket.close();
      }

      logger.info("Killing Server...");
      ZKUtil.deleteRecursive(zk, tasksDirChild + this.serverName);
      System.exit(0);
    } catch (Exception e) {
      logger.error("Exception occurred when trying to kill Server",e);
    }

  }

  /**
   * Gracefully stop the server
   */
  @Override
  public void close() {
    // TODO: Add support for killing all the threads that were spawned
    logger.info("In method KVServer.close()");
    running = false;
    try {
      if(this.getCurServerState() != serverState.SERVER_STOPPED){
        stop();
      }
      if (cache != null)
        cache.clearCache();

      if (serverSocket != null)
        serverSocket.close();

      logger.info("Closing Server...");
      ZKUtil.deleteRecursive(zk, tasksDirChild + this.serverName);
      System.exit(0);
    } catch (Exception e) {
      logger.error("Exception occurred when trying to stop Server",e);
    }
  }

  /**
   * Set the loggers log level
   *
   * @param level The level to set. Valid args: 'ALL', 'DEBUG', 'INFO', 'WARN',
   *              'ERROR', 'FATAL', 'OFF'.
   */
  private static void setLogLevel(String level) {
    switch (level) {
      case "ALL":
        logger.setLevel(Level.ALL);
        break;
      case "DEBUG":
        logger.setLevel(Level.DEBUG);
        break;
      case "INFO":
        logger.setLevel(Level.INFO);
        break;
      case "WARN":
        logger.setLevel(Level.WARN);
        break;
      case "ERROR":
        logger.setLevel(Level.ERROR);
        break;
      case "FATAL":
        logger.setLevel(Level.FATAL);
        break;
      case "OFF":
        logger.setLevel(Level.OFF);
        break;
      default:
        logger.info("Error! Unknown log level. Proceeding with Level.INFO");
    }
  }

  public static void main(String[] args) {
    try {

      new LogSetup("logs/KVServer/" + args[4] + ".log", Level.DEBUG);

      if (args.length != 7) {

        System.out.println("Error! Invalid number of arguments!");
        System.out.println(
            "Usage: Server <port> <cache-size> <cache-strategy> "+
        "<server-name> <ZK-HOSTNAME> <ZK-PORT> <log-level>");
      } else {        
        new LogSetup("logs/KVServer/" + args[3] + ".log", Level.INFO);              
        setLogLevel(args[6]);
        
        logger.info("Server main");
        
        int port = Integer.parseInt(args[0]);
        int cacheSize = Integer.parseInt(args[1]);
        String cacheStrategy = args[2];
        String server_name = args[3];
        String zkHostname = args[4];
        int zkPort = Integer.parseInt(args[5]);
        
        
        
        KVServer server = new KVServer(server_name, zkHostname, zkPort);        
        server.handlePortAndCacheArgs(port, cacheSize, cacheStrategy);
        persistentFilename = storageFileDir + server.serverName;

        server.run();
      }
    } catch (NumberFormatException e) {
      logger.info("Error! Arguments <port> and <cache-size> must be integers.");
      logger.info("Usage: Server <port> <cache-size> <cache-strategy> <log-level>");
    } catch (IllegalArgumentException e) {
      logger.info(e.getMessage());
      logger.info("Usage: Server <port> <cache-size> <cache-strategy> <log-level>");
    } catch (IOException ioe) {
      logger.error("IOexception in main",ioe);
      System.out.println("Error! Unable to initialize logger!");
    } catch (Exception e) {
      logger.error("exception in main",e);
      e.printStackTrace();
      System.out.println("Unexpected exception");
    }
  }

  void initTaskQueue(KVServer kvs) {
    class taskQueue implements Runnable {

      KVServer kvs = null;

      taskQueue(KVServer kvserver) {
        this.kvs = kvserver;
      }

      public void run() {
        queueMessage headNode = null;
        nodeWatcher childWatcher = new nodeWatcher(tasksDirChild + serverName,
            zkHostname+":"+zkPort, "NODE_CHILDREN_CHANGED");
        Gson gson_tmp = new Gson();

        // list of all children after queue pop
        List<String> curSet = new ArrayList<String>();

        // list of all children before queue pop
        List<String> prevSet = new ArrayList<String>();

        while (true) {
          while (headNode == null) {
            headNode = childWatcher.getSharedQueue().poll();

            if (headNode != null) {
              logger.debug("HeadNode: " + headNode.getNodeDataList());
              // get new children from the node list, and take out all seen
              // children
              // (it is whatever new items have been added to the queue)

              curSet = headNode.getNodeDataList();
              if (!prevSet.isEmpty()) {
                curSet.removeAll(prevSet); // curSet = {curSet} - {prevSet}
              }

              prevSet.addAll(curSet); // prev = current
              String new_child = null;
              int i = curSet.size();
              try {
                while (i > 0) {
                  new_child = curSet.get(i-1); // get the newest
                  // node name
                  byte[] arr = zk.getData(tasksDirChild + serverName + "/"
                          + new_child, false,
                      zk.exists("/server1/" + new_child, false));
                  String msg = new String(arr, "UTF-8");
                  logger.info(msg);
                  KVAdminMsg kvMsg = gson_tmp.fromJson(msg, KVAdminMsg.class);
                  CommandTask cmd = new CommandTask(kvs, kvMsg);
                  new Thread(cmd).start();
                  i--;
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }

          logger.debug("Received Message is: " + headNode.getNodeType() + ", "
              + headNode.getZkPath() + ", " + headNode.getNodeDataList());

          // reset headNode to null so we can process more queueMsgs
          headNode = null;
        }
      }
    }

    Thread t = new Thread(new taskQueue(kvs));
    t.start();
  }

  @Override
  public void start() {
    logger.info("run start");
    synchronized (serverStateLock) {
      if (this.curServerState.equals(serverState.SERVER_STOPPED)) {
        this.prevServerState = this.curServerState;
        this.curServerState = serverState.SERVER_STARTED;
      }
    }
    try {
      // delete the znode under the stopped dir, then add to the started dir
      String memberZNodeData = this.getHostname() + ":" + this.port + ":" + this.serverName;

      Stat fileStat = zk.exists(startDirChild + this.serverName, false);
      if (fileStat == null) {
        zk.create(startDirChild + this.serverName, memberZNodeData.getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      }
      
      fileStat = zk.exists(stopDirChild + this.serverName, false);
      if (fileStat != null) {
        System.out.println(fileStat.toString());
        // -1 matches any version to delete
        zk.delete(stopDirChild + this.serverName, -1);
      }

      
    } catch (KeeperException | InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void stop() {
    logger.info("run stop");
    synchronized (serverStateLock) {
      if (!this.curServerState.equals(serverState.SERVER_STOPPED)) {
        this.prevServerState = this.curServerState;
        this.curServerState = serverState.SERVER_STOPPED;
      }
    }
    try {
      
      Stat fileStat;
      
      fileStat = zk.exists(stopDirChild + this.serverName, false);
      if (fileStat == null) {
        zk.create(stopDirChild + this.serverName, "".getBytes(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      }
      
      fileStat = zk.exists(startDirChild + this.serverName, false);
      if (fileStat != null) {
        zk.delete(startDirChild + serverName, -1);
      }

    } catch (KeeperException | InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void lockWrite() {
    logger.info("lock write");
    synchronized (serverStateLock) {
      if (!this.curServerState.equals(serverState.SERVER_WRITE_LOCK)) {
        this.prevServerState = this.curServerState;
        this.curServerState = serverState.SERVER_WRITE_LOCK;
      }
    }
  }

  @Override
  public void unlockWrite() {
    logger.info("run unlock write");
    synchronized (serverStateLock) {
      if (this.curServerState.equals(serverState.SERVER_WRITE_LOCK)) {
        this.curServerState = this.prevServerState;
        this.prevServerState = serverState.SERVER_WRITE_LOCK;
      }      
    }
  }
  
  public void update() {
    try {
      List<String> memberNames = zk.getChildren(startDir, false);
      List<String> memberZData = new ArrayList<>();
      logger.info("in update()");
      if (memberNames != null) {
        for (String member : memberNames) {
          byte[] memberData = zk.getData(startDirChild + member, null, null);
          memberZData.add(new String(memberData));
        }
        logger.info("hash ring update " + memberZData);
        this.hashRing.update(memberZData);
      }
    } catch (KeeperException | InterruptedException e) {
      logger.error("error in update", e);
    }
  }

  public serverState getCurServerState() {
    return curServerState;
  }

  private boolean isKeyInRange(BigInteger key_hash, BigInteger start_hash,
                               BigInteger end_hash) {

    // BigInteger Comparison
    // -1 : Less than
    // 0 : Equal
    // +1 : Greater than
    boolean retval = false;

    int wrap_around = start_hash.compareTo(end_hash);

    if (wrap_around == 0) {
      System.out.println("ERR: This should never happen!");
      return false; // TODO: Check if correct
    }

    // start_hash > end_hash
    if (wrap_around > 0) {
      if (key_hash.compareTo(start_hash) > 0) {
        retval = true;
      } else if (key_hash.compareTo(end_hash) <= 0) {
        retval = true;
      } else {
        // not in range; i.e. end_hash < key_hash < start_hash
        retval = false;
      }
    } else {
      // start_hash < key_hash <= end_hash
      if ((key_hash.compareTo(start_hash) > 0)
          && (key_hash.compareTo(end_hash) <= 0)) {
        retval = true;
      } else {
        retval = false;
      }
    }
    
    return retval;
  }

  // receiving end of KVS
  public void listenData(String serverAddress, String port) {
    
    Socket sock = null;
    BufferedReader input = null;
    FileWriter writer = null;

//    synchronized(dataAcessLock) {
      try {
        logger.info("listenData: Sending Server Credentials: " + serverAddress + ", " + port);
        
        int portNum = Integer.parseInt(port);
                
//        writer = new FileWriter(persistentFilename, true);
        sock = new Socket(serverAddress, portNum);
        input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
  
        // keep receiving until connection is closed
        while (true) {
          String json_msg = input.readLine();
          
          List<String> list_of_kv_pairs = gson.fromJson(json_msg, List.class);
          if(list_of_kv_pairs != null && !list_of_kv_pairs.isEmpty()){
            for(int i = 0; i < list_of_kv_pairs.size() ; i++) {
              String kvpair = list_of_kv_pairs.get(i);
              String[] tokens = kvpair.split(" ", 2);
  //            writer.write(kvpair+"\n");
              putKV(tokens[0], tokens[1]);
              logger.info("listenData: Writing kvpair: " + kvpair);
            }
          }
        }
  
      } catch (Exception e) {
        e.printStackTrace();  //exception is expected
        logger.error("listenData: Exception: ", e);
      } finally {
        if (sock != null) {
          try {
            sock.close();
          } catch (Exception e) {
          }
        }
  
        if (input != null) {
          try {
            input.close();
          } catch (Exception e) {
          }
        }
        
        if(writer != null) {
          try {
            writer.close();
          } catch (Exception e) {
          }
        }
      }
//    }
  }

  // sending end of KVS
  private boolean sendKVPairsToServer(List<KVObject> list, String targetName) {

    try {
      ServerSocket listen_sock = new ServerSocket(0);
      int listen_port = listen_sock.getLocalPort();

      logger.info("sendKVPairsToServer: Listening for connections on: " + listen_port);
      logger.info("sendKVPairsToServer: targetName is: " + targetName);
      
      //format is "server1 8.8.8.8"
      String[] tokens = targetName.split(" ", 2);
      String kvs_znode_name = tokens[0];
      
      Stat zkStat = zk.exists(tasksDirChild+kvs_znode_name, null);
      if (zkStat == null) {
        // Receiving Server ZNODE should already exist
        logger.error("sendKVPairsToServer: ERR: " + kvs_znode_name + " ZNODE does not exist...");
        listen_sock.close();
        return false;
      }

      // indicate our own IP address to the other server with the port that we are listening to
      String myIpAddr = Inet4Address.getLocalHost().getHostAddress();
      String[] arg1 = { myIpAddr };
      String arg2 = Integer.toString(listen_port);
      KVAdminMsg cmd = new KVAdminMsg(KVAdminMsg.CommandType.LISTEN_DATA, arg1, arg2);
      String msg = gson.toJson(cmd);

      // create new task ZNODE for the receiving server
      zk.create(tasksDirChild + kvs_znode_name + "/task", msg.getBytes(),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

      Socket kvs_sock = listen_sock.accept();

      BufferedReader input = new BufferedReader(new InputStreamReader(
          kvs_sock.getInputStream()));

      PrintWriter output = new PrintWriter(kvs_sock.getOutputStream(), true);

      List<String> kv_pairs = new ArrayList<String>();
      
      //construct a list of the KV pairs and just send the list
      for(int i = 0 ; i < list.size() ; i++) {
        KVObject obj = list.get(i);
        String kv_line = obj.getKey() + " " + obj.getValue();
        kv_pairs.add(kv_line);
      }
      
      //convert to string and send
      String json_list = gson.toJson(kv_pairs);
      output.println(json_list);

      listen_sock.close();
      kvs_sock.close();
      input.close();
      output.close();
    } catch (Exception e) {
      logger.error("sendKVPairsToServer: Exception: ", e);
    }

    return true;
  }

  private List<KVObject> readKVObjectsFromFile() {
    List<KVObject> kv_entries = null;

    FileReader inFile = null;
    File in = null;
    BufferedReader reader = null;

    try {
      in = new File(persistentFilename);
      inFile = new FileReader(in);
      reader = new BufferedReader(inFile);

      kv_entries = new ArrayList<KVObject>();

      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] split = line.trim().split("\\s", 2);
        String key = split[0];
        String val = split[1];

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] msgdigest = md5.digest(key.getBytes());

        BigInteger bigint = new BigInteger(1, msgdigest);

        KVObject kvo = new KVObject(key, val, bigint);

        kv_entries.add(kvo);

        // Debugging
        // String hash = bigint.toString(10);
        // System.out.println(hash + ": " + key);
      }

      // sort kv pairs based on hash of key
      Collections.sort(kv_entries, KVObject.hashComp);

    } catch (FileNotFoundException fnfe) {
      // file does not exist
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (inFile != null) {
        try {
          inFile.close();
        } catch (Exception e) {
          System.out.println("ERR: inFile close failed");
        }
      }

      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          System.out.println("ERR: Reader close failed");
        }
      }
    }

    return kv_entries;
  }

  private void writeKVObjectsToFileAndRename(List<KVObject> responsible_list, 
      List<KVObject> not_responsible_list)
  {
    try {
      if(responsible_list != null) {
        for(int i = 0 ; i < responsible_list.size() ; i++) {
          String key = responsible_list.get(i).getKey();
          String val = responsible_list.get(i).getValue();
          
          putKV(key, val);
        }
      }
     
      //delete non responsible from file
      if(not_responsible_list != null) {
        for(int i = 0 ; i < not_responsible_list.size() ; i++) {
          String key = not_responsible_list.get(i).getKey();
          
          putKV(key, "");
        }
      }
    } catch (Exception e) {
      logger.error("writeKVObjectToFileAndRename: Exception: ", e);
    }
  }
  
  private void ack_moveData() {
    //ACK moveData request by creating child on /moveDataAck
    try {
      zk.create(acksDirChild + serverName, serverName.getBytes(),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (Exception e) {
      logger.error("moveData: Exception creating the znodes: ", e);
    }
  }

  public boolean moveData(String[] hashRange, String targetName)
      throws Exception {
    List<KVObject> kv_not_responsible = null;
    List<KVObject> kv_responsible = null;
     
    if (hashRange == null || hashRange.length != 2) {
      throw new IllegalArgumentException("Invalid Parameters");
    }
    
    synchronized (dataAcessLock) {
      try {
        kv_responsible = new ArrayList<KVObject>();
        kv_not_responsible = new ArrayList<KVObject>();
        List<KVObject> kv_entries = readKVObjectsFromFile();

        // nothing to move
        if (kv_entries == null) {
          ack_moveData();
          return true;
        }

        BigInteger start_hash = new BigInteger(hashRange[0]);
        BigInteger end_hash = new BigInteger(hashRange[1]);

        // Separate responsible/not responsible kV Pairs
        for (int i = 0; i < kv_entries.size(); i++) {
          KVObject entry = kv_entries.get(i);
          BigInteger key_hash = entry.getHash();
          
          boolean inRange = isKeyInRange(key_hash, start_hash, end_hash);
          
          //if in range, we are not responsible for it
          if (!inRange) {
            kv_responsible.add(entry);
//            logger.info("\nS: " + start_hash.toString(10) + 
//                "\nY: " + entry.getHash().toString(10) + ": " + entry.getKey() +
//                "\nE: " + end_hash.toString(10));
          } else {
            kv_not_responsible.add(entry);
//            logger.info("\nS: " + start_hash.toString(10) + 
//                "\nN: " + entry.getHash().toString(10) + ": " + entry.getKey() +
//                "\nE: " + end_hash.toString(10));
          }
        }
        
        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // print out the keys that each server is responsible for..
//        for (int i = 0; i < kv_not_responsible.size(); i++) {
//          KVObject obj = kv_not_responsible.get(i);
//          // System.out.println("NR: " + obj.getKey() + " " + obj.getValue());
//          logger.info("NR: " + obj.getHash().toString(10) + ": " + obj.getKey());
//        }
//
//        for (int i = 0; i < kv_responsible.size(); i++) {
//          KVObject obj = kv_responsible.get(i);
//          // System.out.println("R: " + obj.getKey() + " " + obj.getValue());
//          logger.info("R: " + obj.getHash().toString(10) + ": " + obj.getKey());
//        }
        //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      } catch (Exception e) {
        logger.error("moveData: Exception! ", e);
      }
    }

    // send not responsible outside of file lock to allow for concurrent gets
    if (kv_not_responsible != null) {
      if (sendKVPairsToServer(kv_not_responsible, targetName) != true) {
        logger.error("moveData: send kv pairs to server failed");
        return false;
      }
    }
    
    // write responsible to persistent storage
    writeKVObjectsToFileAndRename(kv_responsible, kv_not_responsible);
    
    cache.clearCache();

    ack_moveData();
    
    return true;
  }

}
