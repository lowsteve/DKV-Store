package client;

import com.google.gson.Gson;
import common.KVMessage;
import common.Message;
import ecs.ECSNode;
import ecs.IECSNode;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import logger.LogSetup;

public class KVStore implements KVCommInterface {
  /**
   * Initialize KVStore with address and port of KVServer
   * 
   * @param address
   *          the address of the KVServer
   * @param port
   *          the port of the KVServer
   */
  private Socket clientSocket;
  private String serverAddress;
  private int portNum;
  private BufferedReader input;
  private PrintWriter output;
  private Gson gson;
  private boolean running;
  private static Logger logger = Logger.getRootLogger();
  private boolean firstStart = true;
  static int retryCount = 0;
  List<ECSNode> masterMD;
  private static final String PROMPT = "Client > ";

  /**
   * {@inheritDoc}
   * 
   * constructor for KVStore - setup the server address, the port number, and
   * the gson parser
   * 
   * @param address
   *          the address of the server
   * @param port
   *          the server port that the client is supposed to connect to
   */
  public KVStore(String address, int port) {
    try {
      new LogSetup("logs/client.log", Level.INFO);
    } catch (IOException e) {
      e.printStackTrace();
    }
    serverAddress = address;
    portNum = port;
    gson = new Gson();
    running = false;
    logger.info("KVStore initiazed");
    firstStart = true;
  }

  /**
   * construct the PrinterWriter, buffered reader, and client socket connection
   * set the running boolean to true, meaning that a running connection is
   * active
   */
  @Override
  public void connect() throws Exception {
    try {
      logger.info("Connection attempt in progress");
      clientSocket = new Socket(serverAddress, portNum);
      output = new PrintWriter(clientSocket.getOutputStream(), true);
      input = new BufferedReader(new InputStreamReader(
          clientSocket.getInputStream()));
      if (clientSocket == null || output == null || input == null) {
        throw new Exception("Error establishing connection");
      }
      running = true;
      if (firstStart == true) {
        System.out.println("Connection established to KVStore service");
        firstStart = false;
      }
      logger.info("Connection established at " + serverAddress + ", "
          + Integer.toString(portNum));
    } catch (UnknownHostException uhe) {
      throw uhe;
    } catch (IOException io) {
      throw io;
    }
  }

  /**
   * disconnect the client from the server application
   */
  @Override
  public void disconnect() {

    try {
      logger.info("attempting disconnect");
      if ((clientSocket != null) && isConnected()) {

        if ((input != null) && (output != null)) {
          input.close();
          output.close();
        } else {
          logger.error("disconnect: streams are null");
        }

        clientSocket.close();
        clientSocket = null;
        running = false;
        logger.info("disconnect sucessful: socket and buffers closed");
      }
    } catch (IOException ioe) {
      running = true;
      logger.error("disconnect: unable to close connection!", ioe);
    }
  }

  /**
   * send a put request to the server
   * 
   * @param key
   *          the key to be sent to the server
   * @param val
   *          the value to be sent to the server
   * @return a KVmessage object containing the server response (key, value,
   *         statusType)
   */
  @Override
  public KVMessage put(String key, String val) throws Exception {
    if (key == null || key.length() == 0 || key.length() > 20
        || key.contains(" ")) {
      return new Message(key, val, KVMessage.StatusType.PUT_ERROR,null);
    }
    try {
      logger.debug("REQUEST> PUT " + key + "," + val);

      /*
       * key will be checked for errors in the CLI server handles key bounds
       * errors. CLI will parse out the proper value serialize key and value to
       * json with gson
       */

      KVMessage msg = new Message(key, val, KVMessage.StatusType.PUT, null);
      String jsonMsg = gson.toJson(msg);
      output.println(jsonMsg);
      return readMsg("put", key, val);
    } catch (Exception e) {
      throw e;
    }

  }

  /**
   * send a get request to the server
   * 
   * @param key
   *          the key to be sent to the server
   * @return a KVmessage object containing the server response (key, value,
   *         statusType)
   */
  @Override
  public KVMessage get(String key) throws Exception {
    if (key == null || key.length() == 0 || key.length() > 20
        || key.contains(" ")) {
      return new Message(key,null, KVMessage.StatusType.GET_ERROR,null);
    }
    try {
      logger.debug("REQUEST> GET " + key);

      /*
       * the key will be checked for errors in the CLI server handles key bounds
       * errors. serialize key and value to json with gson
       */
      KVMessage msg = new Message(key, null, KVMessage.StatusType.GET, null);
      String jsonMsg = gson.toJson(msg);
      output.println(jsonMsg);
      return readMsg("get", key, null);
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * read and return the incoming server gson input.
   * 
   * @param key
   * @param value
   * @return a Message (implements KVMessage) that contains the key, value,
   *         statusType, and metadata
   */
  public KVMessage readMsg(String op, String key, String val) throws Exception {
    // receive message as string
    KVMessage KVobj = null;

    try {
      String rcvMsg = input.readLine();
      logger.debug("read message from server: " + rcvMsg);
      KVobj = gson.fromJson(rcvMsg, Message.class);
      KVobj = processResponse(KVobj, op, key, val);
    } catch (IOException e) {
      logger.info("IOException at receiveMessageGet");
      disconnect();
      throw e;
    } catch (Exception e) {
      logger.error("exception in readMsg", e);
      throw e;
    }
    return KVobj;
  }

  /**
   * processes a get or a put response from the server
   * 
   * @param msg
   *          the returned message from the server
   * @param op
   *          (operation) specifies if the command is a get or a put
   */
  public KVMessage processResponse(KVMessage msg, String op, String key,
      String val) throws Exception {

    try {

      if (msg.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {

        logger.debug("[" + serverAddress + ":" + Integer.toString(portNum)
            + "] " + msg.getStatus().name() + ": " + msg.getKey() + " "
            + msg.getValue());

        /*
         * if the server is not responsible, we need to find the responsible
         * server, disconnect the current connection, connect to the new server,
         * and do the same put/get request
         */

        masterMD = msg.getMetadata();
        IECSNode serverNode = msg.getResponsibleServer();

        // disconnect from whatever you are currently connected to
        // and connect to the new server - after that try the put again

        disconnectAndConnect(serverNode.getNodeHost(), serverNode.getNodePort());
        logger.info("key + val" + key + " " + val);
        return handlePutGet(key + " " + val, op);
      } else if (msg.getStatus().equals(KVMessage.StatusType.SERVER_WRITE_LOCK)
          || msg.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED)) {

        logger.debug("[" + serverAddress + ":" + Integer.toString(portNum)
            + "] " + msg.getStatus().name() + ": " + msg.getKey() + " "
            + msg.getValue());

        if (retryCount < 5) {
          logger.warn("retry attempt " + Integer.toString(retryCount)
              + "on the following token: " + key + " " + val);

          // try to do the request again after a second         
          TimeUnit.SECONDS.sleep(1);
          retryCount++;
          return handlePutGet(key + " " + val, op);
        } else {
          retryCount = 0;
          logger.warn("retry timeout (>5s) on the following token: " + key
              + " " + val);
          System.out.println("RESPONSE> request timeout");
          return new Message(key, val, msg.getStatus(), null);
        }
      } else {

        logger.debug("RESPONSE> " + msg.getStatus() + " " + msg.getKey() + " "
            + msg.getValue());

        return msg;
      }
    } catch (Exception e) {
      logger.error("processResponse exception", e);
      throw e;
    }
  }

  /**
   * processes a get or a put request from the user
   * 
   * @param token
   *          the remaining string after the "get" or "put" command
   * @param op
   *          (operation) specifies if the command is a get or a put
   */
  public KVMessage handlePutGet(String token, String op) {
    if (isConnected()) {
      String val = "";
      String key = "";
      KVMessage retMsg = null;

      // find the key and the value. they are separated by a space
      String[] tokens = null;
      tokens = token.split(" ", 2);
      logger.info("token " + token);
      if (op != "put" && op != "get") {
        printError("Put/Get input not recognized");
        logger.info("op (" + op + ") is not recognized: " + op);
        return null;
      } else {

        // parse out the key, and parse out the value if op == put
        key = tokens[0].trim();
        if (op.equals("put") && tokens.length == 2) {
          val = tokens[1].trim();
        }
        try {
          if (key.length() > 0) {

            // find the respective server for this key and send to it
            checkChangeServer(key);

            /*
             * we wish to delete a char if there is no val (i.e whitespace or
             * empty string after the put)
             */

            if (val.length() == 0) {
              if (op.equals("put")) {
                retMsg = put(key, null);
              } else if (op.equals("get")) {
                retMsg = get(key);
              }
            } else {
              if (op.equals("put")) {
                retMsg = put(key, val);
              } else if (op.equals("get")) {
                printError("get accessed with too many args" + "\"" + key
                    + "\"" + " \"" + val + "\"");
                logger.info("get accessed with too many args" + "\"" + key
                    + "\"" + " \"" + val + "\"");
              }
            }

            return retMsg;

          } else {
            printError("key " + "\"" + key + "\"" + "is not a valid length");
            logger.info("key " + "\"" + key + "\"" + "is not a valid length");
            if (op.equals("get")) {
              return new Message(key, null, KVMessage.StatusType.GET_ERROR,
                  null);
            } else if (op.equals("put")) {
              return new Message(key, val, KVMessage.StatusType.PUT_ERROR, null);
            } else {
              return null;
            }
          }

        } catch (Exception e) {
          printError("server connection interrupted!");
          logger.error("exception thrown for get/put", e);
          if (op.equals("get")) {
            return new Message(key, null, KVMessage.StatusType.GET_ERROR, null);
          } else if (op.equals("put")) {
            return new Message(key, val, KVMessage.StatusType.PUT_ERROR, null);
          } else {
            return null;
          }
        }

      }
    } else {
      printError("before puts/gets, initialize connction first");
      if (op.equals("get")) {
        return new Message(null, null, KVMessage.StatusType.GET_ERROR, null);
      } else if (op.equals("put")) {
        return new Message(null, null, KVMessage.StatusType.PUT_ERROR, null);
      } else {
        return null;
      }
    }
  }

  public void checkChangeServer(String key) {
    Message tmp_msg = new Message(key, null, null, masterMD);

    // tmp_msg only useful to find the responsible server
    // if the current server is not responsible, use a new one

    if (masterMD != null) {
      IECSNode currNode = tmp_msg.getResponsibleServer();
      if (currNode.getNodePort() != portNum
          && currNode.getNodeName() != serverAddress) {
        logger.debug("Responsible Server: " + currNode.getNodeHost() + " "
            + currNode.getNodeName() + " " + currNode.getNodePort() + " "
            + currNode.getNodeHashRange() + " ");

        // disconnect from the active server, connect to a new one.
        disconnectAndConnect(currNode.getNodeHost(), currNode.getNodePort());
      }
    }
  }

  public void disconnectAndConnect(String address, int port) {
    if (isConnected()) {
      this.disconnect();
      serverAddress = address;
      portNum = port;
      firstStart = false;
      try {
        this.connect();
      } catch (Exception e) {
        logger.error("connectAndDisconnect falied to connect ", e);
      }
    } else {
      logger.info("could not close the connection");
    }
  }

  @Override
  public boolean isConnected() {
    return running;
  }

  private void printError(String error) {
    System.out.println(PROMPT + "Error! " + error);
  }
}