package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import common.KVMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {

  private static Logger logger = Logger.getRootLogger();
  private static final String PROMPT = "Client> ";
  private KVStore comm = null;
  private BufferedReader stdin;
  private String serverAddress;
  private int serverPort;
  private boolean running;

  /**
   * establishes a new client server connection, by using the KVstore object
   * 
   * @param hostname
   *          the address of the server
   * @param port
   *          the port number of the server
   */
  @Override
  public void newConnection(String hostname, int port) throws Exception {
    try {
      // only create a connection if a connection currently does not exist
      if (comm == null || !comm.isConnected()) {
        comm = new KVStore(hostname, port);
        comm.connect();
      } else {
        printError("Connection already exists!");
        logger.info("new connection attempt made " + "on existing connection");
      }
    } catch (Exception e) {
      logger.error("ERROR: Connection unsuccessful", e);
      System.out.println("connection failed");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KVCommInterface getStore() {
    return comm;
  }

  /**
   * loops until quit command is established reads command line input and passes
   * to a handler (the handleCommand method)
   */
  public void run() {
    running = true;
    this.serverAddress = "ug221";
    this.serverPort = 21231;
    try {
      stdin = new BufferedReader(new InputStreamReader(System.in));
      while (running) {
        System.out.print(PROMPT);
  
          String cmdLine = stdin.readLine();
          this.handleCommand(cmdLine);
      }
    } catch (IOException e) {
      running = false;
      printError("CLI does not respond - Application terminated ");
    } finally {
      if(stdin != null) {
        try {
          stdin.close();
        } catch (Exception e) {
        }
      }
    }
    
    System.out.println("Goodbye!");
  }

  /**
   * grabs the command line string from the run while loop will handle the
   * command if it is a valid type, otherwise print error
   * 
   * @param cmdLine
   *          user input
   */
  private void handleCommand(String cmdLine) {
    // replace all newlines with empty string
    cmdLine.replaceAll("[\n\r]", "");
    String[] cmdTokens = null;
    cmdTokens = cmdLine.split(" ", 2);
    logger.info(cmdLine);

    switch (cmdTokens[0]) {
      case "disconnect":
        handleDisconnect();
        break;
      case "connect":
        handleConnect();
        break;
      case "connectTo":
        handleConnectTo(cmdTokens[1]);
        break;
      case "put":
        if (comm != null) {
          KVMessage msg = comm.handlePutGet(cmdTokens[1], "put");
          if(msg!=null){
            System.out.println("RESPONSE> " + msg.getStatus() + " "
                + msg.getKey() + " " + msg.getValue());
          }
        } else {
          printError("Initialize connection first");
        }
        break;
      case "get":
        if (comm != null) {
          KVMessage msg = comm.handlePutGet(cmdTokens[1], "get");
          if(msg!=null){
            System.out.println("RESPONSE> " + msg.getStatus() + " "
                + msg.getKey() + " " + msg.getValue());
          }
        } else {
          printError("before puts/gets, initialize connction first");
        }
        break;
      case "logLevel":
        handleLogLvl(cmdTokens[1]);
        break;
      case "help":
        printHelp();
        break;
      case "quit":
        handleDisconnect();
        running = false;
        break;
      case "clear":
        System.out.print("\033[H\033[2J");
        System.out.flush();
        break;
      default:
        printError("Unknown command");
        printHelp();
        break;
    }
  }

  private void handleConnect() {
  // parse the string that follows the command for a host name and port number
    try {        
      newConnection(this.serverAddress, this.serverPort);
    } catch (NumberFormatException nfe) {
      printError("No valid address. Port must be a number!");
      logger.info("Unable to parse argument <port>", nfe);
    } catch (UnknownHostException e) {
      printError("Unknown Host!");
      logger.info("Unknown Host!", e);
    } catch (IOException e) {
      printError("Could not establish connection!");
      logger.warn("Could not establish connection!", e);
    } catch (Exception e) {
      printError("general exception");
      logger.warn("general exception", e);
    }    
  }

  /**
   * processes a connect request from the user
   *
   * @param token
   *          the remaining string after the "connect" command
   */
  private void handleConnectTo(String token) {
    // parse the string that follows the command for a host name and port number

    String[] serverParams = token.split("\\s+");
    if (serverParams.length == 2) {
      try {
        serverAddress = serverParams[0];
        serverPort = Integer.parseInt(serverParams[1]);
        newConnection(serverAddress, serverPort);
      } catch (NumberFormatException nfe) {
        printError("No valid address. Port must be a number!");
        logger.info("Unable to parse argument <port>", nfe);
      } catch (UnknownHostException e) {
        printError("Unknown Host!");
        logger.info("Unknown Host!", e);
      } catch (IOException e) {
        printError("Could not establish connection!");
        logger.warn("Could not establish connection!", e);
      } catch (Exception e) {
        printError("general exception");
        logger.warn("general exception", e);
      }
    } else {
      printError("connect request has too many parameters == "
          + Integer.toString(serverParams.length));
      logger.info("connect request has too many parameters == "
          + Integer.toString(serverParams.length));
    }
  }

  /*
   * Beyond this point, code was taken from the example echo server
   * implementation
   */

  /**
   * changes the log level
   * 
   * @param token
   *          the string that remains from the user after the "log" command
   */
  private void handleLogLvl(String token) {
    String[] logLvlParams = token.split("\\s+");
    if (logLvlParams.length == 1) {
      String level = setLevel(logLvlParams[0]);
      if (level.equals("UnknownLevel")) {
        printError("No valid log level!");
        printPossibleLogLevels();
      } else {
        System.out.println(PROMPT + "Log level changed to level " + level);
      }
    } else {
      printError("logLevel request has too many parameters == "
          + Integer.toString(logLvlParams.length));
      logger.info("logLevel request has too many parameters == "
          + Integer.toString(logLvlParams.length));
    }
  }

  /**
   * calls the disconnect method of the KVStore class
   */
  private void handleDisconnect() {
    if (comm == null) {
      return;
    }
    
    if (comm.isConnected()) {
      comm.disconnect();
      if (!comm.isConnected()) {
        comm = null;
        System.out.println(PROMPT
            + "Application has now disconnected from Server!");
      } else {
        System.out.println(PROMPT + "client is still connected to server!");
      }
    }
  }

  /**
   * help messages
   */
  private void printHelp() {
    StringBuilder sb = new StringBuilder();

    sb.append(PROMPT).append("CLIENT HELP (Usage):\n");
    sb.append(PROMPT);
    sb.append("::::::::::::::::::::::::::::::::");
    sb.append("::::::::::::::::::::::::::::::::\n");

    sb.append(PROMPT).append("connect <host> <port>");
    sb.append("\t establishes a connection to a KVServer\n");

    sb.append(PROMPT).append("put <key> <value>");
    sb.append("\t\t sends a PUT request with the provided "
        + "<key> and <value> to the KVServer \n");

    sb.append(PROMPT).append("get <key>");
    sb.append("\t\t sends a GET request with the provided "
        + "<key> to the KVServer \n");

    sb.append(PROMPT).append("disconnect");
    sb.append("\t\t\t disconnects from the server \n");

    sb.append(PROMPT).append("logLevel");
    sb.append("\t\t\t changes the logLevel \n");
    sb.append(PROMPT).append("\t\t\t\t ");
    sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

    sb.append(PROMPT).append("quit ");
    sb.append("\t\t\t exits the program");
    System.out.println(sb.toString());
  }

  /**
   * possible log levels
   */
  private void printPossibleLogLevels() {
    System.out.println(PROMPT + "Possible log levels are:");
    System.out.println(PROMPT
        + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
  }

  /**
   * set the log level
   * 
   * @param levelString
   *          the level that the user has specified
   * @return the string representation of the requested level name
   */
  private String setLevel(String levelString) {

    if (levelString.equals(Level.ALL.toString())) {
      logger.setLevel(Level.ALL);
      return Level.ALL.toString();
    } else if (levelString.equals(Level.DEBUG.toString())) {
      logger.setLevel(Level.DEBUG);
      return Level.DEBUG.toString();
    } else if (levelString.equals(Level.INFO.toString())) {
      logger.setLevel(Level.INFO);
      return Level.INFO.toString();
    } else if (levelString.equals(Level.WARN.toString())) {
      logger.setLevel(Level.WARN);
      return Level.WARN.toString();
    } else if (levelString.equals(Level.ERROR.toString())) {
      logger.setLevel(Level.ERROR);
      return Level.ERROR.toString();
    } else if (levelString.equals(Level.FATAL.toString())) {
      logger.setLevel(Level.FATAL);
      return Level.FATAL.toString();
    } else if (levelString.equals(Level.OFF.toString())) {
      logger.setLevel(Level.OFF);
      return Level.OFF.toString();
    } else {
      return "UnknownLevel";
    }
  }

  /**
   * print errors to console (along with a prompt)
   * 
   * @param error
   *          the string to be printed to the console
   */
  private void printError(String error) {
    System.out.println(PROMPT + "Error! " + error);
  }

  /**
   * Main entry point for the echo server application.
   * 
   * @param args
   *          contains the port number at args[0].
   */
  public static void main(String[] args) {
    try {
    new LogSetup("logs/client.log", Level.OFF); 
    KVClient app = new KVClient();
    app.run();
    
   } catch (IOException e) {
     System.out.println("Error! Unable to initialize logger!");
     e.printStackTrace(); 
     System.exit(1);     
   }    
  }
}