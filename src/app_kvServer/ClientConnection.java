package app_kvServer;

import com.google.gson.Gson;

import common.HashRing;
import common.KVMessage;
import common.Message;
import ecs.ECSNode;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import logger.LogSetup;

/**
 * Represents a connection end point for a particular client that is connected
 * to the server. This class is responsible for message reception and sending.
 * This class is also responsible to communicate with the storage server for
 * put/get requests/replies.
 */
public class ClientConnection implements Runnable {

  private static Logger logger = Logger.getRootLogger();
  private Socket clientSocket;
  private KVServer kvserver;
  private boolean clientConnected;

  /**
   * Constructs a new CientConnection object for a given TCP socket.
   * 
   * @param clientSocket
   *          the Socket object for the client connection.
   * @param kvserver
   *          specifies the storage server object
   */
  public ClientConnection(Socket clientSocket, KVServer kvserver) {
    try {
      new LogSetup(
          "logs/clientConnection/" + kvserver.getServerName() + ".log",
          Level.INFO);
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.clientSocket = clientSocket;
    this.kvserver = kvserver;
    this.clientConnected = true;
  }

  /**
   * Determines the status type to return to the client for a PUT request
   * depending on if PUT request was successful or failed.
   * 
   * @param put_type
   *          can be one of: 0 for Delete, 1 for Update, 2 for Regular Put
   * @param error
   *          specifies whether or not the PUT request was successful or not
   * @returns status to return to the client application
   */
  private KVMessage.StatusType getStatus(int put_type, boolean error) {
    KVMessage.StatusType status = KVMessage.StatusType.PUT;
    if (error) {
      if (put_type == 0) { /* Delete */
        status = KVMessage.StatusType.DELETE_ERROR;
      } else if (put_type == 1 || put_type == 2) {
        /* Same error for Update or PUT */
        status = KVMessage.StatusType.PUT_ERROR;
      } else {
        logger.error("Invalid Put request detected");
      }
    } else {
      if (put_type == 0) { /* Delete */
        status = KVMessage.StatusType.DELETE_SUCCESS;
      } else if (put_type == 1) { /* Update */
        status = KVMessage.StatusType.PUT_UPDATE;
      } else if (put_type == 2) { /* Regular PUT */
        status = KVMessage.StatusType.PUT_SUCCESS;
      } else if (put_type == 3) {
        status = KVMessage.StatusType.SERVER_WRITE_LOCK;
      } else if (put_type == 4) {
        status = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
      } else {
        logger.error("Invalid Put request detected");
      }
    }
    return status;
  }

  /**
   * handles the Exception from a getKV call
   * 
   * @param reqMessage
   * @return
   */
  private KVMessage doGet(KVMessage reqMessage) {
    logger.debug("Trying to call server getKV");
    try {

      String valuestring = kvserver.getKV(reqMessage.getKey());
      return new Message(reqMessage.getKey(), valuestring,
          KVMessage.StatusType.GET_SUCCESS, null);
    } catch (Exception e) {
      return new Message(reqMessage.getKey(), "",
          KVMessage.StatusType.GET_ERROR, null);
    }
  }

  private KVMessage doPut(KVMessage reqMessage) {
    logger.debug("Trying to call server putKV");

    int put_type = -1;
    boolean error = false;

    String key = reqMessage.getKey();
    String value = reqMessage.getValue();

    try {
      /*
       * since putKV does not return anything, have to implement logic to
       * determine what the type of put operation is being performed
       */

      if (kvserver.getCurServerState() == KVServer.serverState.SERVER_WRITE_LOCK) {
        put_type = 3; // server write lock
      } else {
        if (kvserver.inStorage(key)) {
          // if in storage and value indicating delete/update
          if ((value == null) || (value.equals("null")) || (value.equals(""))) {
            put_type = 0; // put_delete
          } else {
            put_type = 1; // put_update
          }
        } else if (value == null || value.equals("null") || value.equals("")) {
          // if not in storage and value indicating delete
          put_type = 0; // put_delete
          throw new Exception("Trying to delete a key that isn't there");
        } else {
          // if not in storage and value is a regular put
          put_type = 2; // put_regular
        }

        kvserver.putKV(key, value);
      }
    } catch (Exception e) {
      error = true;
      logger.error("PUT request invalid");
    }

    KVMessage.StatusType status = getStatus(put_type, error);
    return new Message(key, value, status, null);
  }

  /**
   * Parse a json string into a KVMessage object.
   * 
   * @param json
   *          The JSON string to parse
   * @return The KVMessage representation of the string, or null if json is
   *         improperly formatted.
   */
  private KVMessage parseJSON(String json) {
    Gson gson = new Gson();
    try {
      return gson.fromJson(json, Message.class);
    } catch (Exception e) {
      logger.info("Illegal message format received from client");
      return null;
    }
  }

  /**
   * Initializes and starts the client connection. Loops until the connection is
   * closed or aborted by the client.
   */
  public void run() {
    try {
      BufferedReader input = new BufferedReader(new InputStreamReader(
          clientSocket.getInputStream()));
      PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
      logger.info("Client Connection running");
      while (clientConnected) {
        String client_msg = input.readLine();
        if(client_msg != null){
          logger.info("Client msg was: " + client_msg);
        }
        KVMessage resp_msg = null;

        // parse a KVMessage from JSON string
        // only consider doing a put or a get if the server is not stopped

        KVMessage req_msg = parseJSON(client_msg);
        if (req_msg != null) {
          HashRing hr = kvserver.getHashRing();
          ECSNode testNode = null;
          ECSNode respNode = null;

          // check for general server state errors (not resp, stopped)
          // write lock checking occurs in the doPut
          // if there are no general errors, proceed to the put/get wrappers

          testNode = hr.getNodeOfServerName(kvserver.getServerName(),
              kvserver.getPort());
          respNode = hr.getResponsibleServer(req_msg.getKey());

          if ((respNode != null && testNode != null)
              && (respNode.getIdentifier() != testNode.getIdentifier())) {
            logger.info("RUN() respNodes are not NULL");
            // server not responsible
            resp_msg = new Message(req_msg.getKey(), null,
                KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, kvserver
                    .getHashRing().getServerNodes());

            logger.info("IDENTIFIER COMPARISON " + respNode.getIdentifier()
                + " " + " " + testNode.getIdentifier());
            logger.info("REPLY> SERVER NOT RESPONSIBLE");

          } else if ((req_msg.getStatus().equals(KVMessage.StatusType.GET) || req_msg
              .getStatus().equals(KVMessage.StatusType.PUT))
              && (this.kvserver.getCurServerState()
                  .equals(KVServer.serverState.SERVER_STOPPED))) {

            // server stopped
            resp_msg = new Message(req_msg.getKey(), req_msg.getValue(),
                KVMessage.StatusType.SERVER_STOPPED, null);
            logger.debug("REPLY> SERVER STOPPED");

          } else {
            if (req_msg.getStatus().equals(KVMessage.StatusType.GET)) {
              resp_msg = doGet(req_msg);
            } else if (req_msg.getStatus().equals(KVMessage.StatusType.PUT)) {
              resp_msg = doPut(req_msg);
            } else {
              logger.info("Should never happen");
            }
          }
          Gson gson = new Gson();
          String json = gson.toJson(resp_msg);
          output.println(json);
          logger.info("ClientConnection sent response json back to client.");
          logger.info(json);
        } else {
          logger.debug("oh no! RUN() client message was recorded as null?");
        }
      }
    } catch (IOException ioe) {
      logger.info("Error! Connection lost!");
      ioe.printStackTrace();
      clientConnected = false;
    } catch (Exception e) {
      logger.info("RUN() Exception caught: " + e.getMessage(), e);
      clientConnected = false;
    } finally {
      try {
        if (clientSocket != null) {
          logger.info("Closing connection");
          clientSocket.close();
        }
      } catch (IOException ioe) {
        logger.info("Error! Unable to tear down connection!");
      }
    }
  }
}
