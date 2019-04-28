package CommandTask;

import app_kvServer.KVServer;
import common.KVAdminMsg;
import common.KVAdminMsg.CommandType;


public class CommandTask implements Runnable {
  private CommandType command = null;
  private KVServer kvs = null;
  private KVAdminMsg msg = null;

  public CommandTask(KVServer kvs, KVAdminMsg msg) {
    this.command = msg.getCmd();
    this.kvs = kvs;
    this.msg = msg;
  }

  public void run() {
    
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
  }

  public CommandType getCommand() {
    return command;
  }

  public void setCommand(CommandType command) {
    this.command = command;
  }

  public KVServer getKvs() {
    return kvs;
  }

  public void setKvs(KVServer kvs) {
    this.kvs = kvs;
  }

  public KVAdminMsg getMsg() {
    return msg;
  }

  public void setMsg(KVAdminMsg msg) {
    this.msg = msg;
  }
}
