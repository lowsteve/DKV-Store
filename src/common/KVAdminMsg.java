package common;

public class KVAdminMsg {
  private CommandType cmd;
  private String[] arg1;
  private String arg2;

  public enum CommandType {
    WRITE_LOCK,
    RELEASE_LOCK,
    MOVE_DATA,
    LISTEN_DATA,
    START,
    STOP,
    SHUTDOWN, 
    UPDATE
  }

  public KVAdminMsg(CommandType cmd, String[] arg1, String arg2) {
    this.cmd = cmd;
    this.arg1 = arg1;
    this.arg2 = arg2;
  }

  public CommandType getCmd() {
    return cmd;
  }

  public void setCmd(CommandType cmd) {
    this.cmd = cmd;
  }

  public String[] getArg1() {
    return arg1;
  }

  public void setArg1(String[] arg1) {
    this.arg1 = arg1;
  }

  public String getArg2() {
    return arg2;
  }

  public void setArg2(String arg2) {
    this.arg2 = arg2;
  }
}
