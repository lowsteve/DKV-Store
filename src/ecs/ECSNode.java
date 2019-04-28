package ecs;

import java.math.BigInteger;
import java.util.Comparator;

public class ECSNode implements IECSNode {

  private String serverName;
  private String ip;
  private int port;
  private String hostname;
  private BigInteger hashStart;
  private BigInteger hashEnd;
  private String identifier;
  
  public ECSNode(
      String serverName,
      String ipAddr,
      int portNum,
      String hostName,
      BigInteger hashValStart,
      BigInteger hashValEnd)
  {
    this.serverName = serverName;
    this.ip = ipAddr;
    this.hostname = hostName;
    this.port = portNum;
    this.hashStart = hashValStart;
    this.hashEnd = hashValEnd;
    this.identifier = this.serverName + ":" + this.ip + ":" + Integer.toString(this.port);
  }
  
  public String getIdentifier() {
		return identifier;
	}

	public static Comparator<ECSNode> hashValEndComp = new Comparator<ECSNode>() {
    public int compare(ECSNode node1, ECSNode node2) {
      return node1.hashEnd.compareTo(node2.hashEnd);
    }
  };
  
  public String getNodeName() {
    //Format: "Server 8.8.8.8"
    return (serverName + " " + ip);
  }

  public String getNodeIP() {
    return ip;
  }

  public String getNodeHost() {
    //Format: "8.8.8.8" [hostname]
    return hostname;
  }

  public int getNodePort() {
    // Format: 8080
    return port;
  }

  public String[] getNodeHashRange() {
    // TODO: Check that the toString method gives us what we want.
    return (new String[]{hashStart.toString(), hashEnd.toString()});
  }

  public void setHashStart(BigInteger hashValS) {
    hashStart = hashValS;
  }

  public void setHashEnd(BigInteger hashValS) {
    hashEnd = hashValS;
  }

  public BigInteger getHashEnd() {
    return hashEnd;
  }

  public BigInteger getHashStart() {
    return hashStart;
  }
  
  public String getServerName() {
    return serverName;
  }
  

}
