package watchers;

import java.util.ArrayList;
import java.util.List;

public class queueMessage {
  private String nodeType;
  private List<String> nodeDataList;
  private String zkPath;


  public queueMessage(String nodeType, List<String> nodeDataList,
                      String zkPath) {

    this.nodeType = nodeType;
    this.nodeDataList = nodeDataList;
    this.zkPath = zkPath;
  }

  public queueMessage(String nodeType, String nodeData,
                      String zkPath) {
    this.nodeDataList = new ArrayList<String>();
    this.nodeType = nodeType;
    this.nodeDataList.add(nodeData);
    this.zkPath = zkPath;
  }

  public String getNodeType() {
    return nodeType;
  }

  public void setNodeType(String nodeType) {
    this.nodeType = nodeType;
  }

  public List<String> getNodeDataList() {
    return nodeDataList;
  }

  public void setNodeDataList(List<String> nodeDataList) {
    this.nodeDataList = nodeDataList;
  }

  public String getZkPath() {
    return zkPath;
  }

  public void setZkPath(String zkPath) {
    this.zkPath = zkPath;
  }


}
