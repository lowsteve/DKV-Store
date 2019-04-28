package testing;

import common.HashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class HashRingTest extends TestCase {
  private HashRing hashRing;

  @Before
  public void setUp() {
    hashRing = new HashRing();
    List<String> hashRingMetadata = new ArrayList<>();
    try {
      String hostName = InetAddress.getLocalHost().getHostName();
      hashRingMetadata.add(hostName + ":21231:server1"); // zoo keeper goes here
      hashRingMetadata.add(hostName + ":21232:server2"); // too hard
      hashRingMetadata.add(hostName + ":21233:server3"); // key val goes here
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    hashRing.update(hashRingMetadata);
  }

  @Test
  public void testHashRingUpdateEndHashes() {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      for (int i = 1; i < 4; i++) {
        ECSNode node = hashRing.getNodeOfServerName("server" + i, 21230 + i);
        BigInteger actualHashEnd = node.getHashEnd();

        byte[] digestArr = md5.digest((node.getNodeIP()+":"+node.getNodePort()).getBytes());
        BigInteger expectedHashEnd = new BigInteger(1, digestArr);

        // Make sure we're getting what's expected.
        assertEquals(actualHashEnd.compareTo(expectedHashEnd), 0);
      }
    } catch (NoSuchAlgorithmException e){
      e.printStackTrace();
    }
  }

  @Test
  public void testHashRingUpdateStartHashes() {
    List<ECSNode> nodes = hashRing.getServerNodes();

    BigInteger server1StartHash = nodes.get(0).getHashStart();
    BigInteger server2StartHash = nodes.get(1).getHashStart();
    BigInteger server3StartHash = nodes.get(2).getHashStart();

    BigInteger server1EndHash = nodes.get(0).getHashEnd();
    BigInteger server2EndHash = nodes.get(1).getHashEnd();
    BigInteger server3EndHash = nodes.get(2).getHashEnd();

    int start1Comparison = server1StartHash.compareTo(server3EndHash.add(BigInteger.ONE));
    int start2Comparison = server2StartHash.compareTo(server1EndHash.add(BigInteger.ONE));
    int start3Comparison = server3StartHash.compareTo(server2EndHash.add(BigInteger.ONE));

    assertEquals(start1Comparison, 0);
    assertEquals(start2Comparison, 0);
    assertEquals(start3Comparison, 0);
  }

  @Test
  public void testHashRingGetResponsibleSerer() {
    // We expect the following key/value pairs to hash like this:
    // format: <key>  <val>  --> <server>
    //          zoo   keeper -->  server1
    //          too   hard   -->  server2
    //          help   me    -->  server3

    ECSNode expectServer1 = hashRing.getResponsibleServer("zoo");
    ECSNode expectServer2 = hashRing.getResponsibleServer("too");
    ECSNode expectServer3 = hashRing.getResponsibleServer("help");

    assert(expectServer1.getServerName().equals("server1"));
    assert(expectServer2.getServerName().equals("server2"));
    assert(expectServer3.getServerName().equals("server3"));
  }

  @Test
  public void testHashRingUpdateWithEmptyList() {
    List<String> emptyNodeList = new ArrayList<>();
    hashRing.update(emptyNodeList);
    assertNull(hashRing.getServerNodes());
  }
}
