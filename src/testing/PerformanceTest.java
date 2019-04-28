package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.UUID;

public class PerformanceTest extends TestCase {
  private KVStore kvClient;
  private boolean pass;
  private static final int KVnum = 50;
  private static final int numTests = 750;
  private static final int NumPutTests = 8;
  private static final int NumGetTests = 2;

  public void setUp() {
    kvClient = new KVStore("localhost", 21231);
    try {
      kvClient.connect();
    } catch (Exception e) {
    }
  }

  public void tearDown() {
    kvClient.disconnect();
  }

  public static String randomString(int count) {
    String createdString = UUID.randomUUID().toString();
    createdString.replace("-", "");
    return createdString.substring(0, count);
  }

  @Test
  public void testPutGet() {

    String[] key = new String[KVnum];
    String[] val = new String[KVnum];

    Exception ex = null;
    int rand1 = 0;
    int rand2 = 0;
    int rand3 = 0;

    for (int i = 0; i < KVnum; i++) {
      key[i] = randomString(10);
      val[i] = randomString(20);
    }

    for (int i = 0; i < numTests; i++) {
      try {
        for (int s = 0; s < NumPutTests; s++) {
          rand1 = (int) (Math.random() * KVnum);
          rand2 = (int) (Math.random() * KVnum);
          kvClient.put(key[rand1], val[rand2]);
        }
        for (int t = 0; t < NumGetTests; t++) {
          rand3 = (int) (Math.random() * KVnum);
          kvClient.get(key[rand3]);
        }
      } catch (Exception e) {
        ex = e;
      }

      if (ex == null) {
        pass = true;
      } else {
        pass = false;
      }
    }
    assertTrue(pass);
  }
}
