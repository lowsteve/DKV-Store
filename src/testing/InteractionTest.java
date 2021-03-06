package testing;

import client.KVStore;
import common.KVMessage;
import common.KVMessage.StatusType;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class InteractionTest extends TestCase {

  private KVStore kvClient;

  @Before
  public void setUp() {
    kvClient = new KVStore("localhost", 21231);
    try {
      kvClient.connect();
    } catch (Exception e) {
    }
  }

  @After
  public void tearDown() {
    kvClient.disconnect();
  }


  @Test
  public void testPut() {
    String key = "foo2";
    String value = "bar2";
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.put(key, value);
    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
  }

  @Test
  public void testPutDisconnected() {
    kvClient.disconnect();
    String key = "foo";
    String value = "bar";
    Exception ex = null;

    try {
      KVMessage res = kvClient.put(key, value);
      System.out.println(res);
    } catch (Exception e) {
      ex = e;
    }

    assertNotNull(ex);
  }

  @Test
  public void testUpdate() {
    String key = "updateTestValue";
    String initialValue = "initial";
    String updatedValue = "updated";

    KVMessage response = null;
    Exception ex = null;

    try {
      kvClient.put(key, initialValue);
      response = kvClient.put(key, updatedValue);

    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
        && response.getValue().equals(updatedValue));
  }

  @Test
  public void testDelete() {
    String key = "deleteTestValue";
    String value = "toDelete";

    KVMessage response = null;
    Exception ex = null;

    try {
      kvClient.put(key, value);
      response = kvClient.put(key, "null");

    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
  }

  @Test
  public void testGet() {
    String key = "foo";
    String value = "bar";
    KVMessage response = null;
    Exception ex = null;

    try {
      kvClient.put(key, value);
      response = kvClient.get(key);
    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex == null && response.getValue().equals("bar"));
  }

  @Test
  public void testGetUnsetValue() {
    String key = "an unset value";
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.get(key);
      System.out.println(response.getKey() + " " + response.getStatus() + " " + response.getValue());
    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
  }


}
