package testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import cache.FIFOCache;
import cache.LFUCache;
import cache.LRUCache;
import client.KVStore;
import common.KVMessage;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdditionalTest extends TestCase {

  private KVStore kvstore;
  Exception ex = null;

  @Before
  public void setUp() {
    kvstore = new KVStore("localhost", 21231);
    try {
      kvstore.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {
    kvstore.disconnect();
  }

  @Test
  public void testStub() {
    assertTrue(true);
  }

  @Test
  public void testLRUCache() {
    LRUCache<Integer, Integer> cache = new LRUCache<>(2);

    cache.put(1, 1);
    cache.put(2, 2);
    assertEquals(cache.get(1), (Integer) 1);

    cache.put(3, 3);  // Call should evict key 2.
    assertNull(cache.get(2));

    cache.put(4, 4);  // Call should evict key 1.
    assertNull(cache.get(1));

    assertEquals(cache.get(3), (Integer) 3);
    assertEquals(cache.get(4), (Integer) 4);
  }

  @Test
  public void testFIFOCache() {
    FIFOCache<Integer, Integer> cache = new FIFOCache<>(2);

    cache.put(1, 1);
    cache.put(2, 2);
    assertEquals(cache.get(1), (Integer) 1);

    cache.put(3, 3);  // Call should evict key 1.
    assertNull(cache.get(1));

    cache.put(4, 4);  // Call should evict key 2.
    assertNull(cache.get(2));

    assertEquals(cache.get(3), (Integer) 3);
    assertEquals(cache.get(4), (Integer) 4);
  }

  @Test
  public void testLFUCache() {
    LFUCache<Integer, Integer> cache = new LFUCache<>(2);

    cache.put(1, 1);
    cache.put(2, 2);
    assertEquals(cache.get(1), (Integer) 1);

    cache.put(3, 3);  // Call should evict key 2.
    assertNull(cache.get(2));
    assertEquals(cache.get(3), (Integer) 3);

    cache.put(4, 4);  // Call should evict key 1.
    assertNull(cache.get(1));

    assertEquals(cache.get(3), (Integer) 3);
    assertEquals(cache.get(4), (Integer) 4);
  }

  @Test
  public void testManyClients() {
    Exception ex = null;
    Random rand = new Random();
    int n;
    List<KVStore> clientList = new ArrayList<KVStore>();
    for (int i = 0; i < 100; i++) {
      clientList.add(new KVStore("localhost", 21231));
      try {
        clientList.get(i).connect();
        n = rand.nextInt(100);
        clientList.get(i).put(((Integer) n).toString(), ((Integer) n).toString());
      } catch (Exception e) {
        ex = e;
        break;
      }
    }
    for (int i = 0; i < 100; i++) {
      try {
        clientList.get(i).get(String.valueOf(i));
      } catch (Exception e) {
        ex = e;
        break;
     }
   }
   assertNull(ex);
  }


  @Test
  public void testInvalidGET() {
    String key0 = "bluh";
    String key1 = "blah";
    String key2 = "blabla";
    String key3 = "haha";
    String key4 = "okaythen";

    /*GET request for something that doesn't exist*/
    try {
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key0).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key1).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key2).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key3).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key4).getStatus());
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    ex = null;
  }


  @Test
  public void testInvalidDelete() {
    String key0 = "whatdidyousay";
    String key1 = "alright";
    String key2 = "fine";
    String key3 = "dontworry";
    String key4 = "behappy";

    /*DELETE something that doesn't exist - various types of deletion*/
    try {
      assertEquals(KVMessage.StatusType.DELETE_ERROR,
          kvstore.put(key0, "").getStatus());
      assertEquals(KVMessage.StatusType.DELETE_ERROR,
          kvstore.put(key1, "null").getStatus());
      assertEquals(KVMessage.StatusType.DELETE_ERROR,
          kvstore.put(key2, null).getStatus());

           
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    ex = null;
  }

  @Test
  public void testInvalidKeys() {
    String invalid_key0 = "thisislongerthan20characters";
    String invalid_key1 = "this has spaces";
    String invalid_key2 = "";
    String invalid_key3 = null;

    String val0 = "this is a value";
    String val1 = "this is also a value";
    String val2 = "roger that";
    String val3 = "whatupson";

    /*Invalid Keys, valid value*/
    try {
     assertEquals(KVMessage.StatusType.PUT_ERROR,
          kvstore.put(invalid_key0, val0).getStatus());
      assertEquals(KVMessage.StatusType.PUT_ERROR,
          kvstore.put(invalid_key1, val1).getStatus());
      assertEquals(KVMessage.StatusType.PUT_ERROR,
          kvstore.put(invalid_key2, val2).getStatus());
      assertEquals(KVMessage.StatusType.PUT_ERROR,
          kvstore.put(invalid_key3, val3).getStatus());
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    ex = null;
  }

  @Test
  public void testLargestValueAllowed() {
    String key0 = "akey";

    int length = 120 * 1024;
    StringBuffer buf = new StringBuffer(length);
    for (int i = 0; i < length; i++) {
      buf.append("Z");
    }

    String longest_value = buf.toString();

    //Largest value allowed
    try {
      assertEquals(KVMessage.StatusType.PUT_SUCCESS,
          kvstore.put(key0, longest_value).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          kvstore.get(key0).getStatus());
      assertEquals(KVMessage.StatusType.DELETE_SUCCESS,
          kvstore.put(key0, null).getStatus());
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    ex = null;
  }

  @Test
  public void testLargerThanLongestValueAllowed() {
    String key0 = "randomkey";

    int length = (120 * 1024) + 1;
    StringBuffer buf = new StringBuffer(length);
    for (int i = 0; i < length + 1; i++) {
      buf.append("Z");
    }
    String larger_than_longest_value = buf.toString();


    //One character more than the largest value allowed
    try {
      assertEquals(KVMessage.StatusType.PUT_ERROR,
          kvstore.put(key0, larger_than_longest_value).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key0).getStatus());
      assertEquals(KVMessage.StatusType.DELETE_ERROR,
          kvstore.put(key0, null).getStatus());
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    ex = null;
  }

  @Test
  public void testPutGet_valid() {
    String key0 = "mykey0";
    String key1 = "mykey1";
    String key2 = "mykey2";
    String key3 = "mykey3";
    String key4 = "mykey4";

    String val0 = "myvaluea";
    String val1 = "myvalueb";
    String val2 = "myvaluec";
    String val3 = "myvalued";
    String val4 = "myvaluee";

    String uval0 = "myvaluea" + " okay then";
    String uval1 = "myvalueb" + " okay then";
    String uval2 = "myvaluec" + " okay then";
    String uval3 = "myvalued" + " okay then";
    String uval4 = "myvaluee" + " okay then";

    /* insert kv pair
     * check retrieval status
     * check retrieval value
     * update inserted value
     * check updated value
     * delete kv pair
     * check retrieval status*/
    try {
      //insertion
      assertEquals(KVMessage.StatusType.PUT_SUCCESS,
          kvstore.put(key0, val0).getStatus());
      assertEquals(KVMessage.StatusType.PUT_SUCCESS,
          kvstore.put(key1, val1).getStatus());
      assertEquals(KVMessage.StatusType.PUT_SUCCESS,
          kvstore.put(key2, val2).getStatus());
      assertEquals(KVMessage.StatusType.PUT_SUCCESS,
          kvstore.put(key3, val3).getStatus());
      assertEquals(KVMessage.StatusType.PUT_SUCCESS,
          kvstore.put(key4, val4).getStatus());

      //check status of retrievals
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          kvstore.get(key0).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          kvstore.get(key1).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          kvstore.get(key2).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          kvstore.get(key3).getStatus());
      assertEquals(KVMessage.StatusType.GET_SUCCESS,
          kvstore.get(key4).getStatus());

      //check value of retrievals
      assertEquals(val0, kvstore.get(key0).getValue());
      assertEquals(val1, kvstore.get(key1).getValue());
      assertEquals(val2, kvstore.get(key2).getValue());
      assertEquals(val3, kvstore.get(key3).getValue());
      assertEquals(val4, kvstore.get(key4).getValue());

      //update each record
      assertEquals(KVMessage.StatusType.PUT_UPDATE,
          kvstore.put(key0, uval0).getStatus());
      assertEquals(KVMessage.StatusType.PUT_UPDATE,
          kvstore.put(key1, uval1).getStatus());
      assertEquals(KVMessage.StatusType.PUT_UPDATE,
          kvstore.put(key2, uval2).getStatus());
      assertEquals(KVMessage.StatusType.PUT_UPDATE,
          kvstore.put(key3, uval3).getStatus());
      assertEquals(KVMessage.StatusType.PUT_UPDATE,
          kvstore.put(key4, uval4).getStatus());

      //check value of retrievals
      assertEquals(uval0, kvstore.get(key0).getValue());
      assertEquals(uval1, kvstore.get(key1).getValue());
      assertEquals(uval2, kvstore.get(key2).getValue());
      assertEquals(uval3, kvstore.get(key3).getValue());
      assertEquals(uval4, kvstore.get(key4).getValue());

      //Delete each insertions
      assertEquals(KVMessage.StatusType.DELETE_SUCCESS,
          kvstore.put(key0, null).getStatus());
      assertEquals(KVMessage.StatusType.DELETE_SUCCESS,
          kvstore.put(key1, null).getStatus());
      assertEquals(KVMessage.StatusType.DELETE_SUCCESS,
          kvstore.put(key2, null).getStatus());
      assertEquals(KVMessage.StatusType.DELETE_SUCCESS,
          kvstore.put(key3, null).getStatus());
      assertEquals(KVMessage.StatusType.DELETE_SUCCESS,
          kvstore.put(key4, null).getStatus());

      //check that we cannot retrieval the kv pairs
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key0).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key1).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key2).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key3).getStatus());
      assertEquals(KVMessage.StatusType.GET_ERROR,
          kvstore.get(key4).getStatus());
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    ex = null;
  }

}
