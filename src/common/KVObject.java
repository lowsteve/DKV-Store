package common;

import java.math.BigInteger;
import java.util.Comparator;

public class KVObject {

  private String key;
  private String value;
  private BigInteger hash;

  public KVObject(String key, String value, BigInteger hash) {
    this.key = key;
    this.value = value;
    this.hash = hash;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public BigInteger getHash() {
    return hash;
  }

  public static Comparator<KVObject> hashComp = new Comparator<KVObject>() {
    public int compare(KVObject a, KVObject b) {
      return a.getHash().compareTo(b.getHash());
    }
  };
}