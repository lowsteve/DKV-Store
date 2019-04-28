package cache;

import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * And implementation of LFU caching strategy.
 *
 * @param <K> The type of the caches keys.
 * @param <V> The type of the caches values.
 */
public class LFUCache<K, V> implements ICache<K, V> {

  private FrequencyNode<K> head;
  private int capacity;
  private HashMap<K, V> kvMap;
  private HashMap<K, FrequencyNode<K>> frequencyMap;

  private final Object lock = new Object();

  /**
   * Construct an LFU cache with the given capacity.
   *
   * @param capacity Max number of elements in the cache.
   */
  public LFUCache(final int capacity) {
    this.head = null;
    this.capacity = capacity;
    this.kvMap = new HashMap<>();
    this.frequencyMap = new HashMap<>();
  }

  /**
   * Get the value associated with 'key' from the cache.
   *
   * @param key The key to access in cache.
   * @return The value associated with key 'key'
   */
  @Override
  public V get(K key) {
    synchronized (lock) {
      if (kvMap.containsKey(key)) {
        updateFrequency(key);
        return kvMap.get(key);
      }
    }
    return null;
  }

  /**
   * Put a key-value pair into the cache.
   *
   * @param key   The key to access the data in cache.
   * @param value The value associated with the key in cache.
   */
  @Override
  public void put(K key, V value) {
    synchronized (lock) {
      if (capacity == 0) {
        return;
      }
      if (kvMap.containsKey(key)) {
        kvMap.put(key, value);
      } else {
        if (kvMap.size() < capacity) {
          kvMap.put(key, value);
        } else {
          doEviction();
          kvMap.put(key, value);
        }
        insertAtHead(key);
      }
    }
  }

  /**
   * Remove an element from the cache
   *
   * @param key the key of the entry to be removed from cache.
   */
  @Override
  public void removeElement(K key) {
    synchronized (lock) {
      FrequencyNode<K> frequencyNode = frequencyMap.get(key);
      if (frequencyNode != null) {
        frequencyNode.keys.remove(key);
        if (frequencyNode.keys.size() == 0) {
          removeFrequencyNode(frequencyNode);
        }
      }
      kvMap.remove(key);
    }
  }

  /**
   * Clear all entries from the cache.
   */
  @Override
  public void clearCache() {
    synchronized (lock) {
      head = null;
      kvMap.clear();
      frequencyMap.clear();
    }

  }

  /**
   * Find key 'key' in the frequency list and increase its frequency. This
   * will remove the key from its current frequency node and either put it
   * into a new frequency node with a higher frequency, or if such a node
   * already exists, will append to that node.
   *
   * @param key The key to whose frequency to update.
   */
  private void updateFrequency(K key) {
    // Remove the key from the current frequency node it is in.
    FrequencyNode<K> frequencyNode = frequencyMap.get(key);
    frequencyNode.keys.remove(key);

    if (frequencyNode.next == null) {
      // We are already highest in the frequency list; so make a new node
      frequencyNode.next = new FrequencyNode<K>(frequencyNode.frequency + 1);
      frequencyNode.next.prev = frequencyNode;
      frequencyNode.next.keys.add(key);
    } else if (frequencyNode.next.frequency == frequencyNode.frequency + 1) {
      // Next frequency node already exists
      frequencyNode.next.keys.add(key);
    } else {
      // Insert frequency node between current and next.
      FrequencyNode<K> temp = new FrequencyNode<K>(frequencyNode.frequency + 1);
      temp.keys.add(key);
      temp.prev = frequencyNode;
      temp.next = frequencyNode.next;
      frequencyNode.next.prev = temp;
      frequencyNode.next = temp;
    }
    frequencyMap.put(key, frequencyNode.next);
    if (frequencyNode.keys.size() == 0) {
      removeFrequencyNode(frequencyNode);
    }
  }

  /**
   * Removes a frequencyNode from the frequencyList. This occurs when all keys
   * have been promoted or removed from the node.
   *
   * @param frequencyNode The frequencyNode to delete.
   */
  private void removeFrequencyNode(FrequencyNode<K> frequencyNode) {
    if (frequencyNode.prev == null) {
      head = frequencyNode.next;
    } else {
      frequencyNode.prev.next = frequencyNode.next;
    }
    if (frequencyNode.next != null) {
      frequencyNode.next.prev = frequencyNode.prev;
    }
  }

  /**
   * Evict the LFU key-value pair from the frequencyList.
   */
  private void doEviction() {
    if (head == null) return;
    K lfuNode = null;
    for (K n : head.keys) {
      // Hacky way to get the oldest lfu node in case of multiple.
      lfuNode = n;
      break;
    }
    head.keys.remove(lfuNode);
    if (head.keys.size() == 0) {
      removeFrequencyNode(head);
    }
    frequencyMap.remove(lfuNode);
    kvMap.remove(lfuNode);
  }

  /**
   * Insert a new frequencyNode at the head of the frequencyList.
   *
   * @param key The key with which to make the new frequencyNode.
   */
  private void insertAtHead(K key) {
    if (head == null) {
      head = new FrequencyNode<K>(0);
      head.keys.add(key);
    } else if (head.frequency > 0) {
      FrequencyNode<K> frequencyNode = new FrequencyNode<K>(0);
      frequencyNode.keys.add(key);
      frequencyNode.next = head;
      head.prev = frequencyNode;
      head = frequencyNode;
    } else {
      head.keys.add(key);
    }
    frequencyMap.put(key, head);
  }

  /**
   * A doubly linked list of frequency nodes.
   *
   * @param <Key> The type of the keys.
   */
  private static class FrequencyNode<Key> {

    public LinkedHashSet<Key> keys;
    public FrequencyNode<Key> prev;
    public FrequencyNode<Key> next;
    public int frequency;

    /**
     * Construct a new frequencyNode with frequency 'frequency'.
     *
     * @param frequency The frequency at this node.
     */
    public FrequencyNode(int frequency) {
      this.frequency = frequency;
      this.keys = new LinkedHashSet<>();
      this.prev = null;
      this.next = null;
    }
  }
}
