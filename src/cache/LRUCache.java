package cache;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * And implementation of LRU caching strategy.
 *
 * @param <K> The type of the caches keys.
 * @param <V> The type of the caches values.
 */
public class LRUCache<K, V> implements ICache<K, V> {

  // Careful: cache isn't threadsafe.
  private final Map<K, V> cache;


  /**
   * Construct a LRU cache with the given capacity.
   *
   * @param capacity Max number of elements in the cache.
   */
  public LRUCache(final int capacity) {
    cache = new LinkedHashMap<K, V>(capacity, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > capacity;
      }
    };
  }

  /**
   * Get the value associated with 'key' from the cache.
   *
   * @param key The key to access in cache.
   * @return The value associated with the key in cache.
   */
  @Override
  public V get(K key) {
    synchronized (cache) {
      return cache.get(key);
    }
  }

  /**
   * Put a key-value pair into the cache.
   *
   * @param key   The key to access the data in cache.
   * @param value The value associated with the key in cache.
   */
  @Override
  public void put(K key, V value) {
    synchronized (cache) {
      cache.put(key, value);
    }
  }

  /**
   * Remove an element from the cache.
   *
   * @param key the key of the entry to be removed from cache.
   */
  @Override
  public void removeElement(K key) {
    synchronized (cache) {
      cache.remove(key);
    }
  }

  /**
   * Clear all entries from the cache.
   */
  @Override
  public void clearCache() {
    synchronized (cache) {
      cache.clear();
    }
  }
}
