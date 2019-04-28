package cache;

/**
 * Interface for a cache data structure.
 *
 * @param <K> The type of the cache's keys.
 * @param <V> The type of the cache's values.
 */
public interface ICache<K, V> {

  /**
   * Put a key-value pair into the cache.
   *
   * @param key   The key to access the data in cache.
   * @param value The value associated with the key in cache.
   */
  public void put(K key, V value);

  /**
   * Get the value associated with 'key' from the cache.
   *
   * @param key The key to access in cache.
   * @return The value associated with key 'key'.
   */
  public V get(K key);

  /**
   * Remove an element from the cache.
   *
   * @param key the key of the entry to be removed from cache.
   */
  public void removeElement(K key);

  /**
   * Clear all entries from the cache.
   */
  public void clearCache();
}
