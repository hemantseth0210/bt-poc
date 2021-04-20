package com.example.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConcurrentLRUCache<K,V> {

    private final int maxCapacity;
    private ConcurrentLinkedQueue<K> linkedQueue;
    private Map<K,V> map;

    public ConcurrentLRUCache(int maxCapacity){
        map = new ConcurrentHashMap<>(maxCapacity);
        this.linkedQueue = new ConcurrentLinkedQueue<K>();
        this.maxCapacity = maxCapacity;
    }

    /**
     * Fetches an object from the cache (could be null if no such mapping exists).
     * If the object is found in the cache, then it will be moved to the tail-end of the
     * doubly-linked list to indicate that it was recently accessed.
     *
     * @param key
     */
    public V get(K key){
        V result = map.get(key);
        if(result != null){
            synchronized (this) {
                linkedQueue.remove(key);
                linkedQueue.offer(key);
            }
        }
        return result;
    }

    /**
     * Adds a new object to the cache end. If the cache size has reached it's capacity,
     * then the least recently accessed object will be evicted.
     *
     * @param key
     * @param value
     */
    public void put(K key, V value){
        if (map.containsKey(key)) {
            linkedQueue.remove(key);
            linkedQueue.offer(key);
        } else {
            if (map.size() == maxCapacity) {
                K oldestKey = linkedQueue.poll();
                if (oldestKey != null) {
                    map.remove(oldestKey);
                }
            }
            linkedQueue.offer(key);
            map.put(key, value);
        }
    }



    /**
     * Utility function to print the cache objects.
     */
    public void printCache() {
       for(K key : linkedQueue){
           System.out.print(map.get(key) + " -> ");
       }
       System.out.println();
    }

}
