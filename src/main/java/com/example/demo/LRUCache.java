package com.example.demo;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K,V> {

    private final int maxCapacity;
    private Node<K,V> head, tail;
    private Map<K, Node<K,V>> map;

    public LRUCache(int maxCapacity){
        map = new HashMap<>(maxCapacity);
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
        V result = null;
        Node<K,V> node = map.get(key);
        if(node != null){
            result = node.value;
            remove(node);
            addToTail(node);
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
            Node<K, V> node = map.get(key);
            node.value = value;
            remove(node);
            addToTail(node);
        } else {
            if (map.size() == maxCapacity) {
                map.remove(head.key);
                remove(head);
            }

            Node<K, V> node = new Node<K, V>(key, value);
            addToTail(node);
            map.put(key, node);
        }
    }

    /**
     * Adds a new object to the cache head. If the cache size has reached it's capacity,
     * then the least recently accessed object will be evicted.
     *
     * @param key
     * @param value
     */
    /*
    public void put(K key, V value){
        if(map.containsKey(key)){
            Node<K, V> node = map.get(key);
            remove(node);
            node.value = value;
            addToHead(node);
        } else {
            if(map.size() == maxCapacity){
                map.remove(tail.prev.key);
                remove(tail.prev);
            }

            Node<K, V> node = new Node<K, V>(key, value);
            map.put(key, node);
            addToHead(node);
        }
    }

    public void addToHead(Node<K, V> node){
        Node<K, V> headNext = head.next;
        head.next = node;
        node.prev = head;
        node.next = headNext;
        headNext.prev = node;
    }
    */


    /**
     * Offers a node to the tail-end of the doubly-linked list because
     * it was recently read or written.
     * @param node
     */
    private void addToTail(Node<K, V> node) {
        if (node == null)
            return;
        if (head == null) {
            head = tail = node;
        } else {
            tail.next = node;
            node.prev = tail;
            node.next = null;
            tail = node;
        }
    }

    /**
     * Removes a node from the head position doubly-linked list.
     * @param node
     */
    public void remove(Node<K, V> node){
        if(node == null)
            return;
        Node<K, V> nextNode = node.next;
        Node<K, V> prevNode = node.prev;
        if(prevNode != null){
            prevNode.next = nextNode;
        } else{
            head = nextNode;
        }

        if(nextNode != null){
            nextNode.prev = prevNode;
        } else {
            tail = prevNode;
        }
    }

    /**
     * Utility function to print the cache objects.
     */
    public void printCache() {
        Node<K, V> curr = head;
        while (curr != null) {
            System.out.print(curr.value + " -> ");
            curr = curr.next;
        }
        System.out.println();
    }


    class Node<K,V> {
        K key;
        V value;
        Node<K,V> prev, next;

        public Node(K key, V value){
            this.key = key;
            this.value = value;
        }
    }

}
