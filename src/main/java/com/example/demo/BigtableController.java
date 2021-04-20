package com.example.demo;

import com.google.cloud.bigtable.data.v2.models.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.*;

@RestController
public class BigtableController {

    @Autowired
    private BigTableUtil bigTableUtil;

    @GetMapping("/getRowsByRowKeyByPrefix")
    public Map<String, Row> getRowsByRowKeyByPrefix(@RequestBody List<String> rowKeys) throws IOException {
        return bigTableUtil.getRowsByRowKeyByPrefix("30097",new HashSet<>(rowKeys), "sptrcst", "trstcst");
    }

    @PostMapping("/processNcpEligibleUpcsByPrefixPost")
    public Map<String, Row> processNcpEligibleUpcsByPrefixPost(@RequestBody List<String> upcs) throws IOException {
        return bigTableUtil.processNcpEligibleUpcsByPrefixPost(upcs, "cfinvc", "cfcg");
    }

    @GetMapping("/processNcpEligibleUpcsByPrefixPostWithRedisCache")
    public Map<String, String> processNcpEligibleUpcsByPrefixPostWithRedisCache() throws IOException {
        List<String> upcs = bigTableUtil.getRandomUpcs();
        return bigTableUtil.processNcpEligibleUpcsByPrefixPostWithRedisCache(upcs, "cfinvc", "cfcg");
    }

    @GetMapping("/processNcpEligibleUpcsByPrefixPostWithMemcached")
    public Map<String, Map<String, String>> processNcpEligibleUpcsByPrefixPostWithMemcached() throws IOException {
        List<String> upcs = bigTableUtil.getRandomUpcs();
        return bigTableUtil.processNcpEligibleUpcsByPrefixPostWithMemcached(upcs, "cfinvc", "cfcg");
    }

    @GetMapping("/processNcpEligibleUpcsByPrefixPostWithLRUCache")
    public Map<String, String> processNcpEligibleUpcsByPrefixPostWithLRUCache() throws IOException {
        List<String> upcs = bigTableUtil.getRandomUpcs();
        //List<String> upcs = new ArrayList<>();
        //upcs.add("12214268527");
        return bigTableUtil.processNcpEligibleUpcsByPrefixPostWithLRUCache(upcs, "cfinvc", "cfcg");
    }
}
