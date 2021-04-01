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

    @GetMapping("/getRowsByRowKeyByRange")
    public Map<String, Row> getRowsByRowKeyByRange(@RequestBody List<String> rowKeys) throws IOException {
        return bigTableUtil.getRowsByRowKeyByRange(rowKeys, "sptrcst", "trstcst");
    }

    @GetMapping("/processNcpEligibleUpcsByPrefix")
    public Map<String, Row> processNcpEligibleUpcsByPrefix() throws IOException {
        return bigTableUtil.processNcpEligibleUpcsByPrefix(bigTableUtil.getUpcAndLocData(), "cfinvc", "cfcg");
    }

    @GetMapping("/processNcpEligibleUpcsByRange")
    public Map<String, Row> processNcpEligibleUpcsByRange() throws IOException {
        return bigTableUtil.processNcpEligibleUpcsByRange(bigTableUtil.getUpcAndLocData(), "cfinvc", "cfcg");
    }

    @PostMapping("/processNcpEligibleUpcsByPrefixPost")
    public Map<String, Row> processNcpEligibleUpcsByPrefixPost(@RequestBody List<String> upcs) throws IOException {
        return bigTableUtil.processNcpEligibleUpcsByPrefixPost(upcs, "cfinvc", "cfcg");
    }

    @PostMapping("/processNcpEligibleUpcsByRangePost")
    public Map<String, Row> processNcpEligibleUpcsByRangePost(@RequestBody List<String> upcs) throws IOException {
        return bigTableUtil.processNcpEligibleUpcsByRangePost(upcs, "cfinvc", "cfcg");
    }

    @PostMapping("/processNcpEligibleUpcsByPrefixPostWithRedisCache")
    public Map<String, String> processNcpEligibleUpcsByPrefixPostWithRedisCache(@RequestBody List<String> upcs) throws IOException {
        return bigTableUtil.processNcpEligibleUpcsByPrefixPostWithRedisCache(upcs, "cfinvc", "cfcg");
    }
}
