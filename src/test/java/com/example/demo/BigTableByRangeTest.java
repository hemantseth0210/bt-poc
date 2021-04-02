package com.example.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;


import java.io.IOException;
import java.util.*;

@SpringBootTest
public class BigTableByRangeTest {

    //@Autowired
    BigTableUtil bigTableUtil;

    @BeforeEach
    public void init() throws IOException {
        bigTableUtil = new BigTableUtil("mtech-pfl-poc", "pfl-poc-bt", "cost-optimization","");
    }

    @Test
    public void processNcpEligibleUpcsByRange(){
        Map<String, Set<String>> rowKeys = bigTableUtil.getUpcAndLocData();
        bigTableUtil.processNcpEligibleUpcsByRange(rowKeys, "cfinvc", "cfcg");
    }

}
