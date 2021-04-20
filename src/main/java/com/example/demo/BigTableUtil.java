package com.example.demo;


import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.spy.memcached.MemcachedClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

@Slf4j
@Data
@Component
public class BigTableUtil {

    private String projectId;

    private String instanceId;

    private String tableId;

    private static BigtableDataClient client;
    private static final  String PFLDATAFEED_COLUMNFAMILY_CONGRUENCY =  "cfcg";
    private static final  String PFLDATAFEED_COLUMN_CONGRUENCY_FLAG =  "congruency_f";

    private static Integer count = 0;

    private MemcachedClient mcc = null;

    @Autowired
    RedisTemplate<String, String> redisTemplate;

    private String discoveryEndpoint;

    //@Autowired
    private RedisCommands<String, String> redisCommands;

    //@Autowired
    private RedisAsyncCommands<String, String> redisAsyncCommandsCommands;

   // private RedisAsyncCommands<String, String> asyncCommands;

    private ReentrantLock lock = new ReentrantLock();
    private static int counter = 0;
    LRUCache<String, Map<String, String>> lruCache;
    List<String> upcs;

    public BigTableUtil(@Value("${bigtable.project.id:}") String projectId,
                        @Value("${bigtable.instance.id:}") String instanceId,
                        @Value("${bigtable.tableId:}") String tableId,
                        @Value("${memcached.host:}") String discoveryEndpoint) throws IOException {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.tableId = tableId;
        this.discoveryEndpoint = discoveryEndpoint;
        //Skip establishing bigtable connection if either of these properties blank
        if(!projectId.isBlank() && !instanceId.isBlank() && !tableId.isBlank()) {
            connect();
        }
    }

    /**
     * Create a BigTable connection to GCP
     * @return BigtableDataClient
     */
    public BigtableDataClient connect() throws IOException {
        if(client == null) {
            log.info("Trying to connect to " + projectId + ":" + instanceId + ":" + tableId + " bigtable");
            try {
                client = BigtableDataClient.create(projectId, instanceId);
               // mcc = new MemcachedClient(new InetSocketAddress(discoveryEndpoint, 11211));
                lruCache = new LRUCache(32000);
                upcs = getUpcs();
            } catch (IOException e) {
                log.error("Connect failed!");
                throw e;
            }
            log.info("Connected! to Bigtable");
        }
        return client;
    }


    /**
     * Query bigtable with a toZip as Prefix query
     * @param rowKeyPrefix as name means
     * @param rowKeys List of row key to query
     * @return Map<String, Row>
     */
    public Map<String, Row> getRowsByRowKeyByPrefix(String rowKeyPrefix, Set<String> rowKeys, String... families) {
        Map<String, Row> map = new HashMap<>();
        try {
            Query query = Query.create(tableId).prefix(rowKeyPrefix + "#");
            long queryTime = System.currentTimeMillis();
            if (families != null && families.length > 0) {
                String familyRegex = String.join("|", families);
                Filters.Filter filter  = FILTERS.family().regex(familyRegex);
                query.filter(filter);
            }
            ServerStream<Row> rows = connect().readRows(query);
            queryTime = System.currentTimeMillis();
            int count = 0;
            for(Row row : rows) {
                count++;
                if(rowKeys.contains(row.getKey().toStringUtf8())) {
                    map.put(row.getKey().toStringUtf8(), row);
                }
            }
            log.info("getRowsByRowKeyByPrefix----Time taken for looping the result set of Rows to final final map: {} msc , total count {} , rowKeys Size {}, final count {} " , System.currentTimeMillis() - queryTime, count ,rowKeys.size(), map.size());
        } catch (IOException e) {
            //Create a dummy Row for the row key that throws an exception
            log.debug(String.valueOf(e));
        }
        return map;
    }

    // With Redis
    public Map<String, String> getRowsByRowKeyByPrefixWithRedisCache(String rowKeyPrefix, Set<String> rowKeys,
                                                                  Map<String, String> qualifierFamilyMap) {
        Map<String, String> map = new HashMap<>();
        try {
            String hashKey = rowKeyPrefix + "#";
            //Map<Object, Object> cacheMap = redisTemplate.opsForHash().entries(hashKey);
            Map<String, String> cacheMap = redisCommands.hgetall(hashKey);
            if(cacheMap != null && cacheMap.size() > 0){
                long queryTime = System.currentTimeMillis();
                for(String rowKey : rowKeys) {
                    for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                        String mappedRowkey = rowKey.concat("#").concat(entry.getKey());
                        if(cacheMap.containsKey(mappedRowkey)) {
                            map.put(mappedRowkey, (String) cacheMap.get(mappedRowkey));
                        }
                    }
                }
                log.info("getRowsByRowKeyByPrefixWithRedisCache--Cache--Time taken for looping the result set " +
                                "of Rows to final map: {} msc , total count {} , rowKeys Size {}, final count {} "
                        , System.currentTimeMillis() - queryTime, cacheMap.size() ,rowKeys.size(), map.size());
                map.put("Cache" , "0");
            } else {
                Query query = Query.create(tableId).prefix(rowKeyPrefix + "#");
                long queryTime = System.currentTimeMillis();
                if (qualifierFamilyMap != null && qualifierFamilyMap.size() > 0) {
                    String familyRegex = String.join("|", new HashSet<>(qualifierFamilyMap.values()));
                    Filters.Filter filter  = FILTERS.family().regex(familyRegex);
                    query.filter(filter);
                }
                queryTime = System.currentTimeMillis();
                ServerStream<Row> rows = connect().readRows(query);
                int count = 0;
                Map<String, String> bigtableRowsMap = new HashMap<>();
                /*
                for(Row row : rows) {
                    count++;
                    if(rowKeys.contains(row.getKey().toStringUtf8())) {

                        qualifierFamilyMap.keySet().parallelStream()
                                .map(qualifier -> readCellValue(row, qualifierFamilyMap.get(qualifier), qualifier))
                                .collect(Collectors.toList())
                                .forEach(val -> {
                                    map.putAll(val);
                                    bigtableRowsMap.putAll(val);
                                });
                        continue;
                    }
                    qualifierFamilyMap.keySet().parallelStream()
                            .map(qualifier -> readCellValue(row, qualifierFamilyMap.get(qualifier), qualifier))
                            .collect(Collectors.toList())
                            .forEach(bigtableRowsMap::putAll);
                }

                 */
                for(Row row : rows) {
                    count++;
                    if(rowKeys.contains(row.getKey().toStringUtf8())) {
                        for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                            List<RowCell> rowCells = row.getCells(entry.getValue(), entry.getKey());
                            if(rowCells != null && rowCells.size()>0){
                                String key = row.getKey().toStringUtf8() + "#" + entry.getKey();
                                String value = rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null;
                                map.put(key, value);
                                bigtableRowsMap.put(key, value);
                            }
                        }
                        continue;
                    }
                    for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                        List<RowCell> rowCells = row.getCells(entry.getValue(), entry.getKey());
                        if(rowCells != null && rowCells.size()>0){
                            bigtableRowsMap.put(row.getKey().toStringUtf8() + "#" + entry.getKey(),
                                    rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                        }

                    }
                }
                redisAsyncCommandsCommands.hmset(hashKey, bigtableRowsMap);
                redisAsyncCommandsCommands.expire(hashKey, 7200);
                //redisTemplate.opsForHash().putAll(hashKey, bigtableRowsMap);
                //redisTemplate.expire(hashKey, 10, TimeUnit.SECONDS);
               // updateCache(hashKey, bigtableRowsMap);

                log.info("getRowsByRowKeyByPrefixWithRedisCache--BigTable--Time taken for looping the result set of Rows to final " +
                        "final map: {} msc , total count {} , rowKeys Size {}, final count {} "
                        , System.currentTimeMillis() - queryTime, count ,rowKeys.size(), map.size());
                map.put("BigTable" , "0");
            }

        } catch (IOException e) {
            //Create a dummy Row for the row key that throws an exception
            log.debug(String.valueOf(e));
        }
        return map;
    }

    private Map<String, String> readCellValue(Row row, String family, String qualifier){
        Map<String, String> map = new HashMap<>();
        List<RowCell> rowCells = row.getCells(family, qualifier);
        if(rowCells != null && rowCells.size()>0){
            map.put(String.format("%s#%s",row.getKey().toStringUtf8(),qualifier), rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
        }
        return map;
    }



    // With Memcached
    public Map<String, Map<String, String>> getRowsByRowKeyByPrefixWithMemcached(String rowKeyPrefix, Set<String> rowKeys,
                                                                     Map<String, String> qualifierFamilyMap) {
        Map<String, Map<String, String>> map = new HashMap<>();
        try {
            Map<String, Object> cacheMap = mcc.getBulk(rowKeys);
            if(cacheMap != null && cacheMap.size() > 0){
                long queryTime = System.currentTimeMillis();
                for(String rowKey : rowKeys) {
                    if(cacheMap.containsKey(rowKey)) {
                        String cacheMapValue = (String) cacheMap.get(rowKey);
                        String[] qualifierFamilyArr = cacheMapValue.split("#");
                        if(qualifierFamilyArr != null && qualifierFamilyArr.length > 0){
                            Map<String, String> cacheQualifierFamilyMap = new HashMap<>();
                            for(String arrValue : qualifierFamilyArr){
                                String[] qualifierFamilyValue = arrValue.split(":");
                                if(qualifierFamilyValue != null && qualifierFamilyValue.length ==2){
                                    cacheQualifierFamilyMap.put(qualifierFamilyValue[0], qualifierFamilyValue[1]);
                                }
                            }
                            map.put(rowKey, cacheQualifierFamilyMap);
                        }
                    }
                }
                log.info("getRowsByRowKeyByPrefixWithMemcached--Cache--Time taken for looping the result set " +
                                "of Rows to final map: {} msc , total count {} , rowKeys Size {}, final count {} "
                        , System.currentTimeMillis() - queryTime, 0 ,rowKeys.size(), map.size());
                map.put("Cache" , new HashMap<>());
            } else {
                Query query = Query.create(tableId).prefix(rowKeyPrefix + "#");
                long queryTime = System.currentTimeMillis();
                if (qualifierFamilyMap != null && qualifierFamilyMap.size() > 0) {
                    String familyRegex = String.join("|", new HashSet<>(qualifierFamilyMap.values()));
                    Filters.Filter filter  = FILTERS.family().regex(familyRegex);
                    query.filter(filter);
                }
                ServerStream<Row> rows = connect().readRows(query);
                queryTime = System.currentTimeMillis();
                int count = 0;
                for(Row row : rows) {
                    count++;
                    if(rowKeys.contains(row.getKey().toStringUtf8())) {
                        StringBuilder sb = new StringBuilder();
                        int i = 0;
                        Map<String, String> qualifierMap = new HashMap<>();
                        for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                            List<RowCell> rowCells = row.getCells(entry.getValue(), entry.getKey());
                            if(rowCells != null && rowCells.size()>0){
                                qualifierMap.put(entry.getKey(), rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                                if(i == 0){
                                    sb.append(entry.getKey()).append(":").append(rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                                } else {
                                    sb.append("#").append(entry.getKey()).append(":").append(rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                                }
                                i++;
                            }
                        }
                        map.put(row.getKey().toStringUtf8(), qualifierMap);
                        mcc.set(row.getKey().toStringUtf8(), 7200, sb.toString());
                        continue;
                    }
                    StringBuilder sb = new StringBuilder();
                    int i = 0;
                    for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                        List<RowCell> rowCells = row.getCells(entry.getValue(), entry.getKey());
                        if(rowCells != null && rowCells.size()>0){
                            if(i == 0){
                                sb.append(entry.getKey()).append(":").append(rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                            } else {
                                sb.append("#").append(entry.getKey()).append(":").append(rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                            }
                            i++;
                        }
                    }
                    mcc.set(row.getKey().toStringUtf8(), 7200, sb.toString());
                }
                log.info("getRowsByRowKeyByPrefixWithMemcached--bigtable--Time taken for looping the result set of Rows to final " +
                                "final map: {} msc , total count {} , rowKeys Size {}, final count {} "
                        , System.currentTimeMillis() - queryTime, count ,rowKeys.size(), map.size());
                map.put("BigTable" , new HashMap<>());
            }

        } catch (IOException e) {
            //Create a dummy Row for the row key that throws an exception
            log.debug(String.valueOf(e));
        }
        return map;
    }

    // With In Memory LRU Cache
    public Map<String, String> getRowsByRowKeyByPrefixWithLRUCache(String rowKeyPrefix, Set<String> rowKeys,
                                                                     Map<String, String> qualifierFamilyMap) {
        Map<String, String> map = new HashMap<>();
        try {
            String hashKey = rowKeyPrefix + "#";
            Map<String, String> cacheMap = lruCache.get(hashKey);
            if(cacheMap != null && cacheMap.size() > 0){
                long queryTime = System.currentTimeMillis();
                for(String rowKey : rowKeys) {
                    for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                        String mappedRowkey = rowKey.concat("#").concat(entry.getKey());
                        if(cacheMap.containsKey(mappedRowkey)) {
                            map.put(mappedRowkey, (String) cacheMap.get(mappedRowkey));
                        }
                    }
                }
                log.info("getRowsByRowKeyByPrefixWithLRUCache--Cache--Time taken for looping the result set " +
                                "of Rows to final map: {} msc , total count {} , rowKeys Size {}, final count {} "
                        , System.currentTimeMillis() - queryTime, cacheMap.size() ,rowKeys.size(), map.size());
                map.put("Cache" , "0");
            } else {
                Query query = Query.create(tableId).prefix(rowKeyPrefix + "#");
                long queryTime = System.currentTimeMillis();
                if (qualifierFamilyMap != null && qualifierFamilyMap.size() > 0) {
                    String familyRegex = String.join("|", new HashSet<>(qualifierFamilyMap.values()));
                    Filters.Filter filter  = FILTERS.family().regex(familyRegex);
                    query.filter(filter);
                }
                queryTime = System.currentTimeMillis();
                ServerStream<Row> rows = connect().readRows(query);
                int count = 0;
                Map<String, String> bigtableRowsMap = new HashMap<>();

                for(Row row : rows) {
                    count++;
                    if(rowKeys.contains(row.getKey().toStringUtf8())) {
                        for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                            List<RowCell> rowCells = row.getCells(entry.getValue(), entry.getKey());
                            if(rowCells != null && rowCells.size()>0){
                                String key = row.getKey().toStringUtf8() + "#" + entry.getKey();
                                String value = rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null;
                                map.put(key, value);
                                bigtableRowsMap.put(key, value);
                            }
                        }
                        continue;
                    }
                    for (Map.Entry<String, String> entry : qualifierFamilyMap.entrySet()) {
                        List<RowCell> rowCells = row.getCells(entry.getValue(), entry.getKey());
                        if(rowCells != null && rowCells.size()>0){
                            bigtableRowsMap.put(row.getKey().toStringUtf8() + "#" + entry.getKey(),
                                    rowCells.get(0) != null ? rowCells.get(0).getValue().toStringUtf8() : null);
                        }

                    }
                }
                lruCache.put(hashKey, bigtableRowsMap);
                log.info("getRowsByRowKeyByPrefixWithLRUCache--BigTable--Time taken for looping the result set of Rows to final " +
                                "final map: {} msc , total count {} , rowKeys Size {}, final count {} "
                        , System.currentTimeMillis() - queryTime, count ,rowKeys.size(), map.size());
                map.put("BigTable" , "0");
            }

        } catch (IOException e) {
            //Create a dummy Row for the row key that throws an exception
            log.debug(String.valueOf(e));
        }
        return map;
    }


    public Map<String, Row> processNcpEligibleUpcsByPrefix(Map<String, Set<String>> rowKeys, String... families) {
        Map<String, Row> rowMap = new HashMap<>();
        long queryTime = System.currentTimeMillis();
        rowKeys.keySet().parallelStream()
                .map(upc -> getRowsByRowKeyByPrefix(upc, rowKeys.get(upc), families))
                .collect(Collectors.toList()).forEach(rowMap::putAll);
        log.info("processNcpEligibleUpcsByPrefix----Time taken for looping the result set of Rows to final map: {} msc , total count {} , " +
                "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size(), rowMap.size());
        return rowMap;
    }

    public Map<String, Row> processNcpEligibleUpcsByPrefixPost(List<String> upcs, String... families) {
        Map<String, Set<String>> rowKeys = prepareUpcsAndLocs(upcs);
        Map<String, Row> rowMap = new HashMap<>();
        long queryTime = System.currentTimeMillis();
        rowKeys.keySet().parallelStream()
                .map(upc -> getRowsByRowKeyByPrefix(upc, rowKeys.get(upc), families))
                .collect(Collectors.toList()).forEach(rowMap::putAll);
        log.info("processNcpEligibleUpcsByPrefixPost----Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size());
        return rowMap;
    }

    public Map<String, String> processNcpEligibleUpcsByPrefixPostWithRedisCache(List<String> upcs, String... families) {
        log.info("Serving Request number {} " , increaseCounter());
        Map<String, Set<String>> rowKeys = prepareUpcsAndLocs(upcs);
        Map<String, String> rowMap = new HashMap<>();
        Map<String, String> qualifierFamilyMap = new HashMap<>();
        qualifierFamilyMap.put("congruency_f", "cfcg");
        qualifierFamilyMap.put("ats_thld", "cfinvc");
        qualifierFamilyMap.put("inv_diff", "cfinvc");
        qualifierFamilyMap.put("inv_overrd", "cfinvc");
        long queryTime = System.currentTimeMillis();
        rowKeys.keySet().parallelStream()
                .map(upc -> getRowsByRowKeyByPrefixWithRedisCache(upc, rowKeys.get(upc), qualifierFamilyMap))
                .collect(Collectors.toList()).forEach(rowMap::putAll);
        if(rowMap.containsKey("BigTable")){
            log.info("processNcpEligibleUpcsByPrefixPostWithRedisCache--BigTable--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {}, bigtable rows {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size(), rowMap.get("BigTable"));
        } else {
            log.info("processNcpEligibleUpcsByPrefixPostWithRedisCache--Cache--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {}, bigtable rows {}  " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size(), rowMap.get("Cache"));
        }
        return rowMap;
    }

    public Map<String, Map<String, String>> processNcpEligibleUpcsByPrefixPostWithMemcached(List<String> upcs, String... families) {
        log.info("Serving Request number {} " , increaseCounter());
        Map<String, Set<String>> rowKeys = prepareUpcsAndLocs(upcs);
        Map<String, Map<String, String>> rowMap = new HashMap<>();
        Map<String, String> qualifierFamilyMap = new HashMap<>();
        qualifierFamilyMap.put("congruency_f", "cfcg");
        qualifierFamilyMap.put("ats_thld", "cfinvc");
        qualifierFamilyMap.put("inv_diff", "cfinvc");
        qualifierFamilyMap.put("inv_overrd", "cfinvc");
        long queryTime = System.currentTimeMillis();
        rowKeys.keySet().parallelStream()
                .map(upc -> getRowsByRowKeyByPrefixWithMemcached(upc, rowKeys.get(upc), qualifierFamilyMap))
                .collect(Collectors.toList()).forEach(rowMap::putAll);
        if(rowMap.containsKey("BigTable")){
            log.info("processNcpEligibleUpcsByPrefixPostWithMemcached--BigTable--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size());
        } else {
            log.info("processNcpEligibleUpcsByPrefixPostWithMemcached--Cache--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size());
        }

        return rowMap;
    }

    public Map<String, String> processNcpEligibleUpcsByPrefixPostWithLRUCache(List<String> upcs, String... families) {
        //log.info("Serving Request number {} " , increaseCounter());
        Map<String, Set<String>> rowKeys = prepareUpcsAndLocs(upcs);
        Map<String, String> rowMap = new HashMap<>();
        Map<String, String> qualifierFamilyMap = new HashMap<>();
        qualifierFamilyMap.put("congruency_f", "cfcg");
        qualifierFamilyMap.put("ats_thld", "cfinvc");
        qualifierFamilyMap.put("inv_diff", "cfinvc");
        qualifierFamilyMap.put("inv_overrd", "cfinvc");
        long queryTime = System.currentTimeMillis();
        rowKeys.keySet().parallelStream()
                .map(upc -> getRowsByRowKeyByPrefixWithLRUCache(upc, rowKeys.get(upc), qualifierFamilyMap))
                .collect(Collectors.toList()).forEach(rowMap::putAll);
        if(rowMap.containsKey("BigTable")){
            log.info("processNcpEligibleUpcsByPrefixPostWithLRUCache--BigTable--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {}, bigtable rows {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size(), rowMap.get("BigTable"));
        } else {
            log.info("processNcpEligibleUpcsByPrefixPostWithLRUCache--Cache--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {}, bigtable rows {}  " , System.currentTimeMillis() - queryTime, rowKeys.size()*getLocationNumbers().size(), rowMap.size(), rowMap.get("Cache"));
        }
        return rowMap;
    }

    /**
     * Get cell value
     * @param row big table row
     * @param colFamily  big table column family
     * @param qualifier big table column qualifier
     * @param returnType return type
     * @return converted returnType value
     */
    public static <V> V getCellValue (Row row, String colFamily, String qualifier, Class<V> returnType) {
        String cellValue = "";
        try {
            List<RowCell> rowCells = row.getCells(colFamily, qualifier);
            if (!rowCells.isEmpty()) {
                cellValue = rowCells.get(0).getValue().toStringUtf8();
                return  returnType.getConstructor(String.class).newInstance(cellValue);
            }
        } catch (Exception e) {
            log.error("Unable to cast \"{}\" value to \"{}\" type. Return null as fallback",cellValue, returnType.getTypeName());
            if(log.isDebugEnabled()) {
                log.debug("Error: {}", e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * Creates default filter that limits versions of row to 1 and also applies family filters if so
     * defined.
     *
     * @param families list of column families to filter.
     * @return Chain Filter which includes both limit filter and optional family filter
     */
    private Filters.ChainFilter createQueryFilters(String... families) {
        Filters.ChainFilter chainFilter = FILTERS.chain().filter(FILTERS.limit().cellsPerColumn(1));
        if (families != null && families.length > 0) {
            String familyRegex = String.join("|", families);
            chainFilter.filter(FILTERS.family().regex(familyRegex));
        }
        return chainFilter;
    }

    @PreDestroy
    public void preDestroy() {
        if(client != null ) {
            client.close();
        } else {
            log.info("Bigtable connection closed ");
        }
    }



    private Map<String, Set<String>> prepareUpcsAndLocs(List<String> upcs){
        Map<String, Set<String>> rowKeys = new HashMap<>();
        Set<String> locNumbrs = getLocationNumbers();
        for(String upc : upcs){
            rowKeys.put(upc, prepareUpcLocData(upc, locNumbrs));
        }
        return rowKeys;
    }

    public Map<String, Set<String>> getUpcAndLocData() {
        Map<String, Set<String>> rowKeys = new HashMap<>();
        Set<String> locNumbrs = getLocationNumbers();
        rowKeys.put("19585771977", prepareUpcLocData("19585771977", locNumbrs));
        rowKeys.put("19585748962", prepareUpcLocData("19585748962", locNumbrs));
        rowKeys.put("19585771984", prepareUpcLocData("19585771984", locNumbrs));
        rowKeys.put("19585771991", prepareUpcLocData("19585771991", locNumbrs));
        rowKeys.put("19585772004", prepareUpcLocData("19585772004", locNumbrs));
        rowKeys.put("19585772011", prepareUpcLocData("19585772011", locNumbrs));
        rowKeys.put("19585772028", prepareUpcLocData("19585772028", locNumbrs));
        rowKeys.put("19585772035", prepareUpcLocData("19585772035", locNumbrs));
        rowKeys.put("19585772042", prepareUpcLocData("19585772042", locNumbrs));
        return rowKeys;
    }
    private Set<String> prepareUpcLocData(String upc, Set<String> locNumbrs){
        Set<String> finalLocNumbrs = new HashSet<>();
        for(String locNumbr : locNumbrs){
            finalLocNumbrs.add(upc+ "#" +locNumbr);
        }
        return finalLocNumbrs;
    }

    private Set<String> getLocationNumbers(){
        Set<String> locNumbrs = new HashSet<>();
        locNumbrs.add("0008");
        locNumbrs.add("0009");
        locNumbrs.add("0010");
        locNumbrs.add("0020");
        locNumbrs.add("0021");
        locNumbrs.add("0022");
        locNumbrs.add("0023");
        locNumbrs.add("0025");
        locNumbrs.add("0027");
        locNumbrs.add("0029");
        locNumbrs.add("0030");
        locNumbrs.add("0033");
        locNumbrs.add("0034");
        locNumbrs.add("0036");
        locNumbrs.add("0042");
        locNumbrs.add("0044");
        locNumbrs.add("0045");
        locNumbrs.add("0046");
        locNumbrs.add("0048");
        locNumbrs.add("0050");
        locNumbrs.add("0051");
        locNumbrs.add("0052");
        locNumbrs.add("0053");
        locNumbrs.add("0054");
        locNumbrs.add("0055");
        locNumbrs.add("0057");
        locNumbrs.add("0058");
        locNumbrs.add("0060");
        locNumbrs.add("0064");
        locNumbrs.add("0065");
        locNumbrs.add("0066");
        locNumbrs.add("0067");
        locNumbrs.add("0069");
        locNumbrs.add("0073");
        locNumbrs.add("0079");
        locNumbrs.add("0080");
        locNumbrs.add("0084");
        locNumbrs.add("0085");
        locNumbrs.add("0086");
        locNumbrs.add("0087");
        locNumbrs.add("0089");
        locNumbrs.add("0090");
        locNumbrs.add("0094");
        locNumbrs.add("0096");
        locNumbrs.add("0098");
        locNumbrs.add("0099");
        locNumbrs.add("0104");
        locNumbrs.add("0105");
        locNumbrs.add("0106");
        locNumbrs.add("0110");
        locNumbrs.add("0120");
        locNumbrs.add("0434");
        locNumbrs.add("0442");
        locNumbrs.add("0446");
        locNumbrs.add("0448");
        locNumbrs.add("0452");
        locNumbrs.add("0537");
        locNumbrs.add("0986");
        locNumbrs.add("1380");
        locNumbrs.add("1674");
        locNumbrs.add("2449");
        locNumbrs.add("2450");
        locNumbrs.add("2451");
        locNumbrs.add("2452");
        locNumbrs.add("2454");
        locNumbrs.add("2455");
        locNumbrs.add("2457");
        locNumbrs.add("2458");
        locNumbrs.add("2459");
        locNumbrs.add("2460");
        locNumbrs.add("2461");
        locNumbrs.add("2463");
        locNumbrs.add("2464");
        locNumbrs.add("2468");
        locNumbrs.add("2469");
        locNumbrs.add("2470");
        locNumbrs.add("2471");
        locNumbrs.add("2472");
        locNumbrs.add("2473");
        locNumbrs.add("2474");
        locNumbrs.add("2476");
        locNumbrs.add("2478");
        locNumbrs.add("2486");
        locNumbrs.add("2492");
        locNumbrs.add("2493");
        locNumbrs.add("2495");
        locNumbrs.add("2499");
        locNumbrs.add("2500");
        locNumbrs.add("2504");
        locNumbrs.add("2507");
        locNumbrs.add("2509");
        locNumbrs.add("2510");
        locNumbrs.add("2517");
        locNumbrs.add("2518");
        locNumbrs.add("2522");
        locNumbrs.add("2523");
        locNumbrs.add("2525");
        locNumbrs.add("2526");
        locNumbrs.add("2528");
        locNumbrs.add("3819");
        locNumbrs.add("4549");
        locNumbrs.add("4550");
        locNumbrs.add("4551");
        locNumbrs.add("4552");
        locNumbrs.add("4573");
        locNumbrs.add("4574");
        locNumbrs.add("4592");
        locNumbrs.add("4593");
        locNumbrs.add("4594");
        locNumbrs.add("4595");
        locNumbrs.add("4598");
        locNumbrs.add("4599");
        locNumbrs.add("5015");
        locNumbrs.add("5020");
        locNumbrs.add("5021");
        locNumbrs.add("5022");
        locNumbrs.add("5030");
        locNumbrs.add("5031");
        locNumbrs.add("5032");
        locNumbrs.add("5033");
        locNumbrs.add("5035");
        locNumbrs.add("5036");
        locNumbrs.add("5037");
        locNumbrs.add("5038");
        locNumbrs.add("5041");
        locNumbrs.add("5042");
        locNumbrs.add("5043");
        locNumbrs.add("5044");
        locNumbrs.add("5047");
        locNumbrs.add("5052");
        locNumbrs.add("5066");
        locNumbrs.add("5067");
        locNumbrs.add("5068");
        locNumbrs.add("5069");
        locNumbrs.add("5072");
        locNumbrs.add("5076");
        locNumbrs.add("5077");
        locNumbrs.add("5078");
        locNumbrs.add("5080");
        locNumbrs.add("5088");
        locNumbrs.add("5089");
        locNumbrs.add("5092");
        locNumbrs.add("5098");
        locNumbrs.add("5113");
        locNumbrs.add("5115");
        locNumbrs.add("5133");
        locNumbrs.add("5134");
        locNumbrs.add("5135");
        locNumbrs.add("5138");
        locNumbrs.add("5139");
        locNumbrs.add("5153");
        locNumbrs.add("5155");
        locNumbrs.add("5156");
        locNumbrs.add("5157");
        locNumbrs.add("5160");
        locNumbrs.add("5169");
        locNumbrs.add("5170");
        locNumbrs.add("5187");
        locNumbrs.add("5194");
        locNumbrs.add("5195");
        locNumbrs.add("5200");
        locNumbrs.add("5203");
        locNumbrs.add("5205");
        locNumbrs.add("5210");
        locNumbrs.add("5211");
        locNumbrs.add("5213");
        locNumbrs.add("5217");
        locNumbrs.add("5218");
        locNumbrs.add("5219");
        locNumbrs.add("5225");
        locNumbrs.add("5226");
        locNumbrs.add("5227");
        locNumbrs.add("5228");
        locNumbrs.add("5229");
        locNumbrs.add("5230");
        locNumbrs.add("5234");
        locNumbrs.add("5236");
        locNumbrs.add("5239");
        locNumbrs.add("5246");
        locNumbrs.add("5247");
        locNumbrs.add("5249");
        locNumbrs.add("5251");
        locNumbrs.add("5259");
        locNumbrs.add("5260");
        locNumbrs.add("5261");
        locNumbrs.add("5262");
        locNumbrs.add("5266");
        locNumbrs.add("5271");
        locNumbrs.add("5272");
        locNumbrs.add("5274");
        locNumbrs.add("5275");
        locNumbrs.add("5277");
        locNumbrs.add("5283");
        locNumbrs.add("5284");
        locNumbrs.add("5285");
        locNumbrs.add("5288");
        locNumbrs.add("5297");
        locNumbrs.add("5298");
        locNumbrs.add("5299");
        locNumbrs.add("5300");
        locNumbrs.add("5301");
        locNumbrs.add("5302");
        locNumbrs.add("5306");
        locNumbrs.add("5318");
        locNumbrs.add("5319");
        locNumbrs.add("5320");
        locNumbrs.add("5323");
        locNumbrs.add("5326");
        locNumbrs.add("5330");
        locNumbrs.add("5332");
        locNumbrs.add("5334");
        locNumbrs.add("5341");
        locNumbrs.add("5343");
        locNumbrs.add("5344");
        locNumbrs.add("5350");
        locNumbrs.add("5351");
        locNumbrs.add("5352");
        locNumbrs.add("5353");
        locNumbrs.add("5359");
        locNumbrs.add("5370");
        locNumbrs.add("5382");
        locNumbrs.add("5384");
        locNumbrs.add("5387");
        locNumbrs.add("5395");
        locNumbrs.add("5397");
        locNumbrs.add("5402");
        locNumbrs.add("5408");
        locNumbrs.add("5409");
        locNumbrs.add("5410");
        locNumbrs.add("5411");
        locNumbrs.add("5419");
        locNumbrs.add("5420");
        locNumbrs.add("5421");
        locNumbrs.add("5424");
        locNumbrs.add("5426");
        locNumbrs.add("5432");
        locNumbrs.add("5433");
        locNumbrs.add("5434");
        locNumbrs.add("5435");
        locNumbrs.add("5438");
        locNumbrs.add("5442");
        locNumbrs.add("5446");
        locNumbrs.add("5447");
        locNumbrs.add("5448");
        locNumbrs.add("5449");
        locNumbrs.add("5450");
        locNumbrs.add("5451");
        locNumbrs.add("5452");
        locNumbrs.add("5453");
        locNumbrs.add("5454");
        locNumbrs.add("5458");
        locNumbrs.add("5459");
        locNumbrs.add("5460");
        locNumbrs.add("5465");
        locNumbrs.add("5466");
        locNumbrs.add("5484");
        locNumbrs.add("5485");
        locNumbrs.add("5487");
        locNumbrs.add("5488");
        locNumbrs.add("5494");
        locNumbrs.add("5514");
        locNumbrs.add("6208");
        locNumbrs.add("6210");
        locNumbrs.add("6230");
        locNumbrs.add("6236");
        locNumbrs.add("6241");
        locNumbrs.add("6293");
        return locNumbrs;
    }

    private List<String> getToZipAndLocData(){
        List<String> rowKeys = new ArrayList<>();
        rowKeys.add("30097#0025");
        rowKeys.add("30097#0088");
        rowKeys.add("30097#0120");
        rowKeys.add("30097#0356");
        rowKeys.add("30097#0490");
        rowKeys.add("30097#0941");
        rowKeys.add("30097#1122");
        rowKeys.add("30097#1476");
        rowKeys.add("30097#2448");
        rowKeys.add("30097#2454");
        rowKeys.add("30097#2467");
        rowKeys.add("30097#2487");
        rowKeys.add("30097#2504");
        rowKeys.add("30097#2518");
        rowKeys.add("30097#2537");
        rowKeys.add("30097#4207");
        rowKeys.add("30097#4556");
        rowKeys.add("30097#4574");
        rowKeys.add("30097#4581");
        rowKeys.add("30097#4596");
        rowKeys.add("30097#4602");
        rowKeys.add("30097#4614");
        rowKeys.add("30097#5019");
        rowKeys.add("30097#5030");
        rowKeys.add("30097#5038");
        rowKeys.add("30097#5045");
        rowKeys.add("30097#5054");
        rowKeys.add("30097#5063");
        rowKeys.add("30097#5071");
        rowKeys.add("30097#5077");
        rowKeys.add("30097#5086");
        rowKeys.add("30097#5093");
        rowKeys.add("30097#5102");
        rowKeys.add("30097#5112");
        rowKeys.add("30097#5123");
        rowKeys.add("30097#5134");
        rowKeys.add("30097#5141");
        rowKeys.add("30097#5146");
        rowKeys.add("30097#5154");
        rowKeys.add("30097#5161");
        rowKeys.add("30097#5172");
        rowKeys.add("30097#5176");
        rowKeys.add("30097#5185");
        rowKeys.add("30097#5190");
        rowKeys.add("30097#5197");
        rowKeys.add("30097#5205");
        rowKeys.add("30097#5219");
        rowKeys.add("30097#5228");
        rowKeys.add("30097#5239");
        rowKeys.add("30097#5247");
        rowKeys.add("30097#5253");
        rowKeys.add("30097#5261");
        rowKeys.add("30097#5267");
        rowKeys.add("30097#5275");
        rowKeys.add("30097#5288");
        rowKeys.add("30097#5298");
        rowKeys.add("30097#5356");
        rowKeys.add("30097#5381");
        rowKeys.add("30097#5394");
        rowKeys.add("30097#5400");
        return rowKeys;
    }

    public List<List<String>> getUpcList(){
        List<List<String>> upcsList = new ArrayList<>();
        List<String> upcs1 = new ArrayList<>();
        upcs1.add("19585771977");
        upcs1.add("19585748962");
        upcs1.add("19585771984");
        upcs1.add("19585771991");
        upcs1.add("19585772004");
        upcs1.add("19585772011");
        upcs1.add("19585772028");
        upcs1.add("19585772035");
        upcs1.add("19585772042");
        List<String> upcs2 = new ArrayList<>();
        upcs2.add("10075741066261");
        upcs2.add("10075741068524");
        upcs2.add("10075741070305");
        upcs2.add("1008627911135");
        upcs2.add("111602037759");
        upcs2.add("111602077113");
        upcs2.add("111602218325");
        upcs2.add("111602240456");
        upcs2.add("1131086871");
        upcs2.add("1131208662");
        List<String> upcs3 = new ArrayList<>();
        upcs3.add("12115711207");
        upcs3.add("12115711252");
        upcs3.add("12115711269");
        upcs3.add("12115711290");
        upcs3.add("12214268527");
        upcs3.add("12214268534");
        upcs3.add("12214268541");
        upcs3.add("12214268596");
        upcs3.add("12214268602");
        upcs3.add("12214268619");
        List<String> upcs4 = new ArrayList<>();
        upcs4.add("12214908614");
        upcs4.add("12214908621");
        upcs4.add("12214908638");
        upcs4.add("12214908645");
        upcs4.add("12214908652");
        upcs4.add("12214974183");
        upcs4.add("12214974190");
        upcs4.add("12214974213");
        upcs4.add("12214974220");
        upcs4.add("12214974237");
        List<String> upcs5 = new ArrayList<>();
        upcs5.add("12998070996");
        upcs5.add("12998071009");
        upcs5.add("12998071016");
        upcs5.add("12998071023");
        upcs5.add("12998071030");
        upcs5.add("12998071047");
        upcs5.add("12998071054");
        upcs5.add("12998071061");
        upcs5.add("12998071078");
        upcs5.add("12998071085");
        List<String> upcs6 = new ArrayList<>();
        upcs6.add("12998071092");
        upcs6.add("12998071108");
        upcs6.add("12998071115");
        upcs6.add("12998071122");
        upcs6.add("12998071139");
        upcs6.add("12998072006");
        upcs6.add("12998072013");
        upcs6.add("12998072020");
        upcs6.add("12998236460");
        upcs6.add("12998236668");
        upcsList.add(upcs1);
        upcsList.add(upcs2);
        upcsList.add(upcs3);
        upcsList.add(upcs4);
        upcsList.add(upcs5);
        upcsList.add(upcs6);
        return upcsList;
    }

    private int increaseCounter() {
        int count = 0;
        lock.lock();
        try {
            count = counter++;
        } finally {
            lock.unlock();
        }
        return count;
    }

    public List<String> getRandomUpcs(){
        //List<String> upcs = getUpcs();
        List<String> finalUpcsList = new ArrayList<>();
        Collections.shuffle(upcs, new Random(upcs.size()));
        Random rand = new Random();
        for (int i = 0; i < 10; i++) {
            int randomIndex = rand.nextInt(upcs.size());
            finalUpcsList.add(upcs.get(randomIndex));
        }
        return finalUpcsList;
    }

    private List<String> getUpcs(){
        List<String> upcs = new ArrayList<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("src/main/resources/upcs.txt"));
            String line = reader.readLine();
            while (line != null) {
                upcs.add(line);
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return upcs;
    }



}

