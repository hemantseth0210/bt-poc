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

    @Autowired
    private RedisCommands<String, String> redisCommands;

    @Autowired
    private RedisAsyncCommands<String, String> redisAsyncCommandsCommands;

   // private RedisAsyncCommands<String, String> asyncCommands;

    private ReentrantLock lock = new ReentrantLock();
    private static int counter = 0;
    LRUCache<String, Map<String, String>> lruCache;
    List<String> upcs;
    Set<String> locations;

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
                mcc = new MemcachedClient(new InetSocketAddress(discoveryEndpoint, 11211));
                lruCache = new LRUCache(32000);
                upcs = getUpcs();
                locations = getLocations();
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
                "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size());
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
                    "final count {}, bigtable rows {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size(), rowMap.get("BigTable"));
        } else {
            log.info("processNcpEligibleUpcsByPrefixPostWithRedisCache--Cache--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {}, bigtable rows {}  " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size(), rowMap.get("Cache"));
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
                    "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size());
        } else {
            log.info("processNcpEligibleUpcsByPrefixPostWithMemcached--Cache--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size());
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
                    "final count {}, bigtable rows {} " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size(), rowMap.get("BigTable"));
        } else {
            log.info("processNcpEligibleUpcsByPrefixPostWithLRUCache--Cache--Time taken for looping the result set of Rows to final map: {} msc , rowkeys {} , " +
                    "final count {}, bigtable rows {}  " , System.currentTimeMillis() - queryTime, rowKeys.size()*locations.size(), rowMap.size(), rowMap.get("Cache"));
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
        for(String upc : upcs){
            rowKeys.put(upc, prepareUpcLocData(upc, locations));
        }
        return rowKeys;
    }


    private Set<String> prepareUpcLocData(String upc, Set<String> locNumbrs){
        Set<String> finalLocNumbrs = new HashSet<>();
        for(String locNumbr : locNumbrs){
            finalLocNumbrs.add(upc+ "#" +locNumbr);
        }
        return finalLocNumbrs;
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

    private Set<String> getLocations(){
        Set<String> locations = new HashSet<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("src/main/resources/locs.txt"));
            String line = reader.readLine();
            while (line != null) {
                locations.add(line);
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return locations;
    }



}

