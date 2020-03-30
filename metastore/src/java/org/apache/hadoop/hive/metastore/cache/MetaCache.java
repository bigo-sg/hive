package org.apache.hadoop.hive.metastore.cache;

import javafx.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.redisson.Redisson;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tangyun@bigo.sg
 * @date 3/23/20 12:24 PM
 */
public class MetaCache {

    private final HiveConf hiveConf;
    public static final Logger LOG = LoggerFactory.getLogger(MetaCache.class);
    public static final String THREE_HORIZONTAL_BAR = "---";
    public static final String SINGLE_DOT = ".";
    public static final int LOCK_SECONDS = 30;
    public static final String CACHE_ENABLED_KEY = "CACHE_ENABLED_KEY";

    private JedisPool pool = null;
    private Lock lock = new ReentrantLock();
    private RedissonClient redisson;

    public MetaCache(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        if (hiveConf.get("hive.metastore.redis.cache.enabled").equals("true")) {
            initCache();
        }
    }

    private void initCache() {
        Config config = new Config();
        config.useClusterServers()
                .addNodeAddress(directJoin("redis://",
                        hiveConf.get("hive.metastore.redis.cache.host"),
                        ":",
                        hiveConf.get("hive.metastore.redis.cache.port")));
        redisson = Redisson.create(config);
        try {
            Jedis cache = getCacheFromPool();
            cache.set(CACHE_ENABLED_KEY, "true");
            cache.close();
        } catch (Exception e) {
            LOG.error("cache init failed!", e);
        }
    }

    public RReadWriteLock getReadWriteLock(String key) {
        return redisson.getReadWriteLock(key);
    }

    public Jedis getCacheFromPool() {
        if (pool == null) {
            lock.lock();
            try {
                String host = hiveConf.get("hive.metastore.redis.cache.host");
                Integer port = hiveConf.getInt("hive.metastore.redis.cache.port", 6579);
                if (pool == null) {
                    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                    pool = new JedisPool(jedisPoolConfig, host, port);
                }
            } finally {
                lock.unlock();
            }
        }
        Jedis cache = pool.getResource();
        String password = hiveConf.get("hive.metastore.redis.cache.password");
        cache.auth(password);
        return cache;
    }

    public static List<byte[]> deserilizeFromBytes(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream in = new ObjectInputStream(bais);
        List<byte[]>  objectAsDeserialize = (List<byte[]>) in.readObject();
        bais.close();
        in.close();
        return objectAsDeserialize;
    }

    public static byte[] serilizeToBytes(List<byte[]> list) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos  = new ObjectOutputStream(bos);
        oos.writeObject(list);
        byte[] bytes = bos.toByteArray();
        return bytes;
    }

    public boolean cacheEnabled() {
        try {
            Jedis cache = getCacheFromPool();
            String data = null;
            try {
                data = cache.get(CACHE_ENABLED_KEY);
            } finally {
                cache.close();
            }
            if (null != data) {
                return Boolean.valueOf(data);
            }
        } catch (Exception e) {
            LOG.warn("get cache data failed!", e);
        }
        return false;
    }

    final static String CACHE_PARTITION_NAMES_PS_PROFIX = "CACHE_PARTITION_NAMES_PS_PROFIX_";

    private void invalidateCachedKey(String prefix, final String db_name,
                                     final String tbl_name, String failedMessage) {
        if (!cacheEnabled()) {
            return;
        }
        String key = directJoin(prefix, db_name, SINGLE_DOT, tbl_name);
        RReadWriteLock readWriteLock = getReadWriteLock(directJoin(
                LOCK_PREFIX,
                prefix,
                db_name,
                SINGLE_DOT,
                tbl_name));
        readWriteLock.writeLock().lock(30, TimeUnit.SECONDS);
        try {
            Jedis cache = getCacheFromPool();
            try {
                cache.del(key);
            } finally {
                cache.close();
            }
        } catch (Exception e) {
            LOG.warn(failedMessage, e);
        } finally {
            readWriteLock.writeLock().forceUnlock();
        }
        LOG.info("clear cache key " + key);
    }

    public static final String LOCK_PREFIX = "LOCK_PREFIX_";

    public String directJoin(String... args) {
        if (args == null) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();

        for (String arg: args) {
            stringBuilder.append(arg);
        }
        return stringBuilder.toString();
    }

    public void invalidateCachedPartitionNamesPs(final String db_name,
                                                 final String tbl_name) {
        invalidateCachedKey(CACHE_PARTITION_NAMES_PS_PROFIX, db_name, tbl_name,
                "delete cached partition names failed");
    }

    public List<String> getCachedPartitionNamesPs(final String db_name,
                                                  final String tbl_name,
                                                  final List<String> part_vals) {
        List<String> result = new ArrayList<>();
        if (!cacheEnabled()) {
            return result;
        }
        String table = directJoin(CACHE_PARTITION_NAMES_PS_PROFIX,
                db_name,
                SINGLE_DOT,
                tbl_name);
        String key = join(part_vals, THREE_HORIZONTAL_BAR);
        try {
            Jedis cache = getCacheFromPool();
            String data = null;
            try {
                data = cache.hget(table, key);
            } finally {
                cache.close();
            }
            if (data != null) {
                String[] splited = data.split(THREE_HORIZONTAL_BAR);
                for (String line : splited) {
                    result.add(line);
                }
            }
        } catch (Exception e) {
            LOG.warn("get cache data failed!", e);
        }
        return result;
    }

    public void cachePartitionNamesPs(final String db_name,
                                      final String tbl_name,
                                      final List<String> part_vals,
                                      List<String> data) {
        if (!cacheEnabled()) {
            return;
        }
        String table = directJoin(CACHE_PARTITION_NAMES_PS_PROFIX,
                db_name,
                SINGLE_DOT,
                tbl_name);
        String key = join(part_vals, THREE_HORIZONTAL_BAR);
        String value = join(data, THREE_HORIZONTAL_BAR);
        try {
            Jedis cache = getCacheFromPool();
            try {
                cache.hset(table, key, value);
                cache.expire(key,
                        hiveConf.getInt("hive.metastore.redis.cache.ttl", 60 * 60));
            } finally {
                cache.close();
            }
        } catch (Exception e) {
            LOG.warn("cache data failed!", e);
        }
    }

    public final static String CACHE_PARTITIONS_BY_NAMES_PROFIX = "CACHE_PARTITIONS_BY_NAMES_PROFIX_";

    public void setCachePartitionsByNames(final String dbName,
                                         final String tblName, final List<Partition> partitions) {
        if (cacheEnabled()) {
            if (partitions == null || partitions.isEmpty()) {
                return;
            }
            Jedis cache = getCacheFromPool();
            String key = directJoin(CACHE_PARTITIONS_BY_NAMES_PROFIX,
                    dbName,
                    SINGLE_DOT,
                    tblName);
            try {
                Map<String, String> data = new HashMap<>();
                for (Partition partition : partitions) {
                    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
                    byte[] bytesPartition = new byte[0];
                    try {
                        bytesPartition = serializer.serialize(partition);
                    } catch (TException e) {
                        LOG.error("serializer error", e);
                    }
                    String partName = join(partition.getValues(), THREE_HORIZONTAL_BAR);
                    data.put(partName, new String(bytesPartition));
                }
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    cache.hset(key, entry.getKey(), entry.getValue());
                }
                cache.expire(key, hiveConf.getInt("hive.metastore.redis.cache.ttl", 60 * 60));
            } finally {
                cache.close();
            }
        }
    }

    private String getPartitionNameKey(String partitionName) {
        if (partitionName == null || partitionName.isEmpty()) {
            return partitionName;
        }
        StringBuilder stringBuilder = new StringBuilder();
        String[] partitions = partitionName.split("/");
        stringBuilder.append(partitions[0], partitions[0].lastIndexOf("=") + 1, partitions[0].length());
        for (int i = 1; i < partitions.length; ++i) {
            String partition = partitions[i];
            stringBuilder.append("---");
            stringBuilder.append(partition, partition.lastIndexOf("=") + 1, partition.length());
        }
        return stringBuilder.toString();
    }

    public Pair<List<Partition>, List<String>> getCachedPartitionSByNames(final String dbName,
                                                           final String tblName,
                                                           final List<String> partitionNames) {
        List<Partition> found = new ArrayList<>();
        List<String> notFound = new ArrayList<>();
        if (cacheEnabled()) {
            notFound.addAll(partitionNames);
            return new Pair<>(found, notFound);
        }
        Jedis cache = getCacheFromPool();
        String key = directJoin(CACHE_PARTITIONS_BY_NAMES_PROFIX,
                dbName,
                SINGLE_DOT,
                tblName);
        Map<String, String> data = cache.hgetAll(key);
        for (String partitionName: partitionNames) {
            String nameKey = getPartitionNameKey(partitionName);
            String value = data.get(nameKey);
            if (value == null) {
                notFound.add(partitionName);
                continue;
            }
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            Partition partition = new Partition();
            try {
                deserializer.deserialize(partition, value.getBytes());
            } catch (TException e) {
                LOG.error("deserializer error", e);
            }
            found.add(partition);
        }
        return new Pair<>(found, notFound);
    }

    static String join(List<String> data, String delimiter) {
        if (data == null || data.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(data.get(0));
        for (int i = 1; i < data.size(); ++i) {
            sb.append(delimiter);
            sb.append(data.get(i));
        }
        return sb.toString();
    }

    public final static String CACHE_ALL_TABLES_OF_DB_PROFIX = "CACHE_ALL_TABLES_OF_DB_PROFIX_";

    public void cacheAllTablesOfDb(String dbName, List<String> tables) {
        if (!cacheEnabled()) {
            return;
        }
        try {
            Jedis cache = getCacheFromPool();
            try {
                String value = join(tables, THREE_HORIZONTAL_BAR);
                cache.set(CACHE_ALL_TABLES_OF_DB_PROFIX + dbName, value);
                cache.expire(CACHE_ALL_TABLES_OF_DB_PROFIX + dbName,
                        hiveConf.getInt("hive.metastore.redis.cache.ttl", 60 * 60));
            } finally {
                cache.close();
            }
        } catch (Exception e) {
            LOG.warn("cacheAllTablesOfDb failed!", e);
        }
    }

    public List<String> getCachedAllTablesOfDb(String dbName) {
        List<String> result = new ArrayList<>();
        if (cacheEnabled()) {
            try {
                Jedis cache = getCacheFromPool();
                try {
                    String data = cache.get(CACHE_ALL_TABLES_OF_DB_PROFIX + dbName);
                    if (data == null) {
                        return result;
                    }
                    String[] tables = data.split(THREE_HORIZONTAL_BAR);
                    for (String table : tables) {
                        result.add(table);
                    }
                } finally {
                    cache.close();
                }
            } catch (Exception e) {
                LOG.warn("cache data failed!", e);
            }
        }
        return result;
    }

    public final static String CACHE_TABLE_PROFIX = "CACHE_TABLE_PROFIX_";

    public void invalidateCachedTable(final String dbname, final String name) {
        invalidateCachedKey(CACHE_TABLE_PROFIX, dbname, name,
                "delete cached table failed!");
    }

    public void cacheTable(final String dbname, final String name, Table table) {
        if (!cacheEnabled()) {
            return;
        }
        try {
            Jedis cache = getCacheFromPool();
            try {
                TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
                byte[] bytesPartition = serializer.serialize(table);
                cache.set(directJoin(CACHE_TABLE_PROFIX, dbname, SINGLE_DOT, name), new String(bytesPartition));
                cache.expire(directJoin(CACHE_TABLE_PROFIX, dbname, SINGLE_DOT, name),
                        hiveConf.getInt("hive.metastore.redis.cache.ttl", 60 * 60));
            } finally {
                cache.close();
            }
        } catch (Exception e) {
            LOG.warn("cache table failed!", e);
        }
    }

    public Table getCachedTable(final String dbname, final String name) {
        if (cacheEnabled()) {
            try {
                Jedis cache = getCacheFromPool();
                try {
                    String data = cache.get(directJoin(CACHE_TABLE_PROFIX, dbname, SINGLE_DOT, name));
                    if (data != null) {
                        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
                        byte[] serializedContainer = data.getBytes();
                        Table table = new Table();
                        deserializer.deserialize(table, serializedContainer);
                        return table;
                    }
                } finally {
                    cache.close();
                }
            } catch (Exception e) {
                LOG.warn("get cached table failed!", e);
            }
        }
        return null;
    }

    final static String CACHE_TABLE_FIELDS_PROFIX = "CACHE_TABLE_FIELDS_PROFIX_";

    public void invalidateCachedFields(String databaseName, String tableName) {
        invalidateCachedKey(CACHE_TABLE_FIELDS_PROFIX, databaseName, tableName,
                "delete cached fields failed!");
    }

    public void cacheFields(String databaseName, String tableName, List<FieldSchema> fieldSchemas) {
        if (!cacheEnabled()) {
            return;
        }
        try {
            Jedis cache = getCacheFromPool();
            List<byte[]> data = new ArrayList<>();
            for (FieldSchema fieldSchema: fieldSchemas) {
                TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
                byte[] bytesField = serializer.serialize(fieldSchema);
                data.add(bytesField);
            }
            String value = new String(serilizeToBytes(data));
            try {
                cache.set(directJoin(CACHE_TABLE_FIELDS_PROFIX, databaseName, SINGLE_DOT, tableName), value);
                cache.expire(directJoin(CACHE_TABLE_FIELDS_PROFIX, databaseName, SINGLE_DOT, tableName),
                        hiveConf.getInt("hive.metastore.redis.cache.ttl", 60 * 60));
            } finally {
                cache.close();
            }
        } catch (Exception e) {
            LOG.warn("cache table fields failed!", e);
        }
    }

    public List<FieldSchema> getCachedFields(String databaseName, String tableName) {
        List<FieldSchema> result = new ArrayList<>();
        if (cacheEnabled()) {
            try {
                Jedis cache = getCacheFromPool();
                try {
                    String value = cache.get(directJoin(CACHE_TABLE_FIELDS_PROFIX, databaseName, SINGLE_DOT, tableName));
                    if (value == null) {
                        return result;
                    }
                    byte[] data = value.getBytes();
                    List<byte[]> tmp = deserilizeFromBytes(data);
                    for (byte[] b : tmp) {
                        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
                        FieldSchema fieldSchema = new FieldSchema();
                        deserializer.deserialize(fieldSchema, b);
                        result.add(fieldSchema);
                    }
                } finally {
                    cache.close();
                }
            } catch (Exception e) {
                LOG.warn("get cached table fields failed!", e);
            }
        }
        return result;
    }
}
