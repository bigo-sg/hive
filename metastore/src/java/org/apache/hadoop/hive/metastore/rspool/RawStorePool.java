package org.apache.hadoop.hive.metastore.rspool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tangyun@bigo.sg
 * @date 3/30/20 5:23 PM
 */
public class RawStorePool {

    private static Lock lock = new ReentrantLock();
    private static RawStorePool rawStorePool;
    private Lock storeLock = new ReentrantLock();
    public static final Logger LOG = LoggerFactory.getLogger(RawStorePool.class);
    List<RawStore> rawStores = new LinkedList<>();

    Configuration conf;
    private RawStorePool(Configuration conf) {
        this.conf = conf;
    }

    public static RawStorePool getInstance(Configuration conf) {
        if (rawStorePool == null) {
            lock.lock();
            try {
                if (rawStorePool == null) {
                    rawStorePool = new RawStorePool(conf);
                }
            } finally {
                lock.unlock();
            }
        }
        return rawStorePool;
    }

    //
    // this method only can call when return rs:
    // RawStorePool.getInstance().returnRawStore(rawStore)
    //
    public static RawStorePool getInstance() {
        if (rawStorePool == null) {
            throw new RuntimeException("instance had not been init yet!");
        }
        return rawStorePool;
    }

    public RawStore getRawStore() {
        RawStore rawStore = null;
        storeLock.lock();
        try {
            if (rawStores.isEmpty()) {
                rawStore = newRawStoreForConf(conf);
            } else {
                rawStore = rawStores.get(0);
                rawStores.remove(0);
            }
        } catch (MetaException e) {
            LOG.error("get RawStore failed!");
        } finally {
            storeLock.unlock();
        }
        return rawStore;
    }

    public void returnRawStore(RawStore rawStore) {
        storeLock.lock();
        try {
            rawStores.add(rawStore);
        } finally {
            storeLock.unlock();
        }
    }

    private static RawStore newRawStoreForConf(Configuration conf) throws MetaException {
        HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
        String rawStoreClassName = hiveConf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL);
        LOG.info("Opening raw store with implementation class:" + rawStoreClassName);
        if (hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH)) {
            LOG.info("Fastpath, skipping raw store proxy");
            try {
                RawStore rs =
                        ((Class<? extends RawStore>) MetaStoreUtils.getClass(rawStoreClassName))
                                .newInstance();
                rs.setConf(hiveConf);
                return rs;
            } catch (Exception e) {
                LOG.error("Unable to instantiate raw store directly in fastpath mode", e);
                throw new RuntimeException(e);
            }
        }
        return RawStoreProxy.getProxy(hiveConf, conf, rawStoreClassName, 0);
    }

    public Configuration getConf() {
        return conf;
    }
}
