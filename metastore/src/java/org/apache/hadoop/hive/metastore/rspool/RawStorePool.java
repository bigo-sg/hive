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
    private static volatile RawStorePool rawStorePool;
    private Lock storeLock = new ReentrantLock();
    public static final Logger LOG = LoggerFactory.getLogger(RawStorePool.class);
    List<RawStoreContainer> rawStoreContainers = new LinkedList<>();
    public static final int RETRY_TIMES = 10;
    public static final String TRY_DB = "tmp";

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
    // This method can only be called for returning rs:
    // RawStorePool.getInstance().returnRawStore(rawStore)
    //
    public static RawStorePool getInstance() {
        if (rawStorePool == null) {
            throw new RuntimeException("instance had not been init yet!");
        }
        return rawStorePool;
    }

    public RawStoreContainer getRawStore() {
        RawStoreContainer rawStoreContainer = null;
        boolean ok = false;
        for (int i = 0; i < RETRY_TIMES; ++i) {
            storeLock.lock();
            try {
                if (rawStoreContainers.isEmpty()) {
                    rawStoreContainer = newRawStoreForConf(conf);
                } else {
                    rawStoreContainer = rawStoreContainers.get(0);
                    rawStoreContainers.remove(0);
                }
            } catch (MetaException e) {
                LOG.error("get RawStore failed!", e);
            } finally {
                storeLock.unlock();
            }
            long t1 = System.currentTimeMillis();
            try {
                ok = rawStoreContainer.getRawStore().runTestQuery();
                long t2 = System.currentTimeMillis();
                LOG.info("test jdbc connection cost " + (t2 - t1) + "ms");
                if (ok) break;
            } catch (Exception e) {
                long t2 = System.currentTimeMillis();
                LOG.warn("test jdbc connection cost " + (t2 - t1) + "ms");
            }
            try {
                rawStoreContainer.getRawStore().getDatabase(TRY_DB);
                ok = true;
                long t2 = System.currentTimeMillis();
                LOG.info("test jdbc connection cost " + (t2 - t1) + "ms");
                break;
            } catch (Exception e) {
                long t2 = System.currentTimeMillis();
                LOG.warn("test jdbc connection cost " + (t2 - t1) + "ms", e);
            }
            rawStoreContainer.getRawStore().shutdown();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                LOG.warn("sleep error", e1);
            }
        }
        if (!ok) {
            throw new RuntimeException("get RawStore failed after retry " + RETRY_TIMES + " times");
        }
        return rawStoreContainer;
    }

    public void returnRawStore(RawStoreContainer rawStoreContainer) {
        if (rawStoreContainers.size() > 1000 ||
                rawStoreContainer.getLivedTimeMillis() > 10 * 60 * 60 * 1000) {
            rawStoreContainer.getRawStore().shutdown();
            return;
        }
        storeLock.lock();
        try {
            rawStoreContainers.add(rawStoreContainer);
        } finally {
            storeLock.unlock();
        }
    }

    private static RawStoreContainer newRawStoreForConf(Configuration conf) throws MetaException {
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
                return new RawStoreContainer(rs);
            } catch (Exception e) {
                LOG.error("Unable to instantiate raw store directly in fastpath mode", e);
                throw new RuntimeException(e);
            }
        }
        return new RawStoreContainer(RawStoreProxy.getProxy(hiveConf, conf, rawStoreClassName, 0));
    }
}
