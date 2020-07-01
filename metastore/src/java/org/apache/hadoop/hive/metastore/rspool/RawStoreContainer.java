package org.apache.hadoop.hive.metastore.rspool;

import org.apache.hadoop.hive.metastore.RawStore;

/**
 * @author tangyun@bigo.sg
 * @date 3/31/20 11:12 AM
 */
public class RawStoreContainer {

    private final RawStore rawStore;
    private final long birthTime = System.currentTimeMillis();
    public RawStoreContainer(RawStore rawStore) {
        this.rawStore = rawStore;
    }

    public long getLivedTimeMillis() {
        return System.currentTimeMillis() - birthTime;
    }

    public RawStore getRawStore() {
        return rawStore;
    }
}
