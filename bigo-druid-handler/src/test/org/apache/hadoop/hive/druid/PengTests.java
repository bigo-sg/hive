package org.apache.hadoop.hive.druid;

import org.apache.druid.segment.IndexMergerV9;
import org.junit.Test;

/**
 * @author litao@bigo.sg
 * @date 9/24/19 2:31 PM
 */
public class PengTests {

    @Test
    public void test() {
        IndexMergerV9 aa = DruidStorageHandlerUtils.INDEX_MERGER_V9;
        System.out.println(aa);

        new DruidStorageHandlerUtils();
    }
}
