package org.apache.hadoop.hive.druid;

import org.junit.Test;

/**
 * @author litao@bigo.sg
 * @date 9/24/19 2:31 PM
 */
public class CuratorUtilsTests {

    @Test
    public void test() {
        String zkConnectionString = "localhost:2181";
        String coordinatorAddress = DruidCuratorUtils.getCoordinatorAddress(zkConnectionString);
        System.out.println(coordinatorAddress);
    }
}
