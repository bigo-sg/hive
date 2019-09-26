/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.druid;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * DruidCuratorUtils
 */

public class DruidCuratorUtils {
    private static final Logger log = new Logger(DruidCuratorUtils.class);
    private static final ConcurrentMap<String, CuratorFramework> CACHE = new ConcurrentHashMap<String, CuratorFramework>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                for (CuratorFramework curator : CACHE.values()) {
                    try {
                        curator.close();
                    }
                    catch (Exception ex) {
                        log.error("Error at closing " + curator, ex);
                    }
                }
                CACHE.clear();
            }
        }));
    }

    public static CuratorFramework getCurator(String config) {
        CuratorFramework curator = CACHE.get(config);
        if (curator == null) {
            synchronized (DruidCuratorUtils.class) {
                curator = CACHE.get(config);
                if (curator == null) {
                    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
                    curator = CuratorFrameworkFactory.builder()
                            .connectString(config)
                            .sessionTimeoutMs(5000)
                            .connectionTimeoutMs(5000)
                            .retryPolicy(retryPolicy)
                            .build();
                    curator.start();
                    CACHE.put(config, curator);
                    if (CACHE.size() > 1) {
                        log.warn("More than one singleton exist");
                    }
                }
            }
        }
        return curator;
    }

    public static String getCoordinatorAddress(String config) {
        final CuratorFramework curator = getCurator(config);
        String leaderCoorZkPath = "/druid/discovery/druid:coordinator";
        String coordinator = "";
        try {
            String children = curator.getChildren().forPath(leaderCoorZkPath).get(0);

            byte[] bytes = curator.getData().forPath(leaderCoorZkPath + "/" + children);
            String payload = new String(bytes, Charset.forName("UTF-8"));

            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(payload).getAsJsonObject();
            String address = StringUtils.strip(jsonObject.get("address").toString(), "\"");
            int port = jsonObject.get("port").getAsInt();
            coordinator = address + ":" + port;
        } catch (Exception e) {
            log.error("get leader coordinator failed!", e);
        }
        return coordinator;
    }
}
