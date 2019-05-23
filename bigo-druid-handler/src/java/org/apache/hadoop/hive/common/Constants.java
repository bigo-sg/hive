/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common;

public class Constants {
  /* Constants for LLAP */
  public static final String LLAP_LOGGER_NAME_QUERY_ROUTING = "query-routing";
  public static final String LLAP_LOGGER_NAME_CONSOLE = "console";
  public static final String LLAP_LOGGER_NAME_RFA = "RFA";

  /* Constants for Druid storage handler */
  public static final String DRUID_HIVE_STORAGE_HANDLER_ID =
          "org.apache.hadoop.hive.druid.DruidStorageHandler";
  public static final String DRUID_HIVE_OUTPUT_FORMAT =
          "org.apache.hadoop.hive.druid.io.DruidOutputFormat";
  public static final String DRUID_DATA_SOURCE = "druid.datasource";

  /*
   * default 12,log2 of K that is the number of buckets in the sketch,
   * parameter that controls the size and the accuracy.
   * Must be a power of 2 from 4 to 21 inclusively.
   * */
  public static final String DRUID_HLL_LG_K = "druid.hll.lg.k";
  /*
   * HLL_4(default),The type of the target HLL sketch. Must be "HLL_4", "HLL_6" or "HLL_8"
   * */
  public static final String DRUID_HLL_TGT_TYPE = "druid.hll.tgt.type";
  /*fields used to calculate count distinct with hll algorithm,for example uid,vid*/
  public static final String DRUID_HLL_SKETCH_FIELDS = "druid.hll.fields";
  /*fields used to calculate count distinct with theta algorithm,for example uid,vid*/
  public static final String DRUID_THETA_SKETCH_FIELDS = "druid.theta.fields";
  /**/
  public static final String DRUID_MULTI_VALUE_DIMENSIONS = "druid.multi.value.dimensions";
  /*exclude some of the string type dimensions,for example some fields with type of
   *string that you only want to agg as count distinct*/
  public static final String DRUID_EXCLUDED_DIMENSIONS = "druid.excluded.dimensions";
  public static final String DRUID_SEGMENT_GRANULARITY = "druid.segment.granularity";
  public static final String DRUID_QUERY_GRANULARITY = "druid.query.granularity";
  public static final String DRUID_TIMESTAMP_GRANULARITY_COL_NAME = "__time_granularity";
  public static final String DRUID_QUERY_JSON = "druid.query.json";
  public static final String DRUID_QUERY_TYPE = "druid.query.type";
  public static final String DRUID_QUERY_FETCH = "druid.query.fetch";
  public static final String DRUID_SEGMENT_DIRECTORY = "druid.storage.storageDirectory";
  public static final String DRUID_SEGMENT_VERSION = "druid.segment.version";
  public static final String DRUID_JOB_WORKING_DIRECTORY = "druid.job.workingDirectory";

  public static final String HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR = "HIVE_JOB_CREDSTORE_PASSWORD";
  public static final String HADOOP_CREDENTIAL_PASSWORD_ENVVAR = "HADOOP_CREDSTORE_PASSWORD";
  public static final String HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG = "hadoop.security.credential.provider.path";
}
