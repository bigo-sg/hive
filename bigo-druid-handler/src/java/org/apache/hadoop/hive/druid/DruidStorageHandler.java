/*
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
package org.apache.hadoop.hive.druid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHandler;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorage;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnectorConfig;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusher;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.io.DruidRecordWriter;
import org.apache.hadoop.hive.druid.security.KerberosHttpClient;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.druid.DruidStorageHandlerUtils.JSON_MAPPER;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({ "rawtypes" }) public class DruidStorageHandler extends DefaultHiveMetaHook
    implements HiveStorageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  private static final SessionState.LogHelper CONSOLE = new SessionState.LogHelper(LOG);

  public static final String SEGMENTS_DESCRIPTOR_DIR_NAME = "segmentsDescriptorDir";

  private static final String INTERMEDIATE_SEGMENT_DIR_NAME = "intermediateSegmentDir";

  private static final HttpClient HTTP_CLIENT;

  private static final String DRUID_INDEX_ID = "druid.indexing.id";

  private static final List<String> ALLOWED_ALTER_TYPES = ImmutableList.of("ADDPROPS", "DROPPROPS", "ADDCOLS");

  static {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      lifecycle.start();
    } catch (Exception e) {
      LOG.error("Issues with lifecycle start", e);
    }
    HTTP_CLIENT = makeHttpClient(lifecycle);
    ShutdownHookManager.addShutdownHook(lifecycle::stop);
  }

  private SQLMetadataConnector connector;

  private MetadataStorageTablesConfig druidMetadataStorageTablesConfig = null;

  private String uniqueId = null;

  private String rootWorkingDir = null;

  private Configuration conf;

  public DruidStorageHandler() {
  }

  @VisibleForTesting public DruidStorageHandler(SQLMetadataConnector connector,
      MetadataStorageTablesConfig druidMetadataStorageTablesConfig) {
    this.connector = connector;
    this.druidMetadataStorageTablesConfig = druidMetadataStorageTablesConfig;
  }

  @Override public Class<? extends InputFormat> getInputFormatClass() {
    return new InputFormat<Object, Object>(){
      @Override
      public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        throw new IOException("Hive don't support reading data from druid");
      }

      @Override
      public RecordReader<Object, Object> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        throw new IOException("Hive don't support reading data from druid");
      }
    }.getClass();
  }

  @Override public Class<? extends OutputFormat> getOutputFormatClass() {
    return DruidOutputFormat.class;
  }

  @Override public Class<? extends AbstractSerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override public HiveAuthorizationProvider getAuthorizationProvider() {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override public void preCreateTable(Table table) throws MetaException {
    if (!StringUtils.isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid");
    }
    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
    String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    if (dataSourceName != null) {
      // Already Existing datasource in Druid.
      return;
    }

    // create dataSourceName based on Hive Table name
    dataSourceName = TableName.getDbTable(table.getDbName(), table.getTableName());
    try {
      // NOTE: This just created druid_segments table in Druid metastore.
      // This is needed for the case when hive is started before any of druid services
      // and druid_segments table has not been created yet.
      getConnector().createSegmentTable();
    } catch (Exception e) {
      LOG.error("Exception while trying to create druid segments table", e);
      throw new MetaException(e.getMessage());
    }
    Collection<String>
        existingDataSources =
        DruidStorageHandlerUtils.getAllDataSourceNames(getConnector(), getDruidMetadataStorageTablesConfig());
    LOG.debug("pre-create data source with name {}", dataSourceName);
    // Check for existence of for the datasource we are going to create in druid_segments table.
    if (existingDataSources.contains(dataSourceName)) {
      throw new MetaException(String.format("Data source [%s] already existing", dataSourceName));
    }
    table.getParameters().put(Constants.DRUID_DATA_SOURCE, dataSourceName);
  }

  @Override public void rollbackCreateTable(Table table) {
    cleanWorkingDir();
  }

  @Override public void commitCreateTable(Table table) throws MetaException {
    // For CTAS queries when user has explicitly specified the datasource.
    // We will append the data to existing druid datasource.
    this.commitInsertTable(table, false);
  }

  /**
   * Creates metadata moves then commit the Segment's metadata to Druid metadata store in one TxN.
   *
   * @param table Hive table
   * @param overwrite true if it is an insert overwrite table.
   */
  private List<DataSegment> loadAndCommitDruidSegments(Table table, boolean overwrite, List<DataSegment> segmentsToLoad)
      throws IOException, CallbackFailedException {
    final String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    final String
        segmentDirectory =
        table.getParameters().get(DruidConstants.DRUID_SEGMENT_DIRECTORY) != null ?
            table.getParameters().get(DruidConstants.DRUID_SEGMENT_DIRECTORY) :
            HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY);

    final HdfsDataSegmentPusherConfig hdfsSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    List<DataSegment> publishedDataSegmentList;

    LOG.info(String.format("Moving [%s] Druid segments from staging directory [%s] to Deep storage [%s]",
        segmentsToLoad.size(),
        getStagingWorkingDir().toString(),
        segmentDirectory));
    hdfsSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    DataSegmentPusher dataSegmentPusher = new HdfsDataSegmentPusher(hdfsSegmentPusherConfig, getConf(), JSON_MAPPER);
    publishedDataSegmentList =
        DruidStorageHandlerUtils.publishSegmentsAndCommit(getConnector(),
            getDruidMetadataStorageTablesConfig(),
            dataSourceName,
            segmentsToLoad,
            overwrite,
            getConf(),
            dataSegmentPusher);
    return publishedDataSegmentList;
  }

  /**
   * This function checks the load status of Druid segments by polling druid coordinator.
   * @param segments List of druid segments to check for
   */
  private void checkLoadStatus(List<DataSegment> segments) {
    final String
        coordinatorAddress =
        DruidCuratorUtils.getCoordinatorAddress(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_ZOOKEEPER_DEFAULT_ADDRESS));
    int maxTries = getMaxRetryCount();

    CONSOLE.printInfo("checking load status from coordinator: " + coordinatorAddress);

    String coordinatorResponse;
    try {
      coordinatorResponse =
          RetryUtils.retry(() -> DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
              new Request(HttpMethod.GET, new URL(String.format("http://%s/status", coordinatorAddress))),
              new FullResponseHandler(Charset.forName("UTF-8"))).getContent(),
              input -> input instanceof IOException,
              maxTries);
    } catch (Exception e) {
      CONSOLE.printInfo("Will skip waiting for data loading, coordinator unavailable");
      return;
    }
    if (Strings.isNullOrEmpty(coordinatorResponse)) {
      CONSOLE.printInfo("Will skip waiting for data loading empty response from coordinator");
    }
    CONSOLE.printInfo(String.format("Waiting for the loading of [%s] segments", segments.size()));
    long passiveWaitTimeMs = HiveConf.getLongVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_PASSIVE_WAIT_TIME);
    Set<URL> urlsOfUnloadedSegments = segments
        .stream()
        .map(dataSegment -> {
          try {
            //Need to make sure that we are using segment identifier
            return new URL(String.format("http://%s/druid/coordinator/v1/datasources/%s/segments/%s",
                coordinatorAddress,
                dataSegment.getDataSource(),
                dataSegment.getId().toString()));
          } catch (MalformedURLException e) {
            Throwables.propagate(e);
          }
          return null;
        })
        .collect(Collectors.toSet());

    int numRetries = 0;
    while (numRetries++ < maxTries && !urlsOfUnloadedSegments.isEmpty()) {
      urlsOfUnloadedSegments = ImmutableSet.copyOf(Sets.filter(urlsOfUnloadedSegments, input -> {
        try {
          String
              result =
              DruidStorageHandlerUtils.getResponseFromCurrentLeader(getHttpClient(),
                  new Request(HttpMethod.GET, input),
                  new FullResponseHandler(Charset.forName("UTF-8"))).getContent();

          LOG.debug("Checking segment [{}] response is [{}]", input, result);
          return Strings.isNullOrEmpty(result);
        } catch (InterruptedException | ExecutionException e) {
          LOG.error(String.format("Error while checking URL [%s]", input), e);
          return true;
        }
      }));

      try {
        if (!urlsOfUnloadedSegments.isEmpty()) {
          Thread.sleep(passiveWaitTimeMs);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if (!urlsOfUnloadedSegments.isEmpty()) {
      // We are not Throwing an exception since it might be a transient issue that is blocking loading
      CONSOLE.printError(String.format("Wait time exhausted and we have [%s] out of [%s] segments not loaded yet",
          urlsOfUnloadedSegments.size(),
          segments.size()));
    }
  }

  @VisibleForTesting void deleteSegment(DataSegment segment) throws SegmentLoadingException {

    final Path path = DruidStorageHandlerUtils.getPath(segment);
    LOG.info("removing segment {}, located at path {}", segment.getId().toString(), path);

    try {
      if (path.getName().endsWith(".zip")) {

        final FileSystem fs = path.getFileSystem(getConf());

        if (!fs.exists(path)) {
          LOG.warn("Segment Path {} does not exist. It appears to have been deleted already.", path);
          return;
        }

        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        Path partitionNumDir = path.getParent();
        if (!fs.delete(partitionNumDir, true)) {
          throw new SegmentLoadingException("Unable to kill segment, failed to delete dir [%s]",
              partitionNumDir.toString());
        }

        //try to delete other directories if possible
        Path versionDir = partitionNumDir.getParent();
        if (safeNonRecursiveDelete(fs, versionDir)) {
          Path intervalDir = versionDir.getParent();
          if (safeNonRecursiveDelete(fs, intervalDir)) {
            Path dataSourceDir = intervalDir.getParent();
            safeNonRecursiveDelete(fs, dataSourceDir);
          }
        }
      } else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    } catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  private static boolean safeNonRecursiveDelete(FileSystem fs, Path path) {
    try {
      return fs.delete(path, false);
    } catch (Exception ex) {
      return false;
    }
  }

  @Override public void preDropTable(Table table) {
    // Nothing to do
  }

  @Override public void rollbackDropTable(Table table) {
    // Nothing to do
  }

  @Override public void commitDropTable(Table table, boolean deleteData) {
    String
        dataSourceName =
        Preconditions.checkNotNull(table.getParameters().get(Constants.DRUID_DATA_SOURCE), "DataSource name is null !");
    // Move MetaStoreUtils.isExternalTablePurge(table) calls to a common place for all StorageHandlers
    // deleteData flag passed down to StorageHandler should be true only if
    // MetaStoreUtils.isExternalTablePurge(table) returns true.
    if (deleteData && MetaStoreUtils.isExternalTablePurge(table)) {
      LOG.info("Dropping with purge all the data for data source {}", dataSourceName);
      List<DataSegment>
          dataSegmentList =
          DruidStorageHandlerUtils.getDataSegmentList(getConnector(),
              getDruidMetadataStorageTablesConfig(),
              dataSourceName);
      if (dataSegmentList.isEmpty()) {
        LOG.info("Nothing to delete for data source {}", dataSourceName);
        return;
      }
      for (DataSegment dataSegment : dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        } catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while deleting segment [%s]", dataSegment.getId().toString()), e);
        }
      }
    }
    if (DruidStorageHandlerUtils.disableDataSource(getConnector(),
        getDruidMetadataStorageTablesConfig(),
        dataSourceName)) {
      LOG.info("Successfully dropped druid data source {}", dataSourceName);
    }
  }

  @Override public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    LOG.debug("commit insert into table {} overwrite {}", table.getTableName(), overwrite);
    if (overwrite) {
      throw new IllegalArgumentException("insert overwrite is prohibit by druid handler," +
              " because it's a dangerous action");
    }

    final Boolean
            noDataError =
            Boolean.valueOf(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_NO_DATA_ERROR));

    try {
      // Check if there segments to load
      final Path segmentDescriptorDir = getSegmentDescriptorDir();
      final List<DataSegment> segmentsToLoad = fetchSegmentsMetadata(segmentDescriptorDir);
      final String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
      //No segments to load still need to honer overwrite
      if (segmentsToLoad.isEmpty() && overwrite) {
        //disable datasource
        //Case it is an insert overwrite we have to disable the existing Druid DataSource
        DruidStorageHandlerUtils.disableDataSource(getConnector(),
            getDruidMetadataStorageTablesConfig(),
            dataSourceName);
      } else if (!segmentsToLoad.isEmpty()) {
        // at this point we have Druid segments from reducers but we need to atomically
        // rename and commit to metadata
        // Moving Druid segments and committing to druid metadata as one transaction.
        checkLoadStatus(loadAndCommitDruidSegments(table, overwrite, segmentsToLoad));
      } else if (segmentsToLoad.isEmpty() && noDataError) {
        // No segments to load
        throw new SegmentLoadingException("No data insert, please check!");
      }
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    } catch (CallbackFailedException c) {
      LOG.error("Error while committing transaction to druid metadata storage", c);
      throw new MetaException(c.getCause().getMessage());
    } catch (SegmentLoadingException e) {
      throw new MetaException("No data insert, please check!");
    } finally {
      cleanWorkingDir();
    }
  }

  private List<DataSegment> fetchSegmentsMetadata(Path segmentDescriptorDir) throws IOException {
    LOG.info("segmentDescriptorDir {}, segmentDescriptorDir.toString()");
    if (!segmentDescriptorDir.getFileSystem(getConf()).exists(segmentDescriptorDir)) {
      LOG.info("Directory {} does not exist, ignore this if it is create statement or inserts of 0 rows,"
              + " no Druid segments to move, cleaning working directory {}",
          segmentDescriptorDir.toString(),
          getStagingWorkingDir().toString());
      return Collections.emptyList();
    }
    return DruidStorageHandlerUtils.getCreatedSegments(segmentDescriptorDir, getConf());
  }

  @Override public void preInsertTable(Table table, boolean overwrite) {

  }

  @Override public void rollbackInsertTable(Table table, boolean overwrite) {
    // do nothing
  }

  @Override public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    LOG.info("configureOutputJobProperties");
    jobProperties.put(Constants.DRUID_DATA_SOURCE, tableDesc.getTableName());
    jobProperties.put(DruidConstants.DRUID_SEGMENT_VERSION, new DateTime().toString());
    jobProperties.put(DruidConstants.DRUID_JOB_WORKING_DIRECTORY, getStagingWorkingDir().toString());
    // DruidOutputFormat will write segments in an intermediate directory
    jobProperties.put(DruidConstants.DRUID_SEGMENT_INTERMEDIATE_DIRECTORY,
        getIntermediateSegmentDir().toString());
  }

  @Override public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

    LOG.info("setting job configuration");
    jobConf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, Boolean.FALSE);
    jobConf.set(HiveConf.ConfVars.HIVESPECULATIVEEXECREDUCERS.toString(), Boolean.FALSE.toString());
    jobConf.set(DruidConstants.DRUID_SEGMENT_INTERMEDIATE_DIRECTORY,
            getIntermediateSegmentDir().toString());

    if (UserGroupInformation.isSecurityEnabled()) {
      // AM can not do Kerberos Auth so will do the input split generation in the HS2
      LOG.debug("Setting {} to {} to enable split generation on HS2",
          HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(),
          Boolean.FALSE.toString());
      jobConf.set(HiveConf.ConfVars.HIVE_AM_SPLIT_GENERATION.toString(), Boolean.FALSE.toString());
    }
    try {
      DruidStorageHandlerUtils.addDependencyJars(jobConf, DruidRecordWriter.class);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

  private String getUniqueId() {
    if (uniqueId == null) {
      uniqueId = getConf().get(DRUID_INDEX_ID);
      if (uniqueId == null) {
        uniqueId = Preconditions.checkNotNull(Strings.emptyToNull(HiveConf.getVar(getConf(), HiveConf.ConfVars.HIVEQUERYID)),
          "Hive query id is null");
      }
      getConf().set(DRUID_INDEX_ID, uniqueId);
      LOG.info("get uniqueId {}", uniqueId);
    }
    return uniqueId;
  }

  private Path getStagingWorkingDir() {
    return new Path(getRootWorkingDir(), makeStagingName());
  }

  private MetadataStorageTablesConfig getDruidMetadataStorageTablesConfig() {
    if (druidMetadataStorageTablesConfig != null) {
      return druidMetadataStorageTablesConfig;
    }
    final String base = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_BASE);
    druidMetadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase(base);
    return druidMetadataStorageTablesConfig;
  }

  private SQLMetadataConnector getConnector() {
    return Suppliers.memoize(this::buildConnector).get();
  }

  private SQLMetadataConnector buildConnector() {

    if (connector != null) {
      return connector;
    }

    final String dbType = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_TYPE);
    final String username = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_USERNAME);
    final String password = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_PASSWORD);
    final String uri = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_METADATA_DB_URI);
    LOG.debug("Supplying SQL Connector with DB type {}, URI {}, User {}", dbType, uri, username);
    @SuppressWarnings("Guava") final Supplier<MetadataStorageConnectorConfig>
        storageConnectorConfigSupplier =
        Suppliers.ofInstance(new MetadataStorageConnectorConfig() {
          @Override public String getConnectURI() {
            return uri;
          }

          @Override public String getUser() {
            return Strings.emptyToNull(username);
          }

          @Override public String getPassword() {
            return Strings.emptyToNull(password);
          }
        });
    switch (dbType) {
    case "mysql":
      connector =
          new MySQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig()),
              new MySQLConnectorConfig());
      break;
    case "postgresql":
      connector =
          new PostgreSQLConnector(storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig()),
                  new PostgreSQLConnectorConfig()
          );

      break;
    case "derby":
      connector =
          new DerbyConnector(new DerbyMetadataStorage(storageConnectorConfigSupplier.get()),
              storageConnectorConfigSupplier,
              Suppliers.ofInstance(getDruidMetadataStorageTablesConfig()));
      break;
    default:
      throw new IllegalStateException(String.format("Unknown metadata storage type [%s]", dbType));
    }
    return connector;
  }

  @VisibleForTesting String makeStagingName() {
    return ".staging-".concat(getUniqueId().replace(":", ""));
  }

  private Path getSegmentDescriptorDir() {
    String stagingWorkingDir = getConf().get(DruidConstants.DRUID_JOB_WORKING_DIRECTORY);
    if (stagingWorkingDir == null) {
      return new Path(getStagingWorkingDir(), SEGMENTS_DESCRIPTOR_DIR_NAME);
    } else {
      return new Path(stagingWorkingDir, SEGMENTS_DESCRIPTOR_DIR_NAME);
    }
  }

  private Path getIntermediateSegmentDir() {
    Path stagingPath = getStagingWorkingDir();
    LOG.info("getting staging path of {}", stagingPath.toString());
    return new Path(stagingPath, INTERMEDIATE_SEGMENT_DIR_NAME);
  }

  private void cleanWorkingDir() {
    final FileSystem fileSystem;
    try {
      fileSystem = getStagingWorkingDir().getFileSystem(getConf());
      fileSystem.delete(getStagingWorkingDir(), true);
    } catch (IOException e) {
      LOG.error("Got Exception while cleaning working directory", e);
    }
  }

  private String getRootWorkingDir() {
    if (Strings.isNullOrEmpty(rootWorkingDir)) {
      rootWorkingDir = HiveConf.getVar(getConf(), HiveConf.ConfVars.DRUID_WORKING_DIR);
    }
    return rootWorkingDir;
  }

  private static HttpClient makeHttpClient(Lifecycle lifecycle) {
    final int
        numConnection =
        HiveConf.getIntVar(SessionState.getSessionConf(), HiveConf.ConfVars.HIVE_DRUID_NUM_HTTP_CONNECTION);
    final Period
        readTimeout =
        new Period(HiveConf.getVar(SessionState.getSessionConf(), HiveConf.ConfVars.HIVE_DRUID_HTTP_READ_TIMEOUT));
    LOG.info("Creating Druid HTTP client with {} max parallel connections and {}ms read timeout",
        numConnection,
        readTimeout.toStandardDuration().getMillis());

    final HttpClient
        httpClient =
        HttpClientInit.createClient(HttpClientConfig.builder()
            .withNumConnections(numConnection)
            .withReadTimeout(new Period(readTimeout).toStandardDuration())
            .build(), lifecycle);
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("building Kerberos Http Client");
      return new KerberosHttpClient(httpClient);
    }
    return httpClient;
  }

  public static HttpClient getHttpClient() {
    return HTTP_CLIENT;
  }

  private int getMaxRetryCount() {
    return HiveConf.getIntVar(getConf(), HiveConf.ConfVars.HIVE_DRUID_MAX_TRIES);
  }
}
