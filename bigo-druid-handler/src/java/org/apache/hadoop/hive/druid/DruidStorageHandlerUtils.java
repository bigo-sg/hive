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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.gson.JsonObject;
import org.apache.druid.data.input.impl.*;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHandler;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.aggregation.datasketches.hll.*;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.oldapi.OldApiSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.oldapi.OldSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.expression.*;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusher;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.druid.json.AvroParseSpec;
import org.apache.hadoop.hive.druid.json.AvroStreamInputRowParser;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.GregorianChronology;
import org.joda.time.chrono.ISOChronology;
import org.skife.jdbi.v2.*;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_DRUID_DEPEND_JARS;

/**
 * Utils class for Druid storage handler.
 */
public final class DruidStorageHandlerUtils {
  DruidStorageHandlerUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandlerUtils.class);
  private static final SessionState.LogHelper CONSOLE = new SessionState.LogHelper(LOG);

  private static final int NUM_RETRIES = 8;
  private static final int SECONDS_BETWEEN_RETRIES = 2;
  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB
  private static final int DEFAULT_STREAMING_RESULT_SIZE = 100;
  private static final String SMILE_CONTENT_TYPE = "application/x-jackson-smile";

  static final String INDEX_ZIP = "index.zip";
  private static final String DESCRIPTOR_JSON = "descriptor.json";
  private static final Interval
      DEFAULT_INTERVAL =
      new Interval(new DateTime("1900-01-01", ISOChronology.getInstanceUTC()),
          new DateTime("3000-01-01", ISOChronology.getInstanceUTC())).withChronology(ISOChronology.getInstanceUTC());

  /**
   * Mapper to use to serialize/deserialize Druid objects (JSON).
   */
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  /**
   * Mapper to use to serialize/deserialize Druid objects (SMILE).
   */
  public static final ObjectMapper SMILE_MAPPER = new DefaultObjectMapper(new SmileFactory());
  private static final int DEFAULT_MAX_TRIES = 10;

  private static Field getField(Class clazz, String fieldName)
          throws NoSuchFieldException {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      Class superClass = clazz.getSuperclass();
      if (superClass == null) {
        throw e;
      } else {
        return getField(superClass, fieldName);
      }
    }
  }

  static {
    // This is needed for serde of PagingSpec as it uses JacksonInject for injecting SelectQueryConfig
    InjectableValues.Std
        injectableValues =
        new InjectableValues.Std().addValue(SelectQueryConfig.class, new SelectQueryConfig(false))
            // Expressions macro table used when we deserialize the query from calcite plan
            .addValue(ExprMacroTable.class,
                new ExprMacroTable(ImmutableList.of(new LikeExprMacro(),
                    new RegexpExtractExprMacro(),
                    new TimestampCeilExprMacro(),
                    new TimestampExtractExprMacro(),
                    new TimestampFormatExprMacro(),
                    new TimestampParseExprMacro(),
                    new TimestampShiftExprMacro(),
                    new TimestampFloorExprMacro(),
                    new TrimExprMacro.BothTrimExprMacro(),
                    new TrimExprMacro.LeftTrimExprMacro(),
                    new TrimExprMacro.RightTrimExprMacro())))
            .addValue(ObjectMapper.class, JSON_MAPPER)
            .addValue(DataSegment.PruneLoadSpecHolder.class, DataSegment.PruneLoadSpecHolder.DEFAULT);

    JSON_MAPPER.setInjectableValues(injectableValues);
    SMILE_MAPPER.setInjectableValues(injectableValues);
    // Register the shard sub type to be used by the mapper
    JSON_MAPPER.registerSubtypes(new NamedType(LinearShardSpec.class, "linear"));
    JSON_MAPPER.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));
    JSON_MAPPER.registerSubtypes(new NamedType(AvroParseSpec.class, "avro"));
    SMILE_MAPPER.registerSubtypes(new NamedType(AvroParseSpec.class, "avro"));
    JSON_MAPPER.registerSubtypes(new NamedType(AvroStreamInputRowParser.class, "avro_stream"));
    SMILE_MAPPER.registerSubtypes(new NamedType(AvroStreamInputRowParser.class, "avro_stream"));
    // set the timezone of the object mapper
    // THIS IS NOT WORKING workaround is to set it as part of java opts -Duser.timezone="UTC"
    JSON_MAPPER.setTimeZone(TimeZone.getTimeZone("UTC"));

    // register extensional modules
    JSON_MAPPER.registerModules(new HllSketchModule().getJacksonModules());
    JSON_MAPPER.registerModules(new DoublesSketchModule().getJacksonModules());
    JSON_MAPPER.registerModules(new SketchModule().getJacksonModules());
    JSON_MAPPER.registerModules(new ArrayOfDoublesSketchModule().getJacksonModules());

    try {
      StdSubtypeResolver subtypeResolver = (StdSubtypeResolver) JSON_MAPPER.getSubtypeResolver();
      Field myField = getField(subtypeResolver.getClass(), "_registeredSubtypes");
      myField.setAccessible(true);
      LinkedHashSet<NamedType> registeredSubtypes = (LinkedHashSet<NamedType>) myField.get(subtypeResolver);
      for (NamedType namedType : registeredSubtypes) {
        LOG.info("registered subtypes: " + namedType.toString());
      }
    } catch (Exception e) {
      LOG.warn("fetch registered subtypes failed!", e);
    }

    try {
      // No operation emitter will be used by some internal druid classes.
      EmittingLogger.registerEmitter(new ServiceEmitter("druid-hive-indexer",
          InetAddress.getLocalHost().getHostName(),
          new NoopEmitter()));
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Used by druid to perform IO on indexes.
   */
  public static final IndexIO INDEX_IO = new IndexIO(JSON_MAPPER, new DruidProcessingConfig() {
    @Override public String getFormatString() {
      return "%s-%s";
    }
  });

  /**
   * Used by druid to merge indexes.
   */
  public static final IndexMergerV9
      INDEX_MERGER_V9 =
      new IndexMergerV9(JSON_MAPPER, DruidStorageHandlerUtils.INDEX_IO, TmpFileSegmentWriteOutMediumFactory.instance());

  /**
   * Generic Interner implementation used to read segments object from metadata storage.
   */
  private static final Interner<DataSegment> DATA_SEGMENT_INTERNER = Interners.newWeakInterner();

  /**
   * Method that creates a request for Druid query using SMILE format.
   *
   * @param address of the host target.
   * @param query   druid query.
   * @return Request object to be submitted.
   */
  public static Request createSmileRequest(String address, org.apache.druid.query.Query query) {
    try {
      return new Request(HttpMethod.POST, new URL(String.format("%s/druid/v2/", "http://" + address))).setContent(
          SMILE_MAPPER.writeValueAsBytes(query)).setHeader(HttpHeaders.Names.CONTENT_TYPE, SMILE_CONTENT_TYPE);
    } catch (MalformedURLException e) {
      LOG.error("URL Malformed  address {}", address);
      throw new RuntimeException(e);
    } catch (JsonProcessingException e) {
      LOG.error("can not Serialize the Query [{}]", query.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * Method that submits a request to an Http address and retrieves the result.
   * The caller is responsible for closing the stream once it finishes consuming it.
   *
   * @param client  Http Client will be used to submit request.
   * @param request Http request to be submitted.
   * @return response object.
   * @throws IOException in case of request IO error.
   */
  public static InputStream submitRequest(HttpClient client, Request request) throws IOException {
    try {
      return client.go(request, new InputStreamResponseHandler()).get();
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e.getCause());
    }

  }

  static FullResponseHolder getResponseFromCurrentLeader(HttpClient client,
      Request request,
      FullResponseHandler fullResponseHandler) throws ExecutionException, InterruptedException {
    FullResponseHolder responseHolder = client.go(request, fullResponseHandler).get();
    if (HttpResponseStatus.TEMPORARY_REDIRECT.equals(responseHolder.getStatus())) {
      String redirectUrlStr = responseHolder.getResponse().headers().get("Location");
      LOG.debug("Request[%s] received redirect response to location [%s].", request.getUrl(), redirectUrlStr);
      final URL redirectUrl;
      try {
        redirectUrl = new URL(redirectUrlStr);
      } catch (MalformedURLException ex) {
        throw new ExecutionException(String.format(
            "Malformed redirect location is found in response from url[%s], new location[%s].",
            request.getUrl(),
            redirectUrlStr), ex);
      }
      responseHolder = client.go(withUrl(request, redirectUrl), fullResponseHandler).get();
    }
    return responseHolder;
  }

  private static Request withUrl(Request old, URL url) {
    Request req = new Request(old.getMethod(), url);
    req.addHeaderValues(old.getHeaders());
    if (old.hasContent()) {
      req.setContent(old.getContent());
    }
    return req;
  }

  /**
   * @param taskDir path to the  directory containing the segments descriptor info
   *                the descriptor path will be
   *                ../workingPath/task_id/{@link DruidStorageHandler#SEGMENTS_DESCRIPTOR_DIR_NAME}/*.json
   * @param conf    hadoop conf to get the file system
   * @return List of DataSegments
   * @throws IOException can be for the case we did not produce data.
   */
  public static List<DataSegment> getCreatedSegments(Path taskDir, Configuration conf) throws IOException {
    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();
    FileSystem fs = taskDir.getFileSystem(conf);
    FileStatus[] fss;
    fss = fs.listStatus(taskDir);
    for (FileStatus fileStatus : fss) {
      final DataSegment segment = JSON_MAPPER.readValue((InputStream) fs.open(fileStatus.getPath()), DataSegment.class);
      publishedSegmentsBuilder.add(segment);
    }
    return publishedSegmentsBuilder.build();
  }

  /**
   * Writes to filesystem serialized form of segment descriptor if an existing file exists it will try to replace it.
   *
   * @param outputFS       filesystem.
   * @param segment        DataSegment object.
   * @param descriptorPath path.
   * @throws IOException in case any IO issues occur.
   */
  public static void writeSegmentDescriptor(final FileSystem outputFS,
      final DataSegment segment,
      final Path descriptorPath) throws IOException {
    final DataPusher descriptorPusher = (DataPusher) RetryProxy.create(DataPusher.class, () -> {
      if (outputFS.exists(descriptorPath)) {
        if (!outputFS.delete(descriptorPath, false)) {
          throw new IOException(String.format("Failed to delete descriptor at [%s]", descriptorPath));
        }
      }
      try (final OutputStream descriptorOut = outputFS.create(descriptorPath, true, DEFAULT_FS_BUFFER_SIZE)) {
        JSON_MAPPER.writeValue(descriptorOut, segment);
        descriptorOut.flush();
      }
    }, RetryPolicies.exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS));
    descriptorPusher.push();
  }

  /**
   * @param connector                   SQL metadata connector to the metadata storage
   * @param metadataStorageTablesConfig Table config
   * @return all the active data sources in the metadata storage
   */
  static Collection<String> getAllDataSourceNames(SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig) {
    return connector.getDBI()
        .withHandle((HandleCallback<List<String>>) handle -> handle.createQuery(String.format(
            "SELECT DISTINCT(datasource) FROM %s WHERE used = true",
            metadataStorageTablesConfig.getSegmentsTable()))
            .fold(Lists.<String>newArrayList(),
                (druidDataSources, stringObjectMap, foldController, statementContext) -> {
                  druidDataSources.add(MapUtils.getString(stringObjectMap, "datasource"));
                  return druidDataSources;
                }));
  }

  /**
   * @param connector                   SQL connector to metadata
   * @param metadataStorageTablesConfig Tables configuration
   * @param dataSource                  Name of data source
   * @return true if the data source was successfully disabled false otherwise
   */
  static boolean disableDataSource(SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig,
      final String dataSource) {
    try {
      if (!getAllDataSourceNames(connector, metadataStorageTablesConfig).contains(dataSource)) {
        LOG.warn("Cannot delete data source {}, does not exist", dataSource);
        return false;
      }

      connector.getDBI().withHandle((HandleCallback<Void>) handle -> {
        disableDataSourceWithHandle(handle, metadataStorageTablesConfig, dataSource);
        return null;
      });

    } catch (Exception e) {
      LOG.error(String.format("Error removing dataSource %s", dataSource), e);
      return false;
    }
    return true;
  }

  public static List<Interval> getIntervalsToOverWrite(List<DataSegment> segmentsToOverWrite) {

    Map<Long, Long> intervalPoints = new TreeMap<>();
    List<Interval> intervals = new ArrayList<>();

    segmentsToOverWrite.stream().forEach(dataSegment ->
            intervalPoints.put(dataSegment.getInterval().getStartMillis(),
            dataSegment.getInterval().getEndMillis()));

    AtomicReference<Interval> interval = new AtomicReference<>();

    intervalPoints.forEach((k, v) -> {
      if (interval.get() == null) {
        interval.set(new Interval(k, v).withChronology(GregorianChronology.getInstance(DateTimeZone.UTC)));
      }
      interval.set(interval.get().withEndMillis(v));
      if (!intervalPoints.containsKey(v)) {
        intervals.add(interval.get());
        interval.set(null);
      }
    });
    return intervals;
  }

  public static void disableOverLappedSegment(
          List<DataSegment> segmentsToOverWrite, final Handle handle, String tableName) {

    String dataSource = null;
    for (DataSegment dataSegment: segmentsToOverWrite) {
      if (dataSource == null) {
        dataSource = dataSegment.getDataSource();
      }
      if (dataSource != null) {
        break;
      }
    }
    List<Interval> intervals = getIntervalsToOverWrite(segmentsToOverWrite);
    String sqlTemplate = String.format(
            "UPDATE %1$s SET used=false WHERE dataSource=:dataSource AND start>=:start AND end<=:end",
            tableName);
    LOG.info("disable overlapped segments:" + sqlTemplate);
    final PreparedBatch batch = handle.prepareBatch(sqlTemplate);

    for (Interval interval: intervals) {
      batch.add(new ImmutableMap.Builder<String, Object>().put("dataSource", dataSource)
              .put("start", interval.getStart().toString())
              .put("end", interval.getEnd().toString())
              .build());
      LOG.info("Disabled {}---->{}::{}----{}", interval.getStart(), interval.getEnd(),
              interval.getStartMillis(), interval.getEndMillis());
    }
    batch.execute();
  }

   /**
   *    * First computes the segments timeline to accommodate new segments for insert into case.
   *    * Then moves segments to druid deep storage with updated metadata/version.
   *    * ALL IS DONE IN ONE TRANSACTION
   *    *
   *    * @param connector                   DBI connector to commit
   *    * @param metadataStorageTablesConfig Druid metadata tables definitions
   *    * @param dataSource                  Druid datasource name
   *    * @param segments                    List of segments to move and commit to metadata
   *    * @param overwrite                   if it is an insert overwrite
   *    * @param conf                       Configuration
   * @param dataSegmentPusher           segment pusher
   * @return List of successfully published Druid segments.
   * This list has the updated versions and metadata about segments after move and timeline sorting
   * @throws CallbackFailedException in case the connector can not add the segment to the DB.
   */
  @SuppressWarnings("unchecked") static List<DataSegment> publishSegmentsAndCommit(final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig,
      final String dataSource,
      final List<DataSegment> segments,
      boolean overwrite,
      Configuration conf,
      DataSegmentPusher dataSegmentPusher) throws CallbackFailedException {
    return connector.getDBI().inTransaction((handle, transactionStatus) -> {
      // We create the timeline for the existing and new segments
      VersionedIntervalTimeline<String, DataSegment> timeline;
      if (overwrite) {
        // If we are overwriting, we disable existing sources
        disableDataSourceWithHandle(handle, metadataStorageTablesConfig, dataSource);

        // When overwriting, we just start with empty timeline,
        // as we are overwriting segments with new versions
        timeline = new VersionedIntervalTimeline<>(Ordering.natural());
      } else {
        // Append Mode
        if (segments.isEmpty()) {
          // If there are no new segments, we can just bail out
          return Collections.EMPTY_LIST;
        }
        // Otherwise, build a timeline of existing segments in metadata storage
        Interval
            indexedInterval =
            JodaUtils.umbrellaInterval(segments.stream().map(DataSegment::getInterval).collect(Collectors.toList()));
        LOG.info("Building timeline for umbrella Interval [{}]", indexedInterval);
        timeline = getTimelineForIntervalWithHandle(handle, dataSource, indexedInterval, metadataStorageTablesConfig);
      }

      final List<DataSegment> finalSegmentsToPublish = Lists.newArrayList();
      for (DataSegment segment : segments) {
        List<TimelineObjectHolder<String, DataSegment>> existingChunks = timeline.lookup(segment.getInterval());
        if (existingChunks.size() > 1) {
          // Not possible to expand since we have more than one chunk with a single segment.
          // This is the case when user wants to append a segment with coarser granularity.
          // case metadata storage has segments with granularity HOUR and segments to append have DAY granularity.
          // Druid shard specs does not support multiple partitions for same interval with different granularity.
          throw new IllegalStateException(String.format(
              "Cannot allocate new segment for dataSource[%s], interval[%s], already have [%,d] chunks. "
                  + "Not possible to append new segment.",
              dataSource,
              segment.getInterval(),
              existingChunks.size()));
        }
        // Find out the segment with latest version and maximum partition number
        SegmentIdWithShardSpec max = null;
        final ShardSpec newShardSpec;
        final String newVersion;
        if (!existingChunks.isEmpty()) {
          // Some existing chunk, Find max
          TimelineObjectHolder<String, DataSegment> existingHolder = Iterables.getOnlyElement(existingChunks);
          for (PartitionChunk<DataSegment> existing : existingHolder.getObject()) {
            if (max == null || max.getShardSpec().getPartitionNum() < existing.getObject()
                .getShardSpec()
                .getPartitionNum()) {
              max = SegmentIdWithShardSpec.fromDataSegment(existing.getObject());
            }
          }
        }

        if (max == null) {
          // No existing shard present in the database, use the current version.
          newShardSpec = segment.getShardSpec();
          newVersion = segment.getVersion();
        } else {
          // use version of existing max segment to generate new shard spec
          newShardSpec = getNextPartitionShardSpec(max.getShardSpec());
          newVersion = max.getVersion();
        }
        DataSegment
            publishedSegment =
            publishSegmentWithShardSpec(segment,
                newShardSpec,
                newVersion,
                getPath(segment).getFileSystem(conf),
                dataSegmentPusher);
        finalSegmentsToPublish.add(publishedSegment);
        timeline.add(publishedSegment.getInterval(),
            publishedSegment.getVersion(),
            publishedSegment.getShardSpec().createChunk(publishedSegment));

      }

      // Disabled overlapped segement
      disableOverLappedSegment(finalSegmentsToPublish, handle, metadataStorageTablesConfig.getSegmentsTable());

      // Publish new segments to metadata storage
      final PreparedBatch
          batch =
          handle.prepareBatch(String.format(
              "INSERT INTO %1$s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
                  + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
              metadataStorageTablesConfig.getSegmentsTable())

          );

      CONSOLE.printInfo(String.format("size of finalSegmentsToPublish: %d", finalSegmentsToPublish.size()));
      ArrayList<String> segmentsInfo = new ArrayList<>();
      String dataTime = new DateTime().toString();
      long size = 0;
      for (final DataSegment segment : finalSegmentsToPublish) {

        batch.add(new ImmutableMap.Builder<String, Object>().put("id", segment.getId().toString())
            .put("dataSource", segment.getDataSource())
            .put("created_date", dataTime)
            .put("start", segment.getInterval().getStart().toString())
            .put("end", segment.getInterval().getEnd().toString())
            .put("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
            .put("version", segment.getVersion())
            .put("used", true)
            .put("payload", JSON_MAPPER.writeValueAsBytes(segment))
            .build());

        size += segment.getSize();

        LOG.info("Published {}:{}---->{}:{}---{}", segment.getId().toString(),
                segment.getInterval().getStart(), segment.getInterval().getEnd(),
                segment.getInterval().getStartMillis(),
                segment.getInterval().getEndMillis());
      }
      batch.execute();

      // send segment info to kafka
      try {
        JsonObject segmentInfo = new JsonObject();
        segmentInfo.addProperty("dataSource", dataSource);
        segmentInfo.addProperty("created_date", dataTime);
        segmentInfo.addProperty("size", size);
        segmentsInfo.add(segmentInfo.toString());

        final String topic = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DRUID_SEGMENT_INFO_KAFKA_TOPIC);
        final String kafkaBrokerList = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_DRUID_SEGMENT_INFO_BROKER_LIST);
        CONSOLE.printInfo(String.format("Send segment info to kafka, kafkaBrokerList: %s, topic: %s", kafkaBrokerList, topic));
        KafkaUtils.sendMessage(kafkaBrokerList, topic, segmentsInfo);
      } catch (Exception e) {
        CONSOLE.printError(String.format("Segment info send to kafka failed, datasource: %s, error info: %s", dataSource, e));
      }

      return finalSegmentsToPublish;
    });
  }

  private static void disableDataSourceWithHandle(Handle handle,
      MetadataStorageTablesConfig metadataStorageTablesConfig,
      String dataSource) {
    handle.createStatement(String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource",
        metadataStorageTablesConfig.getSegmentsTable())).bind("dataSource", dataSource).execute();
  }

  /**
   * @param connector                   SQL connector to metadata
   * @param metadataStorageTablesConfig Tables configuration
   * @param dataSource                  Name of data source
   * @return List of all data segments part of the given data source
   */
  static List<DataSegment> getDataSegmentList(final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig metadataStorageTablesConfig,
      final String dataSource) {
    return connector.retryTransaction((handle, status) -> handle.createQuery(String.format(
        "SELECT payload FROM %s WHERE dataSource = :dataSource",
        metadataStorageTablesConfig.getSegmentsTable()))
        .setFetchSize(getStreamingFetchSize(connector))
        .bind("dataSource", dataSource)
        .map(ByteArrayMapper.FIRST)
        .fold(new ArrayList<>(), (Folder3<List<DataSegment>, byte[]>) (accumulator, payload, control, ctx) -> {
          try {
            final DataSegment segment = DATA_SEGMENT_INTERNER.intern(JSON_MAPPER.readValue(payload, DataSegment.class));

            accumulator.add(segment);
            return accumulator;
          } catch (Exception e) {
            throw new SQLException(e.toString());
          }
        }), 3, DEFAULT_MAX_TRIES);
  }

  /**
   * @param connector SQL DBI connector.
   * @return streaming fetch size.
   */
  private static int getStreamingFetchSize(SQLMetadataConnector connector) {
    if (connector instanceof MySQLConnector) {
      return Integer.MIN_VALUE;
    }
    return DEFAULT_STREAMING_RESULT_SIZE;
  }

  /**
   * @param pushedSegment         the pushed data segment object
   * @param segmentsDescriptorDir actual directory path for descriptors.
   * @return a sanitize file name
   */
  public static Path makeSegmentDescriptorOutputPath(DataSegment pushedSegment, Path segmentsDescriptorDir) {
    return new Path(segmentsDescriptorDir, String.format("%s%s.json",
            pushedSegment.getId().toString().replace(":", ""),
            DataSegmentPusher.generateUniquePath()));
  }

  public static String createScanAllQuery(String dataSourceName, List<String> columns) throws JsonProcessingException {
    final ScanQuery.ScanQueryBuilder scanQueryBuilder = ScanQuery.newScanQueryBuilder();
    final List<Interval> intervals = Collections.singletonList(DEFAULT_INTERVAL);
    ScanQuery
        scanQuery =
        scanQueryBuilder.dataSource(dataSourceName)
            .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
            .intervals(new MultipleIntervalSegmentSpec(intervals))
            .columns(columns)
            .build();
    return JSON_MAPPER.writeValueAsString(scanQuery);
  }

  @Nullable static Boolean getBooleanProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    return Boolean.parseBoolean(val);
  }

  static boolean getBooleanProperty(Table table, String propertyName, boolean defaultVal) {
    Boolean val = getBooleanProperty(table, propertyName);
    return val == null ? defaultVal : val;
  }

  @Nullable static Integer getIntegerProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(String.format("Exception while parsing property[%s] with Value [%s] as Integer",
          propertyName,
          val));
    }
  }

  static int getIntegerProperty(Table table, String propertyName, int defaultVal) {
    Integer val = getIntegerProperty(table, propertyName);
    return val == null ? defaultVal : val;
  }

  @Nullable static Long getLongProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(String.format("Exception while parsing property[%s] with Value [%s] as Long",
          propertyName,
          val));
    }
  }

  @Nullable static Period getPeriodProperty(Table table, String propertyName) {
    String val = getTableProperty(table, propertyName);
    if (val == null) {
      return null;
    }
    try {
      return Period.parse(val);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Exception while parsing property[%s] with Value [%s] as Period",
          propertyName,
          val));
    }
  }

  @Nullable public static List<String> getListProperty(Table table, String propertyName) {
    List<String> rv = new ArrayList<>();
    String values = getTableProperty(table, propertyName);
    if (values == null) {
      return null;
    }
    String[] vals = values.trim().split(",");
    for (String val : vals) {
      if (org.apache.commons.lang.StringUtils.isNotBlank(val)) {
        rv.add(val);
      }
    }
    return rv;
  }

  static String getTableProperty(Table table, String propertyName) {
    return table.getParameters().get(propertyName);
  }

  /**
   * Simple interface for retry operations.
   */
  public interface DataPusher {
    void push() throws IOException;
  }

  // Thanks, HBase Storage handler
  @SuppressWarnings("SameParameterValue") static void addDependencyJars(Configuration conf, Class<?>... classes)
          throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<>(conf.getStringCollection("tmpjars"));
    String depJars = HiveConf.getVar(conf, HIVE_DRUID_DEPEND_JARS);
    Set<String> newJars = new HashSet<>();
    String[] theJars = depJars.split(",");
    for (String jar: theJars) {
      String[] nodes = jar.split("/");
      String jar1 = nodes[nodes.length - 1];
      newJars.add(jar1);
      jars.add(jar);
    }
    for (Class<?> clazz : classes) {
      if (clazz == null) {
        continue;
      }
      final String path = Utilities.jarFinderGetJar(clazz);
      if (path == null) {
        throw new RuntimeException("Could not find jar for class " + clazz + " in order to ship it to the cluster.");
      }
      if (!localFs.exists(new Path(path))) {
        throw new RuntimeException("Could not validate jar file " + path + " for class " + clazz);
      }
      String[] nodes = path.split("/");
      String jar = nodes[nodes.length - 1];
      if (!newJars.contains(jar)) {
        LOG.error("need ext jar files {}, make sure to add it as bigo.handler.dependency.jars property", jars);
        throw new IOException("need path of " + jar + " in property hive.druid.depend.jars");
      }
    }
    if (jars.isEmpty()) {
      return;
    }
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  private static VersionedIntervalTimeline<String, DataSegment> getTimelineForIntervalWithHandle(final Handle handle,
      final String dataSource,
      final Interval interval,
      final MetadataStorageTablesConfig dbTables) throws IOException {
    Query<Map<String, Object>>
        sql =
        handle.createQuery(String.format(
            "SELECT payload FROM %s WHERE used = true AND dataSource = ? AND start <= ? AND \"end\" >= ?",
            dbTables.getSegmentsTable()))
            .bind(0, dataSource)
            .bind(1, interval.getEnd().toString())
            .bind(2, interval.getStart().toString());

    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    try (ResultIterator<byte[]> dbSegments = sql.map(ByteArrayMapper.FIRST).iterator()) {
      while (dbSegments.hasNext()) {
        final byte[] payload = dbSegments.next();
        DataSegment segment = JSON_MAPPER.readValue(payload, DataSegment.class);
        timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
      }
    }
    return timeline;
  }

  public static DataSegmentPusher createSegmentPusherForDirectory(String segmentDirectory, Configuration configuration)
      throws IOException {
    LOG.info("segmentDirectory {}", segmentDirectory);
    final HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    return new HdfsDataSegmentPusher(hdfsDataSegmentPusherConfig, configuration, JSON_MAPPER);
  }

  private static DataSegment publishSegmentWithShardSpec(DataSegment segment,
      ShardSpec shardSpec,
      String version,
      FileSystem fs,
      DataSegmentPusher dataSegmentPusher) throws IOException {
    boolean retry = true;
    DataSegment.Builder dataSegmentBuilder = new DataSegment.Builder(segment).version(version);
    Path finalPath = null;
    while (retry) {
      retry = false;
      dataSegmentBuilder.shardSpec(shardSpec);
      final Path intermediatePath = getPath(segment);

      finalPath =
          new Path(dataSegmentPusher.getPathForHadoop(),
              dataSegmentPusher.makeIndexPathName(dataSegmentBuilder.build(), DruidStorageHandlerUtils.INDEX_ZIP));
      // Create parent if it does not exist, recreation is not an error
      fs.mkdirs(finalPath.getParent());

      if (!fs.rename(intermediatePath, finalPath)) {
        if (fs.exists(finalPath)) {
          // Someone else is also trying to append
          shardSpec = getNextPartitionShardSpec(shardSpec);
          retry = true;
        } else {
          throw new IOException(String.format(
              "Failed to rename intermediate segment[%s] to final segment[%s] is not present.",
              intermediatePath,
              finalPath));
        }
      }
    }
    DataSegment dataSegment = dataSegmentBuilder.loadSpec(dataSegmentPusher.makeLoadSpec(finalPath.toUri())).build();

    writeSegmentDescriptor(fs, dataSegment, new Path(finalPath.getParent(), DruidStorageHandlerUtils.DESCRIPTOR_JSON));

    return dataSegment;
  }

  private static ShardSpec getNextPartitionShardSpec(ShardSpec shardSpec) {
    if (shardSpec instanceof LinearShardSpec) {
      return new LinearShardSpec(shardSpec.getPartitionNum() + 1);
    } else if (shardSpec instanceof NumberedShardSpec) {
      return new NumberedShardSpec(shardSpec.getPartitionNum(), ((NumberedShardSpec) shardSpec).getPartitions());
    } else {
      // Druid only support appending more partitions to Linear and Numbered ShardSpecs.
      throw new IllegalStateException(String.format("Cannot expand shard spec [%s]", shardSpec));
    }
  }

  static Path getPath(DataSegment dataSegment) {
    return new Path(String.valueOf(Objects.requireNonNull(dataSegment.getLoadSpec()).get("path")));
  }

  public static GranularitySpec getGranularitySpec(Configuration configuration, Properties tableProperties) {
    final String
        segmentGranularity =
        tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) != null ?
            tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) :
            HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY);
    final boolean
        rollup =
        tableProperties.getProperty(DruidConstants.DRUID_ROLLUP) != null ?
            Boolean.parseBoolean(tableProperties.getProperty(DruidConstants.DRUID_ROLLUP)) :
            true;
    return new UniformGranularitySpec(Granularity.fromString(segmentGranularity),
        Granularity.fromString(tableProperties.getProperty(DruidConstants.DRUID_QUERY_GRANULARITY) == null ?
            "NONE" :
            tableProperties.getProperty(DruidConstants.DRUID_QUERY_GRANULARITY)),
        rollup,
        null);
  }

  public static IndexSpec getIndexSpec(Configuration jc) {
    final BitmapSerdeFactory bitmapSerdeFactory = new RoaringBitmapSerdeFactory(true);

    return new IndexSpec(bitmapSerdeFactory,
        IndexSpec.DEFAULT_DIMENSION_COMPRESSION,
        IndexSpec.DEFAULT_METRIC_COMPRESSION,
        IndexSpec.DEFAULT_LONG_ENCODING);
  }

  public static Set<String> parseFields(String fields) {
    if (fields == null || fields.isEmpty()) {
      return new HashSet<>();
    }
    String[] fieldsArray = fields.split(",");
    Set<String> result = new HashSet<>();
    for (String field: fieldsArray) {
      if (field == null || field.isEmpty()) {
        continue;
      }
      result.add(field);
    }
    return result;
  }

  public static String getTableProperty(Properties tableProperties, JobConf jc, String key) {
    return tableProperties.getProperty(key) == null
            ? jc.get(key)
            : tableProperties.getProperty(key);
  }

  public static FieldTypeEnum getFieldType(String fieldName) {

    if (fieldName == null || fieldName.equals("") || !fieldName.contains("_")) {
      return FieldTypeEnum.OTHER;
    }
    String end = fieldName.substring(fieldName.lastIndexOf("_"));
    if (end.equals("_hll")) {
      return FieldTypeEnum.HLL;
    } else if (end.equals("_theta")) {
      return FieldTypeEnum.THETA;
    } else if (end.equals("_sum")) {
      return FieldTypeEnum.SUM;
    } else if (end.equals("_dim")) {
      return FieldTypeEnum.DIM;
    } else if (end.equals("_cnt")) {
      return FieldTypeEnum.CNT;
    } else if (end.equals("_max")) {
      return FieldTypeEnum.MAX;
    } else if (end.equals("_min")) {
      return FieldTypeEnum.MIN;
    } else if (end.equals("_qua")) {
      return FieldTypeEnum.QUA;
    } else {
      return FieldTypeEnum.OTHER;
    }
  }

  public static Pair<List<DimensionSchema>, AggregatorFactory[]>
      getDimensionsAndAggregates(
              List<String> columnNames,
              List<TypeInfo> columnTypes,
              JobConf jc,
              Properties tableProperties
              ) {

    String druidHllFields = getTableProperty(tableProperties, jc,
            DruidConstants.DRUID_HLL_SKETCH_FIELDS);
    if (druidHllFields != null) {
      druidHllFields = druidHllFields.replaceAll(" ", "").replaceAll("\t", "").toLowerCase();
    }

    String druidThetaFields = getTableProperty(tableProperties, jc,
            DruidConstants.DRUID_THETA_SKETCH_FIELDS);
    if (druidThetaFields != null) {
      druidThetaFields = druidThetaFields.replaceAll(" ", "").replaceAll("\t", "").toLowerCase();
    }

    String druidExcludedDimensions = getTableProperty(tableProperties, jc,
            DruidConstants.DRUID_EXCLUDE_FIELDS);
    if (druidExcludedDimensions != null) {
      druidExcludedDimensions = druidExcludedDimensions.replaceAll(" ", "").replaceAll("\t", "").toLowerCase();
    }

    String sizeString = getTableProperty(tableProperties, jc,
            DruidConstants.DRUID_SKETCH_THETA_SIZE);
    if (sizeString != null) {
      sizeString = sizeString.replaceAll(" ", "").replaceAll("\t", "");
    }
    String druidHllLgK = getTableProperty(tableProperties, jc,
            DruidConstants.DRUID_HLL_LG_K);
    if (druidHllLgK != null) {
      druidHllLgK = druidHllLgK.replaceAll(" ", "").replaceAll("\t", "");
    }

    int lgk = HllSketchAggregatorFactory.DEFAULT_LG_K;
    if (druidHllLgK != null) {
      Integer lgk1 = Integer.parseInt(druidHllLgK);
      if (lgk1 != null && lgk1 > 2 >> 4 && lgk1 < 2 >> 21) {
        lgk = lgk1;
      }
    }

    int k = Integer.parseInt(HiveConf.getVar(jc, HiveConf.ConfVars.HIVE_DRUID_QUANTILES_PARAM_K));

    LOG.info("hive.druid.quantiles.k {}", k);
    String druidHllTgtType = getTableProperty(tableProperties, jc,
            DruidConstants.DRUID_HLL_TGT_TYPE);

    if (druidHllTgtType == null ||
            !(druidHllTgtType.equals("HLL_4") ||
                    druidHllTgtType.equals("HLL_6") || druidHllTgtType.equals("HLL_8"))) {
      druidHllTgtType = HllSketchAggregatorFactory.DEFAULT_TGT_HLL_TYPE.name();
    }

    Integer size = sizeString == null ? SketchAggregatorFactory.DEFAULT_MAX_SKETCH_SIZE / 2:
            Integer.parseInt(sizeString);

    if (size == null || size <= 1) {
      size = SketchAggregatorFactory.DEFAULT_MAX_SKETCH_SIZE / 2;
    }
    Integer hiveSize = Integer.parseInt(HiveConf.getVar(jc,
            HiveConf.ConfVars.HIVE_DRUID_SKETCH_THETA_SIZE));
    if (hiveSize != null && hiveSize > 0) {
      size = hiveSize;
    }

    Set<String> hllFields = parseFields(druidHllFields);
    Set<String> thetaFields = parseFields(druidThetaFields);
    Set<String> excludedDimensions = parseFields(druidExcludedDimensions);

    // Default, all columns that are not metrics or timestamp, are treated as dimensions
    final List<DimensionSchema> dimensions = new ArrayList<>();
    HllSketchModule.registerSerde();
    new OldApiSketchModule().configure(null);
    ImmutableList.Builder<AggregatorFactory> aggregatorFactoryBuilder = ImmutableList.builder();

    for (int i = 0; i < columnTypes.size(); i++) {
      String dColumnName = columnNames.get(i);
      FieldTypeEnum fieldTypeEnum = getFieldType(dColumnName);

      if (excludedDimensions.contains(dColumnName)) {
        continue;
      }
      TypeInfo typeInfo = columnTypes.get(i);
      if (typeInfo instanceof ListTypeInfo) {
        LOG.info("add " + dColumnName + " as mv dim");
        dimensions.add(new StringDimensionSchema(dColumnName));
      } else if (typeInfo instanceof PrimitiveTypeInfo) {
        LOG.info("column type is: " + typeInfo + ", column name is: " + dColumnName);
        // count distinct algorithm for druid
        if (fieldTypeEnum == FieldTypeEnum.HLL || hllFields.contains(dColumnName)) {
          LOG.info("column " + dColumnName + " treat as hll metric");
          aggregatorFactoryBuilder.add(new HllSketchBuildAggregatorFactory(dColumnName,
                  dColumnName, lgk,
                  druidHllTgtType));
          continue;
        } else if (fieldTypeEnum == FieldTypeEnum.THETA || thetaFields.contains(dColumnName)) {
          LOG.info("column " + dColumnName + " treat as sketch metric");
          aggregatorFactoryBuilder.add(new OldSketchBuildAggregatorFactory(dColumnName,
                  dColumnName, size));
          continue;
        }

        final PrimitiveObjectInspector.PrimitiveCategory
                primitiveCategory =
                ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        AggregatorFactory af = null;
        switch (primitiveCategory) {
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            if (fieldTypeEnum == FieldTypeEnum.DIM) {
              dimensions.add(new LongDimensionSchema(dColumnName));
            } else if (fieldTypeEnum == FieldTypeEnum.MAX) {
              af = new LongMaxAggregatorFactory(dColumnName, dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.MIN) {
              af = new LongMinAggregatorFactory(dColumnName, dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.CNT) {
              af = new CountAggregatorFactory(dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.QUA) {
              DoublesSketchModule.registerSerde();
              af = new DoublesSketchAggregatorFactory(dColumnName, dColumnName, k);
            } else {
              af = new LongSumAggregatorFactory(dColumnName, dColumnName);
            }
            break;
          case FLOAT:
            if (fieldTypeEnum == FieldTypeEnum.DIM) {
              dimensions.add(new FloatDimensionSchema(dColumnName));
            } else if (fieldTypeEnum == FieldTypeEnum.MAX) {
              af = new FloatMaxAggregatorFactory(dColumnName, dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.MIN) {
              af = new FloatMinAggregatorFactory(dColumnName, dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.CNT) {
              af = new CountAggregatorFactory(dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.QUA) {
              DoublesSketchModule.registerSerde();
              af = new DoublesSketchAggregatorFactory(dColumnName, dColumnName, k);
            } else {
              af = new FloatSumAggregatorFactory(dColumnName, dColumnName);
            }
            break;
          case DOUBLE:
            if (fieldTypeEnum == FieldTypeEnum.DIM) {
              dimensions.add(new DoubleDimensionSchema(dColumnName));
            } else if (fieldTypeEnum == FieldTypeEnum.MAX) {
              af = new DoubleMaxAggregatorFactory(dColumnName, dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.MIN) {
              af = new DoubleMinAggregatorFactory(dColumnName, dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.CNT) {
              af = new CountAggregatorFactory(dColumnName);
            } else if (fieldTypeEnum == FieldTypeEnum.QUA) {
              DoublesSketchModule.registerSerde();
              af = new DoublesSketchAggregatorFactory(dColumnName, dColumnName, k);
            } else {
              af = new DoubleSumAggregatorFactory(dColumnName, dColumnName);
            }
            break;
          case DECIMAL:
            throw new UnsupportedOperationException(String.format("Druid does not support decimal column type cast column "
                    + "[%s] to double", dColumnName));
          case TIMESTAMP:
            // Granularity column
            if (!dColumnName.equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME)
                    && !dColumnName.equals(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
              throw new IllegalArgumentException("Dimension "
                      + dColumnName
                      + " does not have STRING type: "
                      + primitiveCategory);
            }
            continue;
          default:
            // Dimension
            if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(primitiveCategory)
                    != PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
                    && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
              throw new IllegalArgumentException("Dimension "
                      + dColumnName
                      + " does not have STRING type: "
                      + primitiveCategory);
            }
            if (fieldTypeEnum == FieldTypeEnum.CNT) {
              af = new CountAggregatorFactory(dColumnName);
              break;
            }
            LOG.info("add " + dColumnName + " as normal dim");
            dimensions.add(new StringDimensionSchema(dColumnName));
            continue;
        }
        if (af == null) {
          continue;
        }
        aggregatorFactoryBuilder.add(af);
      }
    }
    ImmutableList<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();
    return Pair.of(dimensions, aggregatorFactories.toArray(new AggregatorFactory[0]));
  }
}
