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
package org.apache.hadoop.hive.druid.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.druid.query.Druids;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandler;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.conf.DruidConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DruidSerDe that is used to  deserialize objects from a Druid data source.
 */
@SerDeSpec(schemaProps = { Constants.DRUID_DATA_SOURCE }) public class DruidSerDe extends AbstractSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(DruidSerDe.class);

  private String[] columns;
  private TypeInfo[] types;
  private ObjectInspector inspector;

  @Override public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    // No query. Either it is a CTAS, or we need to create a Druid meta data Query
    if (!org.apache.commons.lang3.StringUtils.isEmpty(properties.getProperty(serdeConstants.LIST_COLUMNS))
            && !org.apache.commons.lang3.StringUtils.isEmpty(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES))) {
      // CASE CTAS statement
      initFromProperties(properties);
    } else {
      // Segment Metadata query that retrieves all columns present in
      // the data source (dimensions and metrics).
      initFromMetaDataQuery(configuration, properties);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("DruidSerDe initialized with\n"
              + "\t columns: "
              + Arrays.toString(columns)
              + "\n\t types: "
              + Arrays.toString(types));
    }
  }

  private void initFromProperties(final Properties properties) throws SerDeException {
    final List<String> columnNames = new ArrayList<>(Utilities.getColumnNames(properties));
    if (!columnNames.contains(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
      throw new SerDeException("Timestamp column (' "
              + DruidConstants.DEFAULT_TIMESTAMP_COLUMN
              + "') not specified in create table; list of columns is : "
              + properties.getProperty(serdeConstants.LIST_COLUMNS));
    }
    List<TypeInfo> columnTypes = new ArrayList<>();
    List<String> typeNames = Utilities.getColumnTypes(properties);
    for (String typeName: typeNames) {
      LOG.info("type name is :" + typeName);
      if (typeName.startsWith("array<") && typeName.endsWith(">")) {
        String elementTypeName = typeName.substring(6, typeName.length() - 1);
        TypeInfo typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(elementTypeName);
        columnTypes.add(TypeInfoFactory.getListTypeInfo(typeInfo));
      } else {
        columnTypes.add(TypeInfoFactory.getPrimitiveTypeInfo(typeName));
      }
    }

//    final List<PrimitiveTypeInfo>
//            columnTypes =
//            Utilities.getColumnTypes(properties)
//                    .stream()
//                    .map(TypeInfoFactory::getPrimitiveTypeInfo)
//                    .collect(Collectors.toList())
//                    .stream()
//                    .collect(Collectors.toList());

    final List<ObjectInspector> inspectors = new ArrayList<>();
    for (TypeInfo info: columnTypes) {
      if (info.getTypeName().startsWith("array<") && info.getTypeName().endsWith(">")) {
        ListTypeInfo listTypeInfo = (ListTypeInfo)info;
        PrimitiveTypeInfo element = (PrimitiveTypeInfo) listTypeInfo.getListElementTypeInfo();
        inspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(element)));
      } else {
        inspectors.add(PrimitiveObjectInspectorFactory.
                getPrimitiveJavaObjectInspector((PrimitiveTypeInfo)info));
      }
    }
//    final List<ObjectInspector>
//            inspectors =
//            columnTypes.stream()
//                    .map(PrimitiveObjectInspectorFactory::getPrimitiveJavaObjectInspector)
//                    .collect(Collectors.toList());
    columns = columnNames.toArray(new String[0]);
    types = columnTypes.toArray(new TypeInfo[0]);
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  private void initFromMetaDataQuery(final Configuration configuration, final Properties properties)
          throws SerDeException {
    final List<String> columnNames = new ArrayList<>();
    final List<PrimitiveTypeInfo> columnTypes = new ArrayList<>();
    final List<ObjectInspector> inspectors = new ArrayList<>();

    String dataSource = properties.getProperty(Constants.DRUID_DATA_SOURCE);
    if (dataSource == null) {
      throw new SerDeException("Druid data source not specified; use "
              + Constants.DRUID_DATA_SOURCE
              + " in table properties");
    }
    Druids.SegmentMetadataQueryBuilder builder = new Druids.SegmentMetadataQueryBuilder();
    builder.dataSource(dataSource);
    builder.merge(true);
    builder.analysisTypes();
    SegmentMetadataQuery query = builder.build();

    // Execute query in Druid
    String address = HiveConf.getVar(configuration, HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
    if (org.apache.commons.lang3.StringUtils.isEmpty(address)) {
      throw new SerDeException("Druid broker address not specified in configuration");
    }
    // Infer schema
    SegmentAnalysis schemaInfo;
    try {
      schemaInfo = submitMetadataRequest(address, query);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    for (Map.Entry<String, ColumnAnalysis> columnInfo : schemaInfo.getColumns().entrySet()) {
      if (columnInfo.getKey().equals(DruidConstants.DEFAULT_TIMESTAMP_COLUMN)) {
        // Special handling for timestamp column
        columnNames.add(columnInfo.getKey()); // field name
        PrimitiveTypeInfo type = TypeInfoFactory.timestampTypeInfo; // field type
        columnTypes.add(type);
        inspectors
                .add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
        continue;
      }
      columnNames.add(columnInfo.getKey()); // field name
      PrimitiveTypeInfo type = DruidSerDeUtils.convertDruidToHiveType(columnInfo.getValue().getType()); // field type
      columnTypes.add(type);
      inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type));
    }
    columns = columnNames.toArray(new String[0]);
    types = columnTypes.toArray(new PrimitiveTypeInfo[0]);
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  /* Submits the request and returns */
  protected SegmentAnalysis submitMetadataRequest(String address, SegmentMetadataQuery query)
          throws SerDeException, IOException {
    InputStream response;
    try {
      response =
              DruidStorageHandlerUtils.submitRequest(DruidStorageHandler.getHttpClient(),
                      DruidStorageHandlerUtils.createSmileRequest(address, query));
    } catch (Exception e) {
      throw new SerDeException(StringUtils.stringifyException(e));
    }

    // Retrieve results
    List<SegmentAnalysis> resultsList;
    try {
      // This will throw an exception in case of the response from druid is not an array
      // this case occurs if for instance druid query execution returns an exception instead of array of results.
      resultsList =
              DruidStorageHandlerUtils.SMILE_MAPPER.readValue(response, new TypeReference<List<SegmentAnalysis>>() {
              });
    } catch (Exception e) {
      response.close();
      throw new SerDeException(StringUtils.stringifyException(e));
    }
    if (resultsList == null || resultsList.isEmpty()) {
      throw new SerDeException("Connected to Druid but could not retrieve datasource information");
    }
    if (resultsList.size() != 1) {
      throw new SerDeException("Information about segments should have been merged");
    }

    return resultsList.get(0);
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return DruidWritable.class;
  }

  @Override public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString()
              + " can only serialize struct types, but we got: "
              + objectInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> values = soi.getStructFieldsDataAsList(o);
    // We deserialize the result
    final Map<String, Object> value = new HashMap<>();
    for (int i = 0; i < columns.length; i++) {
      if (values.get(i) == null) {
        // null, we just add it
        value.put(columns[i], null);
        continue;
      }
      TypeInfo typeInfo = types[i];
      if (typeInfo instanceof PrimitiveTypeInfo) {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        final Object res;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case TIMESTAMP:
            res = ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector())
                    .getPrimitiveJavaObject(
                            values.get(i)).toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
            break;
          case BYTE:
            res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
            break;
          case SHORT:
            res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
            break;
          case INT:
            res = ((IntObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
            break;
          case LONG:
            res = ((LongObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
            break;
          case FLOAT:
            res = ((FloatObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
            break;
          case DOUBLE:
            res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
            break;
          case CHAR:
            res =
                    ((HiveCharObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i))
                            .getValue();
            break;
          case VARCHAR:
            res =
                    ((HiveVarcharObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i))
                            .getValue();
            break;
          case STRING:
            res = ((StringObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(values.get(i));
            break;
          case BOOLEAN:
            res = ((BooleanObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i)) ? 1L : 0L;
            break;
          default:
            throw new SerDeException("Unsupported type: " + primitiveTypeInfo.getPrimitiveCategory());
        }
        value.put(columns[i], res);
      } else if (typeInfo instanceof ListTypeInfo) {
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) listTypeInfo.getListElementTypeInfo();
        final Object res;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
           case STRING:
//             LOG.info("field type is:" + fields.get(i).getFieldObjectInspector());
             ListObjectInspector listObjectInspector = (LazyBinaryListObjectInspector) fields.get(i);
             res = listObjectInspector.getList(value.get(i));
            break;
          default:
            throw new SerDeException("Unsupported type: " + listTypeInfo.getCategory());
        }
        value.put(columns[i], res);
      }
    }
    //Extract the partitions keys segments granularity and partition key if any
    // First Segment Granularity has to be here.
    final int granularityFieldIndex = columns.length;
    assert values.size() > granularityFieldIndex;
    Preconditions.checkArgument(fields.get(granularityFieldIndex)
            .getFieldName()
            .equals(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME));

    Timestamp
            timestamp =
            ((TimestampObjectInspector) fields.get(granularityFieldIndex).getFieldObjectInspector()).getPrimitiveJavaObject(
                    values.get(granularityFieldIndex));
    Preconditions.checkNotNull(timestamp, "Timestamp column cannot have null value");
    value.put(Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME, timestamp.getTime());

    if (values.size() == columns.length + 2) {
      // Then partition number if any.
      final int partitionNumPos = granularityFieldIndex + 1;
      Preconditions.checkArgument(fields.get(partitionNumPos).getFieldName().equals(DruidConstants.DRUID_SHARD_KEY_COL_NAME),
              String.format("expecting to encounter %s but was %s",
                      DruidConstants.DRUID_SHARD_KEY_COL_NAME,
                      fields.get(partitionNumPos).getFieldName()));
      value.put(DruidConstants.DRUID_SHARD_KEY_COL_NAME,
              ((LongObjectInspector) fields.get(partitionNumPos)
                      .getFieldObjectInspector()).get(values.get(partitionNumPos)));
    }

    return new DruidWritable(value);
  }

  @Override public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  /**
   * @param writable Druid Writable to be deserialized.
   * @return List of Hive Writables.
   * @throws SerDeException if there is Serde issues.
   */
  @Override public Object deserialize(Writable writable) throws SerDeException {
    throw new SerDeException(getClass().toString()
            + " Don't support deserialize ");
  }

  @Override public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return true;
  }
}
