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

package org.apache.iceberg.spark.source;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestSparkDataWrite {
  private static final Configuration CONF = new Configuration();
  private final FileFormat format;
  private static SparkSession spark = null;
  private static final Map<String, String> info = ImmutableMap.of("a", "A", "b", "B");
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()),
      optional(3, "info", Types.MapType.ofOptional(
          4, 5, Types.StringType.get(), Types.StringType.get()))
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" },
        new Object[] { "orc" }
    };
  }

  @BeforeClass
  public static void startSpark() {
    TestSparkDataWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDataWrite.spark;
    TestSparkDataWrite.spark = null;
    currentSpark.stop();
  }

  public TestSparkDataWrite(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Test
  public void testBasicWrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<NestedRecord> expected = Lists.newArrayList(
        new NestedRecord(1, "a", info),
        new NestedRecord(2, "b", info),
        new NestedRecord(3, "c", info)
    );

    Dataset<Row> df = spark.createDataFrame(expected, NestedRecord.class);
    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (DataFile file : ManifestReader.read(manifest, table.io())) {
        // TODO: avro not support split
        if (!format.equals(FileFormat.AVRO)) {
          Assert.assertNotNull("Split offsets not present", file.splitOffsets());
        }
        Assert.assertEquals("Should have reported record count as 1", 1, file.recordCount());
        //TODO: append more metric info
        if (format.equals(FileFormat.PARQUET)) {
          Assert.assertNotNull("Column sizes metric not present", file.columnSizes());
          Assert.assertNotNull("Counts metric not present", file.valueCounts());
          Assert.assertNotNull("Null value counts metric not present", file.nullValueCounts());
          Assert.assertNotNull("Lower bounds metric not present", file.lowerBounds());
          Assert.assertNotNull("Upper bounds metric not present", file.upperBounds());
        }
      }
    }
  }

  @Test
  public void testAppend() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<NestedRecord> records = Lists.newArrayList(
        new NestedRecord(1, "a", info),
        new NestedRecord(2, "b", info),
        new NestedRecord(3, "c", info)
    );

    List<NestedRecord> expected = Lists.newArrayList(
        new NestedRecord(1, "a", info),
        new NestedRecord(2, "b", info),
        new NestedRecord(3, "c", info),
        new NestedRecord(4, "a", info),
        new NestedRecord(5, "b", info),
        new NestedRecord(6, "c", info)
    );

    Dataset<Row> df = spark.createDataFrame(records, NestedRecord.class);

    df.select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    df.withColumn("id", df.col("id").plus(3)).select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<NestedRecord> records = Lists.newArrayList(
        new NestedRecord(1, "a", info),
        new NestedRecord(2, "b", info),
        new NestedRecord(3, "c", info)
    );

    List<NestedRecord> expected = Lists.newArrayList(
        new NestedRecord(1, "a", info),
        new NestedRecord(2, "a", info),
        new NestedRecord(3, "c", info),
        new NestedRecord(4, "b", info),
        new NestedRecord(6, "c", info)
    );

    Dataset<Row> df = spark.createDataFrame(records, NestedRecord.class);

    df.select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    // overwrite with 2*id to replace record 2, append 4 and 6
    df.withColumn("id", df.col("id").multiply(2)).select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("overwrite")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<NestedRecord> expected = Lists.newArrayList(
        new NestedRecord(1, "a", info),
        new NestedRecord(2, "b", info),
        new NestedRecord(3, "c", info)
    );

    Dataset<Row> df = spark.createDataFrame(expected, NestedRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    // overwrite with the same data; should not produce two copies
    df.select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("overwrite")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedCreateWithTargetFileSizeViaTableProperties() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    table.updateProperties()
        .set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "4") // ~4 bytes; low enough to trigger
        .commit();

    List<NestedRecord> expected = Lists.newArrayListWithCapacity(4000);
    for (int i = 0; i < 4000; i++) {
      expected.add(new NestedRecord(i, "a", info));
    }

    Dataset<Row> df = spark.createDataFrame(expected, NestedRecord.class);

    df.select("id", "data", "info").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (DataFile file : ManifestReader.read(manifest, table.io())) {
        files.add(file);
      }
    }
    // TODO: ORC file now not support target file size
    if (format.equals(FileFormat.PARQUET)) {
      Assert.assertEquals("Should have 4 DataFiles", 4, files.size());
      Assert.assertTrue("All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
    }
  }

  @Test
  public void testPartitionedCreateWithTargetFileSizeViaOption() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<NestedRecord> expected = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      expected.add(new NestedRecord(i, "a", info));
      expected.add(new NestedRecord(i, "b", info));
      expected.add(new NestedRecord(i, "c", info));
      expected.add(new NestedRecord(i, "d", info));
    }

    Dataset<Row> df = spark.createDataFrame(expected, NestedRecord.class);

    df.select("id", "data", "info").sort("data").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .option("target-file-size-bytes", 4) // ~4 bytes; low enough to trigger
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (DataFile file : ManifestReader.read(manifest, table.io())) {
        files.add(file);
      }
    }
    // TODO: ORC file now not support target file size
    if (format.equals(FileFormat.PARQUET)) {
      Assert.assertEquals("Should have 8 DataFiles", 8, files.size());
      Assert.assertTrue("All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
    }
  }

  @Test
  public void testWriteProjection() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<NestedRecord> expected = Lists.newArrayList(
        new NestedRecord(1, null, null),
        new NestedRecord(2, null, null),
        new NestedRecord(3, null, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, NestedRecord.class);

    df.select("id").write() // select only id column
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<NestedRecord> actual = result.orderBy("id").as(Encoders.bean(NestedRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testWriteProjectionWithMiddle() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Schema schema = new Schema(
        optional(1, "c1", Types.IntegerType.get()),
        optional(2, "c2", Types.StringType.get()),
        optional(3, "c3", Types.StringType.get())
    );
    Table table = tables.create(schema, spec, location.toString());

    List<ThreeColumnRecord> expected = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "hello"),
        new ThreeColumnRecord(2, null, "world"),
        new ThreeColumnRecord(3, null, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, ThreeColumnRecord.class);

    df.select("c1", "c3").write()
        .format("iceberg")
        .option("write-format", format.toString())
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<ThreeColumnRecord> actual = result.orderBy("c1").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }
}
