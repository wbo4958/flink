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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.flink.formats.parquet.utils.ParquetWriterUtil.createTempParquetFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link ParquetColumnarRowSplitReader}.
 */
@RunWith(Parameterized.class)
public class ParquetColumnarRowSplitReaderForGpuTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final int rowGroupSize;

	@Parameterized.Parameters(name = "rowGroupSize-{0}")
	public static Collection<Integer> parameters() {
		return Arrays.asList(10);
	}

	public ParquetColumnarRowSplitReaderForGpuTest(int rowGroupSize) {
		this.rowGroupSize = rowGroupSize;
	}

	@Test
	public void testNormalTypesReadWithSplitsWithGpuSupporting() throws IOException {
		Path testPath = new Path("/home/bobwang/taxi.parquet");
		long len = testPath.getFileSystem().getFileStatus(testPath).getLen();

		LogicalType[] fieldTypes = new LogicalType[]{
			new IntType(), new IntType(),
			new DoubleType(), new DoubleType(), new DoubleType(),
			new IntType(), new IntType(),
			new DoubleType(), new DoubleType(), new DoubleType(),
			new IntType(), new IntType(), new IntType(), new IntType(), new IntType(), new IntType(),
			new DoubleType()};

		ParquetColumnarRowSplitReader reader = new ParquetColumnarRowSplitReader(
			false,
			true,
			new Configuration(),
			fieldTypes,
			new String[] {
				"vendor_id", "passenger_count",
				"trip_distance", "pickup_longitude", "pickup_latitude",
				"rate_code", "store_and_fwd_flag",
				"dropoff_longitude", "dropoff_latitude", "fare_amount",
				"year", "month", "day", "day_of_week", "is_weekend", "hour",
				"h_distance"},
			VectorizedColumnBatch::new,
			500,
			new org.apache.hadoop.fs.Path(testPath.getPath()),
			0,
			len,
			true);

		System.out.println("vendor_id\t passenger_count\t trip_distance\t pickup_longitude\t pickup_latitude");
		int count = 10;
		while (!reader.reachedEnd() && count-- > 0) {
			ColumnarRowData row = reader.nextRecord();
			System.out.println(row.getInt(0) + " "  + row.getInt(1) + " " +
				row.getDouble(2) + " " + row.getDouble(3) + " " + row.getDouble(4));
		}

	}
}
