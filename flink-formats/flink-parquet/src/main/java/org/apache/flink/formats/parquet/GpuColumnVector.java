package org.apache.flink.formats.parquet;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.BooleanColumnVector;
import org.apache.flink.table.data.vector.ByteColumnVector;
import org.apache.flink.table.data.vector.BytesColumnVector;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.DoubleColumnVector;
import org.apache.flink.table.data.vector.FloatColumnVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.table.data.vector.LongColumnVector;
import org.apache.flink.table.data.vector.ShortColumnVector;
import org.apache.flink.table.data.vector.TimestampColumnVector;

import ai.rapids.cudf.Table;

public class GpuColumnVector implements ColumnVector, IntColumnVector, LongColumnVector, ByteColumnVector,
		ShortColumnVector, BytesColumnVector, BooleanColumnVector, DoubleColumnVector,
		FloatColumnVector, TimestampColumnVector {

	// Data in device
	private ai.rapids.cudf.ColumnVector cudfCv;
	// Data in host
	private final ai.rapids.cudf.HostColumnVector cudfHcv;

	public GpuColumnVector(ai.rapids.cudf.HostColumnVector hcv) {
		this.cudfHcv = hcv;
	}

	@Override
	public boolean isNullAt(int i) {
		return cudfHcv.isNull(i);
	}

	@Override
	public boolean getBoolean(int i) {
		return cudfHcv.getBoolean(i);
	}

	@Override
	public Bytes getBytes(int i) {
		// TODO
		return null;
	}

	@Override
	public int getInt(int i) {
		return cudfHcv.getInt(i);
	}

	@Override
	public long getLong(int i) {
		return cudfHcv.getLong(i);
	}

	@Override
	public short getShort(int i) {
		return cudfHcv.getShort(i);
	}

	@Override
	public double getDouble(int i) {
		return cudfHcv.getDouble(i);
	}

	@Override
	public float getFloat(int i) {
		return cudfHcv.getFloat(i);
	}

	@Override
	public byte getByte(int i) {
		return cudfHcv.getByte(i);
	}

	@Override
	public TimestampData getTimestamp(int i, int precision) {
		// TODO
		return null;
	}

	public static GpuColumnVector from(ai.rapids.cudf.ColumnVector cudfCv) {
		return new GpuColumnVector(cudfCv.copyToHost());
	}

	public static final GpuColumnVector[] extractColumns(Table table) {
		int numColumns = table.getNumberOfColumns();
		GpuColumnVector[] vectors = new GpuColumnVector[numColumns];

		for (int i = 0; i < vectors.length; i++) {
			ai.rapids.cudf.ColumnVector cv = table.getColumn(i);
			vectors[i] = new GpuColumnVector(cv.copyToHost());
		}

		// close table
		table.close();
		return vectors;
	}
}
