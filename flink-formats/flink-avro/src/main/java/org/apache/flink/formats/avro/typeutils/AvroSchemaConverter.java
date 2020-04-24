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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Converts an Avro schema into Flink's type information. It uses {@link RowTypeInfo} for representing
 * objects and converts Avro types into types that are compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * classes {@link AvroRowDeserializationSchema} and {@link AvroRowSerializationSchema}.
 */
public class AvroSchemaConverter {

	/**
	 * Used for time conversions into SQL types.
	 */
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	private AvroSchemaConverter() {
		// private
	}

	/**
	 * Converts an Avro class into a nested row structure with deterministic field order and data
	 * types that are compatible with Flink's Table & SQL API.
	 *
	 * @param avroClass Avro specific record that contains schema information
	 * @return type information matching the schema
	 */
	@SuppressWarnings("unchecked")
	public static <T extends SpecificRecord> TypeInformation<Row> convertToTypeInfo(Class<T> avroClass) {
		Preconditions.checkNotNull(avroClass, "Avro specific record class must not be null.");
		// determine schema to retrieve deterministic field order
		final Schema schema = SpecificData.get().getSchema(avroClass);
		return (TypeInformation<Row>) convertToTypeInfo(schema);
	}

	/**
	 * Converts an Avro schema string into a nested row structure with deterministic field order and data
	 * types that are compatible with Flink's Table & SQL API.
	 *
	 * @param avroSchemaString Avro schema definition string
	 * @return type information matching the schema
	 */
	@SuppressWarnings("unchecked")
	public static <T> TypeInformation<T> convertToTypeInfo(String avroSchemaString) {
		Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
		final Schema schema;
		try {
			schema = new Schema.Parser().parse(avroSchemaString);
		} catch (SchemaParseException e) {
			throw new IllegalArgumentException("Could not parse Avro schema string.", e);
		}
		return (TypeInformation<T>) convertToTypeInfo(schema);
	}

	private static TypeInformation<?> convertToTypeInfo(Schema schema) {
		switch (schema.getType()) {
			case RECORD:
				final List<Schema.Field> fields = schema.getFields();

				final TypeInformation<?>[] types = new TypeInformation<?>[fields.size()];
				final String[] names = new String[fields.size()];
				for (int i = 0; i < fields.size(); i++) {
					final Schema.Field field = fields.get(i);
					types[i] = convertToTypeInfo(field.schema());
					names[i] = field.name();
				}
				return Types.ROW_NAMED(names, types);
			case ENUM:
				return Types.STRING;
			case ARRAY:
				// result type might either be ObjectArrayTypeInfo or BasicArrayTypeInfo for Strings
				return Types.OBJECT_ARRAY(convertToTypeInfo(schema.getElementType()));
			case MAP:
				return Types.MAP(Types.STRING, convertToTypeInfo(schema.getValueType()));
			case UNION:
				final Schema actualSchema;
				if (schema.getTypes().size() == 2 && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
					actualSchema = schema.getTypes().get(1);
				} else if (schema.getTypes().size() == 2 && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
					actualSchema = schema.getTypes().get(0);
				} else if (schema.getTypes().size() == 1) {
					actualSchema = schema.getTypes().get(0);
				} else {
					// use Kryo for serialization
					return Types.GENERIC(Object.class);
				}
				return convertToTypeInfo(actualSchema);
			case FIXED:
				// logical decimal type
				if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
					return Types.BIG_DEC;
				}
				// convert fixed size binary data to primitive byte arrays
				return Types.PRIMITIVE_ARRAY(Types.BYTE);
			case STRING:
				// convert Avro's Utf8/CharSequence to String
				return Types.STRING;
			case BYTES:
				// logical decimal type
				if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
					return Types.BIG_DEC;
				}
				return Types.PRIMITIVE_ARRAY(Types.BYTE);
			case INT:
				// logical date and time type
				final LogicalType logicalType = schema.getLogicalType();
				if (logicalType == LogicalTypes.date()) {
					return Types.SQL_DATE;
				} else if (logicalType == LogicalTypes.timeMillis()) {
					return Types.SQL_TIME;
				}
				return Types.INT;
			case LONG:
				// logical timestamp type
				if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
					return Types.SQL_TIMESTAMP;
				}
				return Types.LONG;
			case FLOAT:
				return Types.FLOAT;
			case DOUBLE:
				return Types.DOUBLE;
			case BOOLEAN:
				return Types.BOOLEAN;
			case NULL:
				return Types.VOID;
		}
		throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
	}

	// --------------------------------------------------------------------------------------------

	public static Row convertAvroRecordToRow(Schema schema, RowTypeInfo typeInfo, IndexedRecord record) {
		final List<Schema.Field> fields = schema.getFields();
		final TypeInformation<?>[] fieldInfo = typeInfo.getFieldTypes();
		final int length = fields.size();
		final Row row = new Row(length);
		for (int i = 0; i < length; i++) {
			final Schema.Field field = fields.get(i);
			row.setField(i, convertAvroType(field.schema(), fieldInfo[i], record.get(i)));
		}
		return row;
	}

	public static Object convertAvroType(Schema schema, TypeInformation<?> info, Object object) {
		// we perform the conversion based on schema information but enriched with pre-computed
		// type information where useful (i.e., for arrays)

		if (object == null) {
			return null;
		}
		switch (schema.getType()) {
			case RECORD:
				if (object instanceof IndexedRecord) {
					return convertAvroRecordToRow(schema, (RowTypeInfo) info, (IndexedRecord) object);
				}
				throw new IllegalStateException("IndexedRecord expected but was: " + object.getClass());
			case ENUM:
			case STRING:
				return object.toString();
			case ARRAY:
				if (info instanceof BasicArrayTypeInfo) {
					final TypeInformation<?> elementInfo = ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo();
					return convertToObjectArray(schema.getElementType(), elementInfo, object);
				} else {
					final TypeInformation<?> elementInfo = ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo();
					return convertToObjectArray(schema.getElementType(), elementInfo, object);
				}
			case MAP:
				final MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) info;
				final Map<String, Object> convertedMap = new HashMap<>();
				final Map<?, ?> map = (Map<?, ?>) object;
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					convertedMap.put(
						entry.getKey().toString(),
						convertAvroType(schema.getValueType(), mapTypeInfo.getValueTypeInfo(), entry.getValue()));
				}
				return convertedMap;
			case UNION:
				final List<Schema> types = schema.getTypes();
				final int size = types.size();
				final Schema actualSchema;
				if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
					return convertAvroType(types.get(1), info, object);
				} else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
					return convertAvroType(types.get(0), info, object);
				} else if (size == 1) {
					return convertAvroType(types.get(0), info, object);
				} else {
					// generic type
					return object;
				}
			case FIXED:
				final byte[] fixedBytes = ((GenericFixed) object).bytes();
				if (info == Types.BIG_DEC) {
					return convertToDecimal(schema, fixedBytes);
				}
				return fixedBytes;
			case BYTES:
				final ByteBuffer byteBuffer = (ByteBuffer) object;
				final byte[] bytes = new byte[byteBuffer.remaining()];
				byteBuffer.get(bytes);
				if (info == Types.BIG_DEC) {
					return convertToDecimal(schema, bytes);
				}
				return bytes;
			case INT:
				if (info == Types.SQL_DATE) {
					return convertToDate(object);
				} else if (info == Types.SQL_TIME) {
					return convertToTime(object);
				}
				return object;
			case LONG:
				if (info == Types.SQL_TIMESTAMP) {
					return convertToTimestamp(object);
				}
				return object;
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return object;
		}
		throw new RuntimeException("Unsupported Avro type:" + schema);
	}

	public static BigDecimal convertToDecimal(Schema schema, byte[] bytes) {
		final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
		return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
	}

	public static Date convertToDate(Object object) {
		final long millis;
		if (object instanceof Integer) {
			final Integer value = (Integer) object;
			// adopted from Apache Calcite
			final long t = (long) value * 86400000L;
			millis = t - (long) LOCAL_TZ.getOffset(t);
		} else {
			// use 'provided' Joda time
			final LocalDate value = (LocalDate) object;
			millis = value.toDate().getTime();
		}
		return new Date(millis);
	}

	public static Time convertToTime(Object object) {
		final long millis;
		if (object instanceof Integer) {
			millis = (Integer) object;
		} else {
			// use 'provided' Joda time
			final LocalTime value = (LocalTime) object;
			millis = (long) value.get(DateTimeFieldType.millisOfDay());
		}
		return new Time(millis - LOCAL_TZ.getOffset(millis));
	}

	public static Timestamp convertToTimestamp(Object object) {
		final long millis;
		if (object instanceof Long) {
			millis = (Long) object;
		} else {
			// use 'provided' Joda time
			final DateTime value = (DateTime) object;
			millis = value.toDate().getTime();
		}
		return new Timestamp(millis - LOCAL_TZ.getOffset(millis));
	}

	public static Object[] convertToObjectArray(Schema elementSchema, TypeInformation<?> elementInfo, Object object) {
		final List<?> list = (List<?>) object;
		final Object[] convertedArray = (Object[]) Array.newInstance(
			elementInfo.getTypeClass(),
			list.size());
		for (int i = 0; i < list.size(); i++) {
			convertedArray[i] = convertAvroType(elementSchema, elementInfo, list.get(i));
		}
		return convertedArray;
	}

	// --------------------------------------------------------------------------------------------

	public static GenericRecord convertRowToAvroRecord(Schema schema, Row row) {
		final List<Schema.Field> fields = schema.getFields();
		final int length = fields.size();
		final GenericRecord record = new GenericData.Record(schema);
		for (int i = 0; i < length; i++) {
			final Schema.Field field = fields.get(i);
			record.put(i, convertFlinkType(field.schema(), row.getField(i)));
		}
		return record;
	}

	public static Object convertFlinkType(Schema schema, Object object) {
		if (object == null) {
			return null;
		}
		switch (schema.getType()) {
			case RECORD:
				if (object instanceof Row) {
					return convertRowToAvroRecord(schema, (Row) object);
				}
				throw new IllegalStateException("Row expected but was: " + object.getClass());
			case ENUM:
				return new GenericData.EnumSymbol(schema, object.toString());
			case ARRAY:
				final Schema elementSchema = schema.getElementType();
				final Object[] array = (Object[]) object;
				final GenericData.Array<Object> convertedArray = new GenericData.Array<>(array.length, schema);
				for (Object element : array) {
					convertedArray.add(convertFlinkType(elementSchema, element));
				}
				return convertedArray;
			case MAP:
				final Map<?, ?> map = (Map<?, ?>) object;
				final Map<Utf8, Object> convertedMap = new HashMap<>();
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					convertedMap.put(
						new Utf8(entry.getKey().toString()),
						convertFlinkType(schema.getValueType(), entry.getValue()));
				}
				return convertedMap;
			case UNION:
				final List<Schema> types = schema.getTypes();
				final int size = types.size();
				final Schema actualSchema;
				if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
					actualSchema = types.get(1);
				} else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
					actualSchema = types.get(0);
				} else if (size == 1) {
					actualSchema = types.get(0);
				} else {
					// generic type
					return object;
				}
				return convertFlinkType(actualSchema, object);
			case FIXED:
				// check for logical type
				if (object instanceof BigDecimal) {
					return new GenericData.Fixed(
						schema,
						convertFromDecimal(schema, (BigDecimal) object));
				}
				return new GenericData.Fixed(schema, (byte[]) object);
			case STRING:
				return new Utf8(object.toString());
			case BYTES:
				// check for logical type
				if (object instanceof BigDecimal) {
					return ByteBuffer.wrap(convertFromDecimal(schema, (BigDecimal) object));
				}
				return ByteBuffer.wrap((byte[]) object);
			case INT:
				// check for logical types
				if (object instanceof Date) {
					return convertFromDate(schema, (Date) object);
				} else if (object instanceof Time) {
					return convertFromTime(schema, (Time) object);
				}
				return object;
			case LONG:
				// check for logical type
				if (object instanceof Timestamp) {
					return convertFromTimestamp(schema, (Timestamp) object);
				}
				return object;
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				return object;
		}
		throw new RuntimeException("Unsupported Avro type:" + schema);
	}

	public static byte[] convertFromDecimal(Schema schema, BigDecimal decimal) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType instanceof LogicalTypes.Decimal) {
			final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
			// rescale to target type
			final BigDecimal rescaled = decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
			// byte array must contain the two's-complement representation of the
			// unscaled integer value in big-endian byte order
			return decimal.unscaledValue().toByteArray();
		} else {
			throw new RuntimeException("Unsupported decimal type.");
		}
	}

	public static int convertFromDate(Schema schema, Date date) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.date()) {
			// adopted from Apache Calcite
			final long time = date.getTime();
			final long converted = time + (long) LOCAL_TZ.getOffset(time);
			return (int) (converted / 86400000L);
		} else {
			throw new RuntimeException("Unsupported date type.");
		}
	}

	public static int convertFromTime(Schema schema, Time date) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.timeMillis()) {
			// adopted from Apache Calcite
			final long time = date.getTime();
			final long converted = time + (long) LOCAL_TZ.getOffset(time);
			return (int) (converted % 86400000L);
		} else {
			throw new RuntimeException("Unsupported time type.");
		}
	}

	public static long convertFromTimestamp(Schema schema, Timestamp date) {
		final LogicalType logicalType = schema.getLogicalType();
		if (logicalType == LogicalTypes.timestampMillis()) {
			// adopted from Apache Calcite
			final long time = date.getTime();
			return time + (long) LOCAL_TZ.getOffset(time);
		} else {
			throw new RuntimeException("Unsupported timestamp type.");
		}
	}
}
