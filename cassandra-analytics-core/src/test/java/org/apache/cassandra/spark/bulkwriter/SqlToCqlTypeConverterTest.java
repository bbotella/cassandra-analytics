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

package org.apache.cassandra.spark.bulkwriter;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.spark.data.CqlField;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.ASCII;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BIGINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BLOB;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BOOLEAN;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.COUNTER;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DECIMAL;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DOUBLE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.FLOAT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INET;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.SMALLINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TEXT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIME;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMESTAMP;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMEUUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TINYINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.UUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARINT;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlCustom;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockListCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockMapCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockSetCqlType;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

@RunWith(Enclosed.class)
public final class SqlToCqlTypeConverterTest
{
    private SqlToCqlTypeConverterTest()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @RunWith(Parameterized.class)
    public static class ConverterTests
    {
        public ConverterTests(CqlField.CqlType dt, Class expectedConverter)
        {
            this.dt = dt;
            this.expectedConverter = expectedConverter;
        }

        @Parameterized.Parameters(name = "{index}: {0} -> {1}")
        public static Collection<Object[]> dataTypes()
        {
            return Arrays.asList(na(mockCqlType(BIGINT), SqlToCqlTypeConverter.LongConverter.class),
                                 na(mockCqlType(BLOB), SqlToCqlTypeConverter.BytesConverter.class),
                                 na(mockListCqlType(INT), SqlToCqlTypeConverter.ListConverter.class),
                                 na(mockCqlType(DECIMAL), SqlToCqlTypeConverter.BigDecimalConverter.class),
                                 na(mockCqlType(TIMESTAMP), SqlToCqlTypeConverter.TimestampConverter.class),
                                 na(mockCqlType(TIME), SqlToCqlTypeConverter.TimeConverter.class),
                                 na(mockCqlType(UUID), SqlToCqlTypeConverter.UUIDConverter.class),
                                 na(mockCqlType(VARINT), SqlToCqlTypeConverter.BigIntegerConverter.class),
                                 na(mockCqlType(TIMEUUID), SqlToCqlTypeConverter.TimeUUIDConverter.class),
                                 na(mockCqlType(INET), SqlToCqlTypeConverter.InetAddressConverter.class),
                                 na(mockCqlType(DATE), SqlToCqlTypeConverter.DateConverter.class),
                                 na(mockMapCqlType(INT, INT), SqlToCqlTypeConverter.MapConverter.class),
                                 na(mockSetCqlType(INET), SqlToCqlTypeConverter.SetConverter.class),
                                 // Special Cassandra 1.2 Timestamp type should use TimestampConverter
                                 na(mockCqlCustom("org.apache.cassandra.db.marshal.DateType"), SqlToCqlTypeConverter.TimestampConverter.class));
        }

        private CqlField.CqlType dt;
        private Class expectedConverter;

        @Test
        public void testConverter()
        {
            assertConverterType(dt, expectedConverter);
        }

        private static Object[] na(CqlField.CqlType dataType, Class converter)
        {
            return new Object[]{dataType, converter};
        }
    }

    @RunWith(Parameterized.class)
    public static class TypeConverterNoOpTests
    {
        @Parameterized.Parameters(name = "{index}: {0}")
        public static Collection<Object[]> dataTypes()
        {
            return Arrays.asList(na(mockCqlType(ASCII)),
                                 na(mockCqlType(BOOLEAN)),
                                 na(mockCqlType(COUNTER)),
                                 na(mockCqlType(DOUBLE)),
                                 na(mockCqlType(FLOAT)),
                                 na(mockCqlType(TEXT)),
                                 na(mockCqlType(VARCHAR)),
                                 na(mockCqlType(SMALLINT)),
                                 na(mockCqlType(TINYINT)));
        }

        @Parameterized.Parameter
        public CqlField.CqlType dataType;  // CHECKSTYLE IGNORE: Public mutable field for parameterized testing

        private static Object[] na(CqlField.CqlType dataType)
        {
            return new Object[]{dataType};
        }

        @Test
        public void testNoOpTypes()
        {
            assertConverterType(dataType, SqlToCqlTypeConverter.NoOp.class);
        }
    }

    public static void assertConverterType(CqlField.CqlType dataType, Class expectedType)
    {
        assertThat(SqlToCqlTypeConverter.getConverter(dataType), instanceOf(expectedType));
    }
}
