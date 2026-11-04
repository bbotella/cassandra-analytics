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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.SSTableWriter;
import org.apache.cassandra.spark.data.BridgeUdtValue;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.RowData;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.utils.UUIDs;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CASSANALYTICS-35: the bulk writer must support every partition key type that Cassandra allows.
 * <p>
 * TRIGGER: a table whose partition key column has a type that the writer could not tokenize, such as
 * {@code smallint}, {@code date}, a frozen collection or a frozen UDT.
 * <p>
 * HARNESS: the real bridge for each supported Cassandra version. Each row is normalized by the writer's
 * converters, keyed by a {@link Tokenizer} that went through Java serialization like Spark closures do, and
 * written to an SSTable through the bridge's {@link SSTableWriter}, which wraps Cassandra's
 * {@code CQLSSTableWriter}, in the order of the computed keys.
 * <p>
 * ORACLE: for every row, the token and partition key bytes computed by {@link Tokenizer#getDecoratedKey} are
 * equal to the token and partition key that Cassandra wrote to the SSTable. Before the fix, building the
 * table schema failed with {@code RuntimeException: Could not find ColumnType for type name <type>}.
 */
class TokenizerTest
{
    private static final String KEYSPACE = "tokenizer_test";
    // Cassandra keeps a single schema per JVM, so every test case gets its own table
    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    private static final StructType SIMPLE_UDT_SCHEMA = new StructType().add("a", DataTypes.IntegerType)
                                                                        .add("b", DataTypes.StringType);
    private static final StructType NESTED_UDT_SCHEMA = new StructType().add("id", DataTypes.IntegerType)
                                                                        .add("tags", DataTypes.createArrayType(DataTypes.StringType))
                                                                        .add("inner", SIMPLE_UDT_SCHEMA);

    @TempDir
    private Path tempDir;

    static Stream<Arguments> partitionKeyTypes()
    {
        List<Arguments> arguments = new ArrayList<>();
        for (String version : CassandraVersion.supportedVersions())
        {
            int versionNumber = CassandraBridgeFactory.get(version).getVersion().versionNumber();
            for (KeyTypeCase keyType : keyTypeCases())
            {
                if (versionNumber >= keyType.minimumVersion.versionNumber())
                {
                    arguments.add(Arguments.of(version, keyType));
                }
            }
        }
        return arguments.stream();
    }

    static Stream<Arguments> versions()
    {
        return Arrays.stream(CassandraVersion.supportedVersions()).map(Arguments::of);
    }

    /**
     * Every type that Cassandra accepts in a primary key: all native types except {@code counter} and
     * {@code duration}, and frozen collections, tuples and UDTs, nested in each other. Values are given in the
     * form Spark passes them to the writer.
     */
    static List<KeyTypeCase> keyTypeCases()
    {
        return Arrays.asList(
        new KeyTypeCase("ascii", "a", "b c", "~"),
        new KeyTypeCase("bigint", Long.MIN_VALUE, -1L, 0L, 42L, Long.MAX_VALUE),
        new KeyTypeCase("blob", new byte[]{0}, new byte[]{1, 2, 3}, new byte[]{(byte) 0xFF, 0x00}),
        new KeyTypeCase("boolean", true, false),
        new KeyTypeCase("date", new Date(0L), new Date(TimeUnit.DAYS.toMillis(365)), new Date(-TimeUnit.DAYS.toMillis(30))),
        new KeyTypeCase("decimal", new BigDecimal("1.5"), new BigDecimal("-0.001"), new BigDecimal("12345678901234567890.123")),
        new KeyTypeCase("double", -1.5d, 0.0d, Double.MAX_VALUE, Double.MIN_VALUE),
        new KeyTypeCase("float", -1.5f, 0.0f, Float.MAX_VALUE, Float.MIN_VALUE),
        new KeyTypeCase("inet", "127.0.0.1", "10.1.2.3", "::1", "2001:db8::ff00:42:8329"),
        new KeyTypeCase("int", Integer.MIN_VALUE, -1, 0, 7, Integer.MAX_VALUE),
        new KeyTypeCase("smallint", Short.MIN_VALUE, (short) -1, (short) 0, Short.MAX_VALUE),
        new KeyTypeCase("text", "a", "bb", "ünïcödé", "😀"),
        new KeyTypeCase("time", 0L, 1L, TimeUnit.HOURS.toNanos(13) + 5, TimeUnit.DAYS.toNanos(1) - 1),
        new KeyTypeCase("timestamp", new Timestamp(0L), new Timestamp(1_700_000_000_123L), new Timestamp(-1_000L)),
        new KeyTypeCase("timeuuid", UUIDs.startOf(0L).toString(), UUIDs.startOf(1_700_000_000_000L).toString()),
        new KeyTypeCase("tinyint", Byte.MIN_VALUE, (byte) -1, (byte) 0, Byte.MAX_VALUE),
        new KeyTypeCase("uuid", "00000000-0000-0000-0000-000000000000", "123e4567-e89b-42d3-a456-556642440000",
                        "ffffffff-ffff-4fff-bfff-ffffffffffff"),
        new KeyTypeCase("varchar", "x", "yz"),
        new KeyTypeCase("varint", BigInteger.ZERO, new BigInteger("-98765432109876543210"), 42L),
        new KeyTypeCase("frozen<list<int>>", list(1, 2, 3), list(3, 2, 1), list()),
        new KeyTypeCase("frozen<set<text>>", set("banana", "apple"), set("cherry"), set()),
        new KeyTypeCase("frozen<map<text, int>>", map("banana", 2, "apple", 1), map("cherry", 3), map()),
        new KeyTypeCase("frozen<tuple<int, text>>", RowFactory.create(1, "a"), RowFactory.create(2, null)),
        new KeyTypeCase("frozen<simple_udt>", simpleUdt(1, "a"), simpleUdt(2, null)),
        new KeyTypeCase("frozen<nested_udt>",
                        nestedUdt(1, set("banana", "apple", "cherry"), simpleUdt(1, "a")),
                        nestedUdt(2, set(), simpleUdt(2, "b"))),
        new KeyTypeCase("frozen<list<frozen<set<int>>>>", list(set(3, 1, 2), set()), list(set(4))),
        new KeyTypeCase("frozen<map<int, frozen<tuple<int, text>>>>",
                        map(1, RowFactory.create(1, "a"), 2, RowFactory.create(2, "b")),
                        map(3, RowFactory.create(3, "c"))),
        new KeyTypeCase("frozen<set<frozen<simple_udt>>>", set(simpleUdt(1, "a"), simpleUdt(2, "b")), set(simpleUdt(3, "c"))),
        new KeyTypeCase("frozen<tuple<int, frozen<simple_udt>>>",
                        RowFactory.create(1, simpleUdt(1, "a")),
                        RowFactory.create(2, simpleUdt(2, "b"))),
        new KeyTypeCase(CassandraVersion.FIVEZERO, "vector<float, 3>", list(1.0f, 2.0f, 3.0f), list(-1.0f, 0.0f, 1.0f)));
    }

    @ParameterizedTest(name = "{index}: version={0} partition key type={1}")
    @MethodSource("partitionKeyTypes")
    void singlePartitionKey(String version, KeyTypeCase keyType) throws IOException
    {
        TestTable table = new TestTable(KEYSPACE, "pk %s, value int, PRIMARY KEY (pk)", keyType.cqlType, "pk", "value");
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < keyType.values.size(); i++)
        {
            rows.add(new Object[]{keyType.values.get(i), i});
        }
        assertTokenizerMatchesCassandra(version, Partitioner.Murmur3Partitioner, table, rows);
    }

    @ParameterizedTest(name = "{index}: version={0} partition key type={1}")
    @MethodSource("partitionKeyTypes")
    void compositePartitionKey(String version, KeyTypeCase keyType) throws IOException
    {
        TestTable table = new TestTable(KEYSPACE, "pk1 %1$s, pk2 %1$s, value int, PRIMARY KEY ((pk1, pk2))",
                                        keyType.cqlType, "pk1", "pk2", "value");
        List<Object[]> rows = new ArrayList<>();
        int size = keyType.values.size();
        for (int i = 0; i < size; i++)
        {
            rows.add(new Object[]{keyType.values.get(i), keyType.values.get((i + 1) % size), i});
        }
        assertTokenizerMatchesCassandra(version, Partitioner.Murmur3Partitioner, table, rows);
    }

    @ParameterizedTest(name = "{index}: version={0} clustering key type={1}")
    @MethodSource("partitionKeyTypes")
    void clusteringKeyDoesNotChangePartitionKey(String version, KeyTypeCase keyType) throws IOException
    {
        // The data frame columns are not in table order, and the clustering values are not sorted
        TestTable table = new TestTable(KEYSPACE, "pk int, ck %s, value int, PRIMARY KEY (pk, ck)",
                                        keyType.cqlType, "value", "ck", "pk");
        List<Object[]> rows = new ArrayList<>();
        for (int i = keyType.values.size() - 1; i >= 0; i--)
        {
            rows.add(new Object[]{i, keyType.values.get(i), i % 2});
        }
        assertTokenizerMatchesCassandra(version, Partitioner.Murmur3Partitioner, table, rows);
    }

    @ParameterizedTest(name = "{index}: version={0}")
    @MethodSource("versions")
    void randomPartitioner(String version) throws IOException
    {
        String keyspace = KEYSPACE + "_random";
        for (KeyTypeCase keyType : Arrays.asList(new KeyTypeCase("int", -1, 0, 1),
                                                 new KeyTypeCase("frozen<set<text>>", set("banana", "apple"), set()),
                                                 new KeyTypeCase("frozen<simple_udt>", simpleUdt(1, "a"), simpleUdt(2, "b"))))
        {
            TestTable table = new TestTable(keyspace, "pk1 %1$s, pk2 int, value int, PRIMARY KEY ((pk1, pk2))",
                                            keyType.cqlType, "pk1", "pk2", "value");
            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < keyType.values.size(); i++)
            {
                rows.add(new Object[]{keyType.values.get(i), i, i});
            }
            assertTokenizerMatchesCassandra(version, Partitioner.RandomPartitioner, table, rows);
        }
    }

    @ParameterizedTest(name = "{index}: version={0}")
    @MethodSource("versions")
    void compositePartitionKeyWithEmptyComponent(String version) throws IOException
    {
        // Only a whole partition key may not be empty, so a composite key can have empty components
        TestTable table = new TestTable(KEYSPACE, "pk1 %1$s, pk2 %1$s, value int, PRIMARY KEY ((pk1, pk2))",
                                        "text", "pk1", "pk2", "value");
        List<Object[]> rows = Arrays.asList(new Object[]{"", "", 0}, new Object[]{"", "a", 1}, new Object[]{"a", "", 2});
        assertTokenizerMatchesCassandra(version, Partitioner.Murmur3Partitioner, table, rows);
    }

    @ParameterizedTest(name = "{index}: version={0}")
    @MethodSource("versions")
    void emptyPartitionKeyIsRejected(String version)
    {
        TestTable table = new TestTable(KEYSPACE, "pk %s, value int, PRIMARY KEY (pk)", "text", "pk", "value");
        TestSchema schema = new TestSchema(version, Partitioner.Murmur3Partitioner, table);
        Object[] row = schema.normalize(new Object[]{"", 1});

        assertThatThrownBy(() -> schema.tokenizer().getDecoratedKey(row))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Key may not be empty");
    }

    @ParameterizedTest(name = "{index}: version={0}")
    @MethodSource("versions")
    void oversizedPartitionKeyIsRejected(String version)
    {
        TestTable table = new TestTable(KEYSPACE, "pk %s, value int, PRIMARY KEY (pk)", "blob", "pk", "value");
        TestSchema schema = new TestSchema(version, Partitioner.Murmur3Partitioner, table);
        Object[] row = schema.normalize(new Object[]{new byte[65536], 1});

        assertThatThrownBy(() -> schema.tokenizer().getDecoratedKey(row))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Key length of 65536 is longer than maximum of 65535");
    }

    @ParameterizedTest(name = "{index}: version={0}")
    @MethodSource("versions")
    void nullPartitionKeyIsRejected(String version)
    {
        TestTable table = new TestTable(KEYSPACE, "pk %s, value int, PRIMARY KEY (pk)", "frozen<simple_udt>", "pk", "value");
        TestSchema schema = new TestSchema(version, Partitioner.Murmur3Partitioner, table);
        Object[] row = schema.normalize(new Object[]{null, 1});

        assertThatThrownBy(() -> schema.tokenizer().getDecoratedKey(row))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("All key columns must be non-null");
    }

    private void assertTokenizerMatchesCassandra(String version,
                                                 Partitioner partitioner,
                                                 TestTable table,
                                                 List<Object[]> sparkRows) throws IOException
    {
        TestSchema schema = new TestSchema(version, partitioner, table);
        Tokenizer driverTokenizer = schema.tokenizer();
        // Spark serializes the tokenizer to the executors, which compute the keys
        Tokenizer executorTokenizer = javaSerializationRoundTrip(driverTokenizer);
        CqlWriterValueConverter valueConverter = new CqlWriterValueConverter(schema.cqlTable, schema.bridge.getVersion());

        Map<ByteBuffer, BigInteger> expected = new HashMap<>();
        List<Map.Entry<DecoratedKey, Object[]>> keyedRows = new ArrayList<>();
        for (Object[] sparkRow : sparkRows)
        {
            Object[] row = schema.normalize(sparkRow);
            DecoratedKey decoratedKey = executorTokenizer.getDecoratedKey(row);
            assertThat(structure(row)).as("the tokenizer must not modify the row it keys")
                                      .isEqualTo(structure(schema.normalize(sparkRow)));
            assertThat(driverTokenizer.getDecoratedKey(row)).isEqualTo(decoratedKey);
            keyedRows.add(new AbstractMap.SimpleEntry<>(decoratedKey, row));
            expected.put(decoratedKey.getKey(), decoratedKey.getToken());
        }

        // Rows reach the SSTable writer in the order of their decorated keys, as in a bulk write job.
        // The SSTable writer fails if that order does not match the order of the keys Cassandra computes.
        keyedRows.sort(Map.Entry.comparingByKey());
        Path directory = Files.createDirectories(tempDir.resolve(table.name));
        try (SSTableWriter writer = schema.bridge.getSSTableWriter(directory.toString(),
                                                                   partitioner.name(),
                                                                   table.createStatement,
                                                                   schema.tableSchema.modificationStatement,
                                                                   table.udtStatements,
                                                                   128))
        {
            writer.setSSTablesProducedListener(descriptors -> { });
            for (Map.Entry<DecoratedKey, Object[]> keyedRow : keyedRows)
            {
                Map<String, Object> values = new HashMap<>();
                for (int column = 0; column < table.columnNames.length; column++)
                {
                    String columnName = table.columnNames[column];
                    values.put(columnName, valueConverter.convert(columnName, keyedRow.getValue()[column]));
                }
                writer.addRow(values);
            }
        }

        assertThat(readPartitionKeys(schema.bridge.getVersion(), partitioner, table, directory)).isEqualTo(expected);
    }

    private static Map<ByteBuffer, BigInteger> readPartitionKeys(CassandraVersion version,
                                                                 Partitioner partitioner,
                                                                 TestTable table,
                                                                 Path directory) throws IOException
    {
        LocalDataLayer dataLayer = new LocalDataLayer(version,
                                                      partitioner,
                                                      table.keyspace,
                                                      table.createStatement,
                                                      table.udtStatements,
                                                      Collections.emptyList(),
                                                      false,
                                                      null,
                                                      SSTableTimeRangeFilter.ALL,
                                                      directory.toString());
        Map<ByteBuffer, BigInteger> partitionKeys = new HashMap<>();
        try (StreamScanner<RowData> scanner = dataLayer.openCompactionScanner(0, Collections.emptyList(), null))
        {
            while (scanner.next())
            {
                RowData rowData = scanner.data();
                partitionKeys.put(copy(rowData.getPartitionKey()), rowData.getToken());
                scanner.advanceToNextColumn();
            }
        }
        return partitionKeys;
    }

    private static Tokenizer javaSerializationRoundTrip(Tokenizer tokenizer) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes))
        {
            output.writeObject(tokenizer);
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            return (Tokenizer) input.readObject();
        }
        catch (ClassNotFoundException exception)
        {
            throw new AssertionError(exception);
        }
    }

    /**
     * @return {@code value} with arrays turned into lists and UDT values turned into maps, at any depth, so that
     * normalized rows can be compared with {@link Object#equals}. A value that the bridge's SSTable writer accepts,
     * such as a UDT or tuple value of the bridge, is kept as it is and is not equal to its normalized form.
     */
    private static Object structure(Object value)
    {
        if (value instanceof Object[])
        {
            return Arrays.stream((Object[]) value).map(TokenizerTest::structure).collect(Collectors.toList());
        }
        if (value instanceof List)
        {
            return ((List<?>) value).stream().map(TokenizerTest::structure).collect(Collectors.toList());
        }
        if (value instanceof Set)
        {
            return ((Set<?>) value).stream().map(TokenizerTest::structure).collect(Collectors.toSet());
        }
        if (value instanceof Map)
        {
            Map<Object, Object> map = new HashMap<>();
            ((Map<?, ?>) value).forEach((key, entry) -> map.put(structure(key), structure(entry)));
            return map;
        }
        if (value instanceof BridgeUdtValue)
        {
            BridgeUdtValue udtValue = (BridgeUdtValue) value;
            return Arrays.asList(udtValue.name, structure(udtValue.udtMap));
        }
        return value;
    }

    private static ByteBuffer copy(ByteBuffer buffer)
    {
        ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
        copy.put(buffer.duplicate());
        copy.flip();
        return copy;
    }

    private static Row simpleUdt(int a, String b)
    {
        return new GenericRowWithSchema(new Object[]{a, b}, SIMPLE_UDT_SCHEMA);
    }

    private static Row nestedUdt(int id, Set<String> tags, Row inner)
    {
        return new GenericRowWithSchema(new Object[]{id, tags, inner}, NESTED_UDT_SCHEMA);
    }

    @SafeVarargs
    private static <T> List<T> list(T... values)
    {
        return Arrays.asList(values);
    }

    @SafeVarargs
    private static <T> Set<T> set(T... values)
    {
        return new HashSet<>(Arrays.asList(values));
    }

    private static Map<Object, Object> map(Object... keysAndValues)
    {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2)
        {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    /**
     * A table with a unique name, and the order of its columns in the data frame being written
     */
    private static final class TestTable
    {
        private static final Set<String> UDT_STATEMENTS = new HashSet<>(Arrays.asList(
        "CREATE TYPE %s.simple_udt (a int, b text);",
        "CREATE TYPE %s.nested_udt (id int, tags set<text>, inner frozen<simple_udt>);"));

        final String keyspace;
        final String name;
        final String createStatement;
        final Set<String> udtStatements;
        final String[] columnNames;

        TestTable(String keyspace, String columnsFormat, String cqlType, String... columnNames)
        {
            this.keyspace = keyspace;
            this.name = "table_" + TABLE_COUNTER.incrementAndGet();
            this.createStatement = String.format("CREATE TABLE %s.%s (%s);",
                                                 keyspace, name, String.format(columnsFormat, cqlType));
            this.udtStatements = UDT_STATEMENTS.stream()
                                               .map(statement -> String.format(statement, keyspace))
                                               .collect(Collectors.toCollection(HashSet::new));
            this.columnNames = columnNames;
        }
    }

    /**
     * The writer's view of a {@link TestTable}, built the way a bulk write job builds it on the driver
     */
    private static final class TestSchema
    {
        final CassandraBridge bridge;
        final CqlTable cqlTable;
        final TableSchema tableSchema;
        final Partitioner partitioner;
        final TestTable table;

        TestSchema(String version, Partitioner partitioner, TestTable table)
        {
            this.bridge = CassandraBridgeFactory.get(version);
            this.partitioner = partitioner;
            this.table = table;
            this.cqlTable = bridge.buildSchema(table.createStatement, table.keyspace,
                                               RecordWriter.IGNORED_REPLICATION_FACTOR, partitioner, table.udtStatements);
            StructType dataFrameSchema = new StructType();
            for (String columnName : table.columnNames)
            {
                // Only the field names of the data frame schema are used to build the table schema
                dataFrameSchema = dataFrameSchema.add(columnName, DataTypes.BinaryType);
            }
            this.tableSchema = new TableSchema(dataFrameSchema,
                                               new CqlTableInfoProvider(table.createStatement, cqlTable),
                                               WriteMode.INSERT,
                                               TTLOption.forever(),
                                               TimestampOption.now(),
                                               bridge.getVersion(),
                                               false,
                                               false);
        }

        Tokenizer tokenizer()
        {
            BroadcastableSchemaInfo schemaInfo = BroadcastableSchemaInfo.from(new SchemaInfo()
            {
                @Override
                public TableSchema getTableSchema()
                {
                    return tableSchema;
                }

                @NotNull
                @Override
                public Set<String> getUserDefinedTypeStatements()
                {
                    return table.udtStatements;
                }
            });
            return new Tokenizer(schemaInfo, table.keyspace, partitioner);
        }

        Object[] normalize(Object[] sparkRow)
        {
            return BroadcastableTableSchema.from(tableSchema).normalize(sparkRow.clone());
        }
    }

    static final class KeyTypeCase
    {
        final CassandraVersion minimumVersion;
        final String cqlType;
        final List<Object> values;

        KeyTypeCase(String cqlType, Object... values)
        {
            this(CassandraVersion.FOURZERO, cqlType, values);
        }

        KeyTypeCase(CassandraVersion minimumVersion, String cqlType, Object... values)
        {
            this.minimumVersion = minimumVersion;
            this.cqlType = cqlType;
            this.values = Arrays.asList(values);
        }

        @Override
        public String toString()
        {
            return cqlType;
        }
    }
}
