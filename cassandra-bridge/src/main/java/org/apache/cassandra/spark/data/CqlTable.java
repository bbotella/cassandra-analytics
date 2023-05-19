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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraBridge;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({ "WeakerAccess", "unused" })
public class CqlTable implements Serializable
{
    public static final long serialVersionUID = 42L;

    private final ReplicationFactor replicationFactor;
    private final String keyspace;
    private final String table;
    private final String createStatement;
    private final List<CqlField> fields;
    private final Set<CqlField.CqlUdt> udts;

    private final Map<String, CqlField> fieldsMap;
    private final List<CqlField> partitionKeys;
    private final List<CqlField> clusteringKeys;
    private final List<CqlField> staticColumns;
    private final List<CqlField> valueColumns;
    private final transient Map<String, CqlField> columns;
    private final int indexCount;

    public CqlTable(@NotNull String keyspace,
                    @NotNull String table,
                    @NotNull String createStatement,
                    @NotNull ReplicationFactor replicationFactor,
                    @NotNull List<CqlField> fields)
    {
        this(keyspace, table, createStatement, replicationFactor, fields, Collections.emptySet(), 0);
    }

    public CqlTable(@NotNull String keyspace,
                    @NotNull String table,
                    @NotNull String createStatement,
                    @NotNull ReplicationFactor replicationFactor,
                    @NotNull List<CqlField> fields,
                    @NotNull Set<CqlField.CqlUdt> udts,
                    int indexCount)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.createStatement = createStatement;
        this.replicationFactor = replicationFactor;
        this.fields = fields.stream().sorted().collect(Collectors.toList());
        this.fieldsMap = this.fields.stream().collect(Collectors.toMap(CqlField::name, Function.identity()));
        this.partitionKeys = this.fields.stream().filter(CqlField::isPartitionKey).sorted().collect(Collectors.toList());
        this.clusteringKeys = this.fields.stream().filter(CqlField::isClusteringColumn).sorted().collect(Collectors.toList());
        this.staticColumns = this.fields.stream().filter(CqlField::isStaticColumn).sorted().collect(Collectors.toList());
        this.valueColumns = this.fields.stream().filter(CqlField::isValueColumn).sorted().collect(Collectors.toList());
        this.udts = Collections.unmodifiableSet(udts);
        this.indexCount = indexCount;

        // We use a linked hashmap to guarantee ordering of a 'SELECT * FROM ...'
        this.columns = new LinkedHashMap<>();
        for (CqlField column : partitionKeys)
        {
            columns.put(column.name(), column);
        }
        for (CqlField column : clusteringKeys)
        {
            columns.put(column.name(), column);
        }
        for (CqlField column : staticColumns)
        {
            columns.put(column.name(), column);
        }
        for (CqlField column : valueColumns)
        {
            columns.put(column.name(), column);
        }
    }

    public ReplicationFactor replicationFactor()
    {
        return replicationFactor;
    }

    public CqlField column(String columnName)
    {
        return columns.get(columnName);
    }

    public List<CqlField> columns()
    {
        return new ArrayList<>(columns.values());
    }

    public List<CqlField> primaryKey()
    {
        List<CqlField> pk = new ArrayList<>(partitionKeys.size() + clusteringKeys.size());
        pk.addAll(partitionKeys);
        pk.addAll(clusteringKeys);
        return pk;
    }

    public List<CqlField> partitionKeys()
    {
        return partitionKeys;
    }

    public int numPartitionKeys()
    {
        return partitionKeys.size();
    }

    public List<CqlField> clusteringKeys()
    {
        return clusteringKeys;
    }

    public int numClusteringKeys()
    {
        return clusteringKeys.size();
    }

    public int numPrimaryKeyColumns()
    {
        return numPartitionKeys() + numClusteringKeys();
    }

    public int numNonValueColumns()
    {
        return numPartitionKeys() + numClusteringKeys() + numStaticColumns();
    }

    public List<CqlField> valueColumns()
    {
        return valueColumns;
    }

    public int numValueColumns()
    {
        return valueColumns.size();
    }

    public List<CqlField> staticColumns()
    {
        return staticColumns;
    }

    public int numStaticColumns()
    {
        return staticColumns.size();
    }

    public int numFields()
    {
        return fields.size();
    }

    public boolean has(String field)
    {
        return fieldsMap.containsKey(field);
    }

    public List<CqlField> fields()
    {
        return fields;
    }

    public Set<CqlField.CqlUdt> udts()
    {
        return udts;
    }

    public Set<String> udtCreateStmts()
    {
        return udts.stream()
                   .map(udt -> udt.createStatement(keyspace))
                   .collect(Collectors.toSet());
    }

    public CqlField getField(String name)
    {
        return fieldsMap.get(name);
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public String createStatement()
    {
        return createStatement;
    }

    public int indexCount()
    {
        return indexCount;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
               .append(keyspace)
               .append(table)
               .append(createStatement)
               .append(fields)
               .append(udts)
               .toHashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        CqlTable that = (CqlTable) other;
        return new EqualsBuilder()
               .append(this.keyspace, that.keyspace)
               .append(this.table, that.table)
               .append(this.createStatement, that.createStatement)
               .append(this.fields, that.fields)
               .append(this.udts, that.udts)
               .isEquals();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlTable>
    {
        private final CassandraBridge bridge;

        public Serializer(CassandraBridge bridge)
        {
            this.bridge = bridge;
        }

        @Override
        public CqlTable read(Kryo kryo, Input input, Class type)
        {
            String keyspace = input.readString();
            String table = input.readString();
            String createStatement = input.readString();
            ReplicationFactor replicationFactor = kryo.readObject(input, ReplicationFactor.class);
            int numFields = input.readInt();
            List<CqlField> fields = new ArrayList<>(numFields);
            for (int field = 0; field < numFields; field++)
            {
                fields.add(kryo.readObject(input, CqlField.class));
            }
            int numUdts = input.readInt();
            Set<CqlField.CqlUdt> udts = new LinkedHashSet<>(numUdts);
            for (int udt = 0; udt < numUdts; udt++)
            {
                udts.add((CqlField.CqlUdt) CqlField.CqlType.read(input, bridge));
            }
            int indexCount = input.readInt();
            return new CqlTable(keyspace, table, createStatement, replicationFactor, fields, udts, indexCount);
        }

        @Override
        public void write(Kryo kryo, Output output, CqlTable table)
        {
            output.writeString(table.keyspace());
            output.writeString(table.table());
            output.writeString(table.createStatement());
            kryo.writeObject(output, table.replicationFactor());
            List<CqlField> fields = table.fields();
            output.writeInt(fields.size());
            for (CqlField field : fields)
            {
                kryo.writeObject(output, field);
            }
            Set<CqlField.CqlUdt> udts = table.udts();
            output.writeInt(udts.size());
            for (CqlField.CqlUdt udt : udts)
            {
                udt.write(output);
            }
            output.writeInt(table.indexCount());
        }
    }
}
