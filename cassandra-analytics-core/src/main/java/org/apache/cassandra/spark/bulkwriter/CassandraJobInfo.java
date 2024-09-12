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

import java.util.UUID;

import org.apache.cassandra.spark.bulkwriter.coordinatedwrite.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CassandraJobInfo implements JobInfo
{
    private static final long serialVersionUID = 6140098484732683759L;
    protected final BulkSparkConf conf;
    protected final UUID restoreJobId;
    protected final TokenPartitioner tokenPartitioner;

    public CassandraJobInfo(BulkSparkConf conf, UUID restoreJobId, TokenPartitioner tokenPartitioner)
    {
        this.restoreJobId = restoreJobId;
        this.conf = conf;
        this.tokenPartitioner = tokenPartitioner;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return conf.consistencyLevel;
    }

    @Override
    public String getLocalDC()
    {
        return conf.localDC;
    }

    @Override
    public int sstableDataSizeInMiB()
    {
        return conf.sstableDataSizeInMiB;
    }

    @Override
    public int getCommitBatchSize()
    {
        return conf.commitBatchSize;
    }

    @Override
    public boolean skipExtendedVerify()
    {
        return conf.skipExtendedVerify;
    }

    @Override
    public boolean getSkipClean()
    {
        return conf.getSkipClean();
    }

    @Override
    public DataTransportInfo transportInfo()
    {
        return conf.getTransportInfo();
    }

    @Override
    public int jobKeepAliveMinutes()
    {
        return conf.getJobKeepAliveMinutes();
    }

    @Override
    public long jobTimeoutSeconds()
    {
        return conf.getJobTimeoutSeconds();
    }

    @Override
    public int effectiveSidecarPort()
    {
        return conf.getEffectiveSidecarPort();
    }

    @Override
    public double importCoordinatorTimeoutMultiplier()
    {
        return conf.importCoordinatorTimeoutMultiplier;
    }

    @Nullable
    @Override
    public CoordinatedWriteConf coordinatedWriteConf()
    {
        return conf.coordinatedWriteConf();
    }

    @Override
    public int getCommitThreadsPerInstance()
    {
        return conf.commitThreadsPerInstance;
    }

    @Override
    public UUID getRestoreJobId()
    {
        return restoreJobId;
    }

    @Override
    public String getConfiguredJobId()
    {
        return conf.configuredJobId;
    }

    @Override
    public TokenPartitioner getTokenPartitioner()
    {
        return tokenPartitioner;
    }

    @NotNull
    @Override
    public DigestAlgorithmSupplier digestAlgorithmSupplier()
    {
        return conf.digestAlgorithmSupplier;
    }

    @NotNull
    public QualifiedTableName qualifiedTableName()
    {
        return new QualifiedTableName(conf.keyspace, conf.table, conf.quoteIdentifiers);
    }
}
