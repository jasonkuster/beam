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
package org.apache.beam.sdk.io.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.postgresql.ds.PGSimpleDataSource;


/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>This test requires a running instance of Postgres, and the test dataset must exist in the
 * database. `JdbcTestDataSet` will create the read table.
 *
 * <p>You can run just this test by doing the following:
 * <pre>
 * mvn test-compile compile failsafe:integration-test -D beamTestPipelineOptions='[
 * "--postgresServerName=1.2.3.4",
 * "--postgresUsername=postgres",
 * "--postgresDatabaseName=myfancydb",
 * "--postgresPassword=yourpassword",
 * "--postgresSsl=false"
 * ]' -DskipITs=false -Dit.test=org.apache.beam.sdk.io.jdbc.JdbcIOIT -DfailIfNoTests=false
 * </pre>
 */
public class JdbcIOITReadPipeline {
  private static PGSimpleDataSource dataSource;


  private static class CreateKVOfNameAndId implements JdbcIO.RowMapper<KV<String, Integer>> {
    @Override
    public KV<String, Integer> mapRow(ResultSet resultSet) throws Exception {
      KV<String, Integer> kv =
          KV.of(resultSet.getString("name"), resultSet.getInt("id"));
      return kv;
    }
  }

  /**
   * Does a test read of a few rows from a postgres database.
   *
   * <p>Note that IT read tests must not do any data table manipulation (setup/clean up.)
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    PostgresTestOptions options = PipelineOptionsFactory.fromArgs().withValidation()
        .as(PostgresTestOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    dataSource = JdbcTestDataSet.getDataSource(options);
    String tableName = JdbcTestDataSet.READ_TABLE_NAME;


    PCollection<KV<String, Integer>> output = pipeline.apply(JdbcIO.<KV<String, Integer>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withQuery("select name,id from " + tableName)
            .withRowMapper(new CreateKVOfNameAndId())
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    // TODO: validate actual contents of rows, not just count.
    PAssert.thatSingleton(
        output.apply("Count All", Count.<KV<String, Integer>>globally()))
        .isEqualTo(1000L);

    List<KV<String, Long>> expectedCounts = new ArrayList<>();
    for (String scientist : JdbcTestDataSet.SCIENTISTS) {
      expectedCounts.add(KV.of(scientist, 100L));
    }
    PAssert.that(output.apply("Count Scientist", Count.<String, Integer>perKey()))
        .containsInAnyOrder(expectedCounts);

    pipeline.run().waitUntilFinish();
  }
}
