/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.delta.sqlserver;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.plugin.common.ColumnEvaluation;
import org.junit.Assert;
import org.junit.Test;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SqlServerTableAssessorTest {

  private static final String SCALE_100_NANOS = "7";
  private static final String SCALE_MICROS = "6";
  private static final String SCALE_MILLIS = "3";
  private static final String DATETIME = "DATETIME";
  private static final String TIME = "TIME";
  private static final String DB = "db";
  private static final String TABLE = "table";
  private static final String SCHEMA = "schema";
  private static final String UPDATED_TIME_COLUMN = "updated_time";
  private static final String UPDATED_AT_COLUMN = "updated_at";

  private SqlServerTableAssessor tableAssessor = new SqlServerTableAssessor();

  @Test
  public void testDateTime2WithNanoScaleMapping() {
    Map<String, String> props = new HashMap<>();
    props.put(SqlServerTableAssessor.TYPE_NAME, SqlServerTableAssessor.DATETIME2);
    props.put(SqlServerTableAssessor.SCALE, SCALE_100_NANOS);
    ColumnDetail columnDetail = new ColumnDetail(UPDATED_AT_COLUMN, JDBCType.TIMESTAMP, true, props);
    TableDetail tableDetail = new TableDetail.Builder(DB, TABLE, SCHEMA)
      .setColumns(Arrays.asList(columnDetail)).build();

    ColumnEvaluation columnEvaluation = SqlServerTableAssessor.evaluateColumn(columnDetail);
    TableAssessment assessment = tableAssessor.assess(tableDetail);

    Assert.assertEquals(1, assessment.getColumns().size());
    ColumnAssessment columnAssessment = assessment.getColumns().get(0);
    Assert.assertEquals(SqlServerTableAssessor.DATETIME2, columnAssessment.getType());
    Assert.assertEquals(ColumnSupport.PARTIAL, columnAssessment.getSupport());
    Assert.assertEquals(Schema.LogicalType.DATETIME,
                        columnEvaluation.getField().getSchema().getNonNullable().getLogicalType());
  }

  @Test
  public void testDateTime2WithMicrosScaleMapping() {
    Map<String, String> props = new HashMap<>();
    props.put(SqlServerTableAssessor.TYPE_NAME, SqlServerTableAssessor.DATETIME2);
    props.put(SqlServerTableAssessor.SCALE, SCALE_MICROS);
    ColumnDetail columnDetail = new ColumnDetail(UPDATED_AT_COLUMN, JDBCType.TIMESTAMP, true, props);
    TableDetail tableDetail = new TableDetail.Builder(DB, TABLE, SCHEMA)
      .setColumns(Arrays.asList(columnDetail)).build();

    ColumnEvaluation columnEvaluation = SqlServerTableAssessor.evaluateColumn(columnDetail);
    TableAssessment assessment = tableAssessor.assess(tableDetail);

    Assert.assertEquals(Schema.LogicalType.DATETIME,
                        columnEvaluation.getField().getSchema().getNonNullable().getLogicalType());
    Assert.assertEquals(1, assessment.getColumns().size());
    Assert.assertEquals(ColumnSupport.YES, assessment.getColumns().get(0).getSupport());
  }

  @Test
  public void testDateTimeMapping() {
    Map<String, String> props = new HashMap<>();
    props.put(SqlServerTableAssessor.TYPE_NAME, DATETIME);
    ColumnDetail columnDetail = new ColumnDetail(UPDATED_AT_COLUMN, JDBCType.TIMESTAMP, true, props);
    TableDetail tableDetail = new TableDetail.Builder(DB, TABLE, SCHEMA)
      .setColumns(Arrays.asList(columnDetail)).build();

    ColumnEvaluation columnEvaluation = SqlServerTableAssessor.evaluateColumn(columnDetail);
    TableAssessment assessment = tableAssessor.assess(tableDetail);

    Assert.assertEquals(Schema.LogicalType.DATETIME,
                        columnEvaluation.getField().getSchema().getNonNullable().getLogicalType());
    Assert.assertEquals(1, assessment.getColumns().size());
    ColumnAssessment columnAssessment = assessment.getColumns().get(0);
    Assert.assertEquals(DATETIME, columnAssessment.getType());
    Assert.assertEquals(ColumnSupport.YES, columnAssessment.getSupport());
  }

  @Test
  public void testTimeWithNanoScaleMapping() {
    Map<String, String> props = new HashMap<>();
    props.put(SqlServerTableAssessor.TYPE_NAME, TIME);
    props.put(SqlServerTableAssessor.SCALE, SCALE_100_NANOS);
    ColumnDetail columnDetail = new ColumnDetail(UPDATED_TIME_COLUMN, JDBCType.TIME, true, props);
    TableDetail tableDetail = new TableDetail.Builder(DB, TABLE, SCHEMA)
      .setColumns(Arrays.asList(columnDetail)).build();

    ColumnEvaluation columnEvaluation = SqlServerTableAssessor.evaluateColumn(columnDetail);
    TableAssessment assessment = tableAssessor.assess(tableDetail);

    Assert.assertEquals(Schema.LogicalType.TIME_MICROS,
                        columnEvaluation.getField().getSchema().getNonNullable().getLogicalType());
    Assert.assertEquals(1, assessment.getColumns().size());
    Assert.assertEquals(ColumnSupport.PARTIAL, assessment.getColumns().get(0).getSupport());
  }

  @Test
  public void testTimeWithMicrosScaleMapping() {
    Map<String, String> props = new HashMap<>();
    props.put(SqlServerTableAssessor.TYPE_NAME, TIME);
    props.put(SqlServerTableAssessor.SCALE, SCALE_MICROS);
    ColumnDetail columnDetail = new ColumnDetail(UPDATED_TIME_COLUMN, JDBCType.TIME, true, props);
    TableDetail tableDetail = new TableDetail.Builder(DB, TABLE, SCHEMA)
      .setColumns(Arrays.asList(columnDetail)).build();

    ColumnEvaluation columnEvaluation = SqlServerTableAssessor.evaluateColumn(columnDetail);
    TableAssessment assessment = tableAssessor.assess(tableDetail);

    Assert.assertEquals(Schema.LogicalType.TIME_MICROS,
                        columnEvaluation.getField().getSchema().getNonNullable().getLogicalType());
    Assert.assertEquals(1, assessment.getColumns().size());
    Assert.assertEquals(ColumnSupport.YES, assessment.getColumns().get(0).getSupport());
  }

  @Test
  public void testTimeWithMillisScaleMapping() {
    Map<String, String> props = new HashMap<>();
    props.put(SqlServerTableAssessor.TYPE_NAME, TIME);
    props.put(SqlServerTableAssessor.SCALE, SCALE_MILLIS);
    ColumnDetail columnDetail = new ColumnDetail(UPDATED_TIME_COLUMN, JDBCType.TIME, true, props);
    TableDetail tableDetail = new TableDetail.Builder(DB, TABLE, SCHEMA)
      .setColumns(Arrays.asList(columnDetail)).build();

    ColumnEvaluation columnEvaluation = SqlServerTableAssessor.evaluateColumn(columnDetail);
    TableAssessment assessment = tableAssessor.assess(tableDetail);

    Assert.assertEquals(Schema.LogicalType.TIME_MILLIS,
                        columnEvaluation.getField().getSchema().getNonNullable().getLogicalType());
    Assert.assertEquals(1, assessment.getColumns().size());
    Assert.assertEquals(ColumnSupport.YES, assessment.getColumns().get(0).getSupport());
  }
}
