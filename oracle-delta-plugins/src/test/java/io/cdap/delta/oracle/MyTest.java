/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.delta.oracle;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.SourceColumn;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test
 * */
public class  MyTest {

  private OracleConfig conf = new OracleConfig("localhost",
                                               1521,
                                               "c##xstrm",
                                               "xs",
                                               "ORCLCDB",
                                               "ORCLPDB1",
                                               "dbzxout",
                                               "OJDBC",
                                               "/Users/shifuxu/Desktop/oracle-resources/native-lib",
                                               "jdbc:oracle:thin:@localhost:1521/ORCLCDB");
  private OracleTableRegistry oracleTableRegistry;

  @Before
  public void setUp() {
    oracleTableRegistry = new OracleTableRegistry(conf, null);
  }

  @After
  public void tearDown() {
//    oracleConnection.resetSessionToCdb();
  }

  @Test
  public void testListTables() throws Exception {
    TableList tableList = oracleTableRegistry.listTables();
    System.out.print(tableList.getTables().size());
  }

  @Test
  public void testDescribeTable() throws Exception {
    TableDetail tableDetail = oracleTableRegistry.describeTable(conf.getDbName(), "PRODUCTS");
    System.out.println(tableDetail.getColumns().size());
  }

  @Test
  public void testAssess1FullSupport() throws Exception {
    TableDetail tableDetail = oracleTableRegistry.describeTable(conf.getDbName(), "PRODUCTS");
    OracleTableAssessor assessor = new OracleTableAssessor();
    TableAssessment tableAssessment = assessor.assess(tableDetail);
    System.out.print(tableAssessment.getFeatureProblems().size());
  }

  @Test
  public void testAssess2FullSupport() throws Exception {
    TableDetail tableDetail = oracleTableRegistry.describeTable(conf.getDbName(), "CUSTOMERS");
    OracleTableAssessor assessor = new OracleTableAssessor();
    TableAssessment tableAssessment = assessor.assess(tableDetail);
    System.out.print(tableAssessment.getFeatureProblems().size());
  }

  @Test
  public void testAssess3PartialSupport() throws Exception {
    TableDetail tableDetail = oracleTableRegistry.describeTable(conf.getDbName(), "EVENTS");
    OracleTableAssessor assessor = new OracleTableAssessor();
    TableAssessment tableAssessment = assessor.assess(tableDetail);
    System.out.print(tableAssessment.getFeatureProblems().size());
  }

  @Test
  public void testKeepSelectedColumns() {
    Set<SourceColumn> cols = new HashSet<>();
    cols.add(new SourceColumn("ID"));
    cols.add(new SourceColumn("FIRST_NAME"));
    cols.add(new SourceColumn("LAST_NAME"));
//    cols.add(new SourceColumn("EMAIL"));
    Schema schema = Schema.recordOf("after",
            Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
            Schema.Field.of("FIRST_NAME", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("LAST_NAME", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("EMAIL", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("ID", 123)
      .set("FIRST_NAME", "Bob")
      .set("LAST_NAME", "Alex")
      .set("EMAIL", "bob@gmail.com")
      .build();

    StructuredRecord res = Records.keepSelectedColumns(record, cols);
    System.out.println(res.toString());
  }
}
