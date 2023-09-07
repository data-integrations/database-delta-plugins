/*
 * Copyright (c) 2023.
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
package io.cdap.plugin;

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

/**
 * BQValidation.
 */
public class BQValidation {

  /**
   * Extracts entire data from source and target tables.
   *
   * @param sourceTable table at the source side
   * @param targetTable table at the sink side
   * @return true if the values in source and target side are equal
   */

  public static boolean validateDBToBQRecordValues(String schema, String sourceTable, String targetTable)
    throws SQLException, ClassNotFoundException, IOException, InterruptedException, ParseException {
    List<Object> bigQueryRows = new ArrayList<>();
    List<JsonObject> bigQueryResponse = new ArrayList<>();

    getBigQueryTableData(targetTable, bigQueryRows);

    for (Object rows : bigQueryRows) {
      JsonObject row = new Gson().fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(row);
    }

    String getSourceQuery = "SELECT * FROM " + schema + "." + sourceTable;
    try (Connection connect = MssqlClient.getMssqlConnection();
         ResultSet rsSource = executeQuery(connect, getSourceQuery)) {
      return compareResultSetAndJsonData(rsSource, bigQueryResponse);
    }
  }

  /**
   * Executes the given SQL query on the provided database connection and returns the result set.
   *
   * @param connection The Connection object representing the active database connection.
   * @param query   The SQL query to be executed on the database.
   * @return A ResultSet object containing the data retrieved by the executed query.
   * @throws SQLException If a database access error occurs or the SQL query is invalid.
   */

  public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
    connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE,
                                                  ResultSet.HOLD_CURSORS_OVER_COMMIT);
    return statement.executeQuery(query);
  }

  /**
   * Retrieves the data from a specified BigQuery table and populates it into the provided list of objects.
   *
   * @param table        The name of the BigQuery table to fetch data from.
   * @param bigQueryRows The list to store the fetched BigQuery data.
   */
  private static void getBigQueryTableData(String table, List<Object> bigQueryRows)
    throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON (t) FROM (SELECT * EXCEPT( _row_id, _source_timestamp, _is_deleted, " +
      "_sequence_num) FROM `" + projectId + "." + dataset + "." + table + "`) AS t ORDER BY ID";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
  }

  /**
   * Compares the data in the result set obtained from the MsSQL database with the provided BigQuery JSON objects.
   *
   * @param rsSource     The result set obtained from the MsSQL database.
   * @param bigQueryData The list of BigQuery JSON objects to compare with the result set data.
   * @return True if the result set data matches the BigQuery data, false otherwise.
   * @throws SQLException   If an SQL error occurs during the result set operations.
   * @throws ParseException If an error occurs while parsing the data.
   */
  public static boolean compareResultSetAndJsonData(ResultSet rsSource, List<JsonObject> bigQueryData)
    throws SQLException, ParseException {
    ResultSetMetaData mdSource = rsSource.getMetaData();
    boolean result = false;
    int columnCountSource = mdSource.getColumnCount();

    if (bigQueryData == null) {
      Assert.fail("BigQuery data is null");
      return result;
    }

    //Variable 'jsonObjectIdx' to track the index of the current JsonObject in the bigQueryData list,
    int jsonObjectIdx = 0;
    int columnCountTarget = 0;

    if (bigQueryData.size() > 0) {
      columnCountTarget = bigQueryData.get(jsonObjectIdx).entrySet().size();
    }

    // Compare the number of columns in the source and target
    Assert.assertEquals("Number of columns in source and target are not equal", columnCountSource,
                        columnCountTarget);
    while (rsSource.next()) {
      int currentColumnCount = 1;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);
        switch (columnType) {
          case Types.BIT:
            boolean sourceBitValue = rsSource.getBoolean(currentColumnCount);
            boolean targetBitValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBoolean();
            Assert.assertEquals("Different values found for column : %s", sourceBitValue, targetBitValue);
            break;
          case Types.DECIMAL:
          case Types.NUMERIC:
            BigDecimal sourceDecimalValue = rsSource.getBigDecimal(currentColumnCount);
            BigDecimal targetDecimalValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBigDecimal();
            Assert.assertEquals(String.format("Different values found for column: %s", columnName),
                                sourceDecimalValue.intValue(), targetDecimalValue.intValue());
            break;
          case Types.REAL:
            float sourceRealValue = rsSource.getFloat(currentColumnCount);
            float targetRealValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsFloat();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Float.compare(sourceRealValue, targetRealValue));
            break;

          case Types.DOUBLE:
          case Types.FLOAT:
            double sourceDoubleValue = rsSource.getDouble(currentColumnCount);
            double targetDoubleValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsDouble();
            Assert.assertEquals(String.format("Different values found for column : %s", columnName), 0,
                                Double.compare(sourceDoubleValue, targetDoubleValue));
            break;
          case Types.TIME:
            Time sourceTimeValue = rsSource.getTime(currentColumnCount);
            Time targetTimeValue = Time.valueOf(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", sourceTimeValue, targetTimeValue);
            break;
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            String sourceBinaryValue = new String(Base64.getEncoder().encode(rsSource.getBytes(currentColumnCount)));
            String targetBinaryValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s",
                                sourceBinaryValue, targetBinaryValue);
            break;
          case Types.BIGINT:
            long sourceBigIntValue = rsSource.getLong(currentColumnCount);
            long targetBigIntValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsLong();
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceBigIntValue),
                                String.valueOf(targetBigIntValue));
            break;
          case Types.SMALLINT:
          case Types.TINYINT:
          case Types.INTEGER:
            int sourceIntValue = rsSource.getInt(currentColumnCount);
            int targetIntValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsInt();
            Assert.assertEquals("Different values found for column : %s", String.valueOf(sourceIntValue),
                                String.valueOf(targetIntValue));
            break;
          case Types.DATE:
            java.sql.Date dateSource = rsSource.getDate(currentColumnCount);
            java.sql.Date dateTarget = java.sql.Date.valueOf(
              bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", dateSource, dateTarget);
            break;
          case Types.TIMESTAMP:
            Timestamp sourceTimestampValue = rsSource.getTimestamp(columnName);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            Date parsedDate = dateFormat.parse(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Timestamp targetTimestampValue = new Timestamp(parsedDate.getTime());
            Assert.assertEquals("Different values found for column: %s",
                                sourceTimestampValue, targetTimestampValue);
            break;
          default:
          case Types.VARCHAR:
          case Types.CHAR:
          case Types.NCHAR:
          case Types.NVARCHAR:
          case Types.LONGNVARCHAR:
          case Types.OTHER:
            String sourceStringValue = rsSource.getString(currentColumnCount);
            String targetStringValue = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals(String.format("Different %s values found for column : %s", columnTypeName,
                                              columnName),
                                String.valueOf(sourceStringValue), String.valueOf(targetStringValue));
        }
        currentColumnCount++;
      }
      jsonObjectIdx++;
    }
    Assert.assertFalse("Number of rows in Source table is greater than the number of rows in Target table",
                       rsSource.next());
    return true;
  }
}
