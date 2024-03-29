/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.debezium.connector.mysql.antlr;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.antlr.listener.MySqlAntlrDdlParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlLexer;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * An ANTLR based parser for MySQL DDL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlAntlrDdlParser extends AntlrDdlParser<MySqlLexer, MySqlParser> {

  public static final Logger LOG = LoggerFactory.getLogger(MySqlAntlrDdlParser.class);
  private static final String CLASS_CHARSET_MAPPING = "com.mysql.cj.CharsetMapping";
  public static ClassLoader jdbcClassLoader;
  private final ConcurrentMap<String, String> charsetNameForDatabase = new ConcurrentHashMap<>();
  private final MySqlValueConverters converters;
  private final TableFilter tableFilter;

  public MySqlAntlrDdlParser() {
    this(null, TableFilter.includeAll());
  }

  public MySqlAntlrDdlParser(MySqlValueConverters converters) {
    this(converters, TableFilter.includeAll());
  }

  public MySqlAntlrDdlParser(MySqlValueConverters converters, TableFilter tableFilter) {
    this(true, false, false, converters, tableFilter);
  }

  public MySqlAntlrDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                             MySqlValueConverters converters, TableFilter tableFilter) {
    super(throwErrorsFromTreeWalk, includeViews, includeComments);
    systemVariables = new MySqlSystemVariables();
    this.converters = converters;
    this.tableFilter = tableFilter;
  }

  /**
   * Extracts the enumeration values properly parsed and escaped.
   *
   * @param enumValues the raw enumeration values from the parsed column definition
   * @return the list of options allowed for the {@code ENUM} or {@code SET}; never null.
   */
  public static List<String> extractEnumAndSetOptions(List<String> enumValues) {
    return enumValues.stream()
      .map(MySqlAntlrDdlParser::withoutQuotes)
      .map(MySqlAntlrDdlParser::escapeOption)
      .collect(Collectors.toList());
  }

  public static String escapeOption(String option) {
    // Replace comma to backslash followed by comma (this escape sequence implies comma is part of the option)
    // Replace backlash+single-quote to a single-quote.
    // Replace double single-quote to a single-quote.
    return option.replaceAll(",", "\\\\,").replaceAll("\\\\'", "'").replaceAll("''", "'");
  }

  @Override
  protected ParseTree parseTree(MySqlParser parser) {
    return parser.root();
  }

  @Override
  protected AntlrDdlParserListener createParseTreeWalkerListener() {
    return new MySqlAntlrDdlParserListener(this);
  }

  @Override
  protected MySqlLexer createNewLexerInstance(CharStream charStreams) {
    return new MySqlLexer(charStreams);
  }

  @Override
  protected MySqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
    return new MySqlParser(commonTokenStream);
  }

  @Override
  protected SystemVariables createNewSystemVariablesInstance() {
    return new MySqlSystemVariables();
  }

  @Override
  protected boolean isGrammarInUpperCase() {
    return true;
  }

  @Override
  protected DataTypeResolver initializeDataTypeResolver() {
    DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

    dataTypeResolverBuilder.registerDataTypes(MySqlParser.StringDataTypeContext.class.getCanonicalName(), Arrays.asList(
      new DataTypeEntry(Types.CHAR, MySqlParser.CHAR),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.CHAR, MySqlParser.VARYING),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT),
      new DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR),
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARYING),
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR),
      new DataTypeEntry(Types.CHAR, MySqlParser.CHAR, MySqlParser.BINARY),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.VARCHAR, MySqlParser.BINARY),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.TINYTEXT, MySqlParser.BINARY),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.TEXT, MySqlParser.BINARY),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.MEDIUMTEXT, MySqlParser.BINARY),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.LONGTEXT, MySqlParser.BINARY),
      new DataTypeEntry(Types.NCHAR, MySqlParser.NCHAR, MySqlParser.BINARY),
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NVARCHAR, MySqlParser.BINARY),
      new DataTypeEntry(Types.CHAR, MySqlParser.CHARACTER),
      new DataTypeEntry(Types.VARCHAR, MySqlParser.CHARACTER, MySqlParser.VARYING)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.NationalStringDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.VARCHAR).setSuffixTokens(MySqlParser.BINARY),
      new DataTypeEntry(Types.NCHAR, MySqlParser.NATIONAL, MySqlParser.CHARACTER).setSuffixTokens(MySqlParser.BINARY),
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NCHAR, MySqlParser.VARCHAR).setSuffixTokens(MySqlParser.BINARY)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.NationalVaryingStringDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.CHAR, MySqlParser.VARYING),
      new DataTypeEntry(Types.NVARCHAR, MySqlParser.NATIONAL, MySqlParser.CHARACTER, MySqlParser.VARYING)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.DimensionDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.SMALLINT, MySqlParser.TINYINT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.SMALLINT, MySqlParser.INT1)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.SMALLINT, MySqlParser.SMALLINT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.SMALLINT, MySqlParser.INT2)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.INTEGER, MySqlParser.MEDIUMINT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.INTEGER, MySqlParser.INT3)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.INTEGER, MySqlParser.MIDDLEINT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.INTEGER, MySqlParser.INT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.INTEGER, MySqlParser.INTEGER)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.INTEGER, MySqlParser.INT4)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.BIGINT, MySqlParser.BIGINT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.BIGINT, MySqlParser.INT8)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.REAL, MySqlParser.REAL)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.DOUBLE, MySqlParser.DOUBLE)
        .setSuffixTokens(MySqlParser.PRECISION, MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.DOUBLE, MySqlParser.FLOAT8)
        .setSuffixTokens(MySqlParser.PRECISION, MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.FLOAT, MySqlParser.FLOAT4)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL),
      new DataTypeEntry(Types.DECIMAL, MySqlParser.DECIMAL)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
        .setDefaultLengthScaleDimension(10, 0),
      new DataTypeEntry(Types.DECIMAL, MySqlParser.DEC)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
        .setDefaultLengthScaleDimension(10, 0),
      new DataTypeEntry(Types.DECIMAL, MySqlParser.FIXED)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
        .setDefaultLengthScaleDimension(10, 0),
      new DataTypeEntry(Types.NUMERIC, MySqlParser.NUMERIC)
        .setSuffixTokens(MySqlParser.SIGNED, MySqlParser.UNSIGNED, MySqlParser.ZEROFILL)
        .setDefaultLengthScaleDimension(10, 0),
      new DataTypeEntry(Types.BIT, MySqlParser.BIT),
      new DataTypeEntry(Types.TIME, MySqlParser.TIME),
      new DataTypeEntry(Types.TIMESTAMP_WITH_TIMEZONE, MySqlParser.TIMESTAMP),
      new DataTypeEntry(Types.TIMESTAMP, MySqlParser.DATETIME),
      new DataTypeEntry(Types.BINARY, MySqlParser.BINARY),
      new DataTypeEntry(Types.VARBINARY, MySqlParser.VARBINARY),
      new DataTypeEntry(Types.BLOB, MySqlParser.BLOB),
      new DataTypeEntry(Types.INTEGER, MySqlParser.YEAR)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.SimpleDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.DATE, MySqlParser.DATE),
      new DataTypeEntry(Types.BLOB, MySqlParser.TINYBLOB),
      new DataTypeEntry(Types.BLOB, MySqlParser.MEDIUMBLOB),
      new DataTypeEntry(Types.BLOB, MySqlParser.LONGBLOB),
      new DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOL),
      new DataTypeEntry(Types.BOOLEAN, MySqlParser.BOOLEAN),
      new DataTypeEntry(Types.BIGINT, MySqlParser.SERIAL)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.CollectionDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.CHAR, MySqlParser.ENUM).setSuffixTokens(MySqlParser.BINARY),
      new DataTypeEntry(Types.CHAR, MySqlParser.SET).setSuffixTokens(MySqlParser.BINARY)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.SpatialDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRYCOLLECTION),
      new DataTypeEntry(Types.OTHER, MySqlParser.GEOMCOLLECTION),
      new DataTypeEntry(Types.OTHER, MySqlParser.LINESTRING),
      new DataTypeEntry(Types.OTHER, MySqlParser.MULTILINESTRING),
      new DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOINT),
      new DataTypeEntry(Types.OTHER, MySqlParser.MULTIPOLYGON),
      new DataTypeEntry(Types.OTHER, MySqlParser.POINT),
      new DataTypeEntry(Types.OTHER, MySqlParser.POLYGON),
      new DataTypeEntry(Types.OTHER, MySqlParser.JSON),
      new DataTypeEntry(Types.OTHER, MySqlParser.GEOMETRY)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.LongVarbinaryDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.BLOB, MySqlParser.LONG)
        .setSuffixTokens(MySqlParser.VARBINARY)));
    dataTypeResolverBuilder.registerDataTypes(MySqlParser.LongVarcharDataTypeContext.class.getCanonicalName(),
                                              Arrays.asList(
      new DataTypeEntry(Types.VARCHAR, MySqlParser.LONG)
        .setSuffixTokens(MySqlParser.VARCHAR)));

    return dataTypeResolverBuilder.build();
  }

  /**
   * Provides a map of default character sets by database/schema name.
   *
   * @return map of default character sets.
   */
  public ConcurrentMap<String, String> charsetNameForDatabase() {
    return charsetNameForDatabase;
  }

  /**
   * Parse a name from {@link MySqlParser.UidContext}.
   *
   * @param uidContext uid context
   * @return name without quotes.
   */
  public String parseName(MySqlParser.UidContext uidContext) {
    return withoutQuotes(uidContext);
  }

  /**
   * Parse qualified table identification from {@link MySqlParser.FullIdContext}.
   * {@link MySqlAntlrDdlParser#currentSchema()} will be used if definition of schema name is not part of the context.
   *
   * @param fullIdContext full id context.
   * @return qualified {@link TableId}.
   */
  public TableId parseQualifiedTableId(MySqlParser.FullIdContext fullIdContext) {
    final char[] fullTableName = fullIdContext.getText().toCharArray();
    StringBuilder component = new StringBuilder();
    String dbName = null;
    String tableName = null;
    final char empty = '\0';
    char lastQuote = empty;
    for (int i = 0; i < fullTableName.length; i++) {
      char c = fullTableName[i];
      if (isQuote(c)) {
        // Opening quote
        if (lastQuote == empty) {
          lastQuote = c;
        } else if (lastQuote == c) {
          // escape of quote by doubling
          if (i < fullTableName.length - 1 && fullTableName[i + 1] == c) {
            component.append(c);
            i++;
          } else {
            lastQuote = empty;
          }
        } else {
          // Quote that is part of name
          component.append(c);
        }
      } else if (c == '.' && lastQuote == empty) {
        // dot that is not in quotes, so name separator
        dbName = component.toString();
        component = new StringBuilder();
      } else {
        // Any char is part of name including quoted dot
        component.append(c);
      }
    }
    tableName = component.toString();
    return resolveTableId(dbName != null ? dbName : currentSchema(), tableName);
  }

  /**
   * Parse column names for primary index from {@link MySqlParser.IndexColumnNamesContext}. This method will updates
   * column to be not optional and set primary key column names to table.
   *
   * @param indexColumnNamesContext primary key index column names context.
   * @param tableEditor             editor for table where primary key index is parsed.
   */
  public void parsePrimaryIndexColumnNames(MySqlParser.IndexColumnNamesContext indexColumnNamesContext,
                                           TableEditor tableEditor) {
    List<String> pkColumnNames = indexColumnNamesContext.indexColumnName().stream()
      .map(indexColumnNameContext -> {
        // MySQL does not allow a primary key to have nullable columns, so let's make sure we model that correctly ...
        String columnName;
        if (indexColumnNameContext.uid() != null) {
          columnName = parseName(indexColumnNameContext.uid());
        } else {
          columnName = withoutQuotes(indexColumnNameContext.STRING_LITERAL().getText());
        }
        Column column = tableEditor.columnWithName(columnName);
        if (column != null && column.isOptional()) {
          final ColumnEditor ce = column.edit().optional(false);
          if (ce.hasDefaultValue() && !ce.defaultValueExpression().isPresent()) {
            ce.unsetDefaultValueExpression();
          }
          tableEditor.addColumn(ce.create());
        }
        return column != null ? column.name() : columnName;
      })
      .collect(Collectors.toList());

    tableEditor.setPrimaryKeyNames(pkColumnNames);
  }

  /**
   * Get the name of the character set for the current database, via the "character_set_database" system property.
   *
   * @return the name of the character set for the current database, or null if not known ...
   */
  public String currentDatabaseCharset() {
    String charsetName = systemVariables.getVariable(MySqlSystemVariables.CHARSET_NAME_DATABASE);
    if (charsetName == null || "DEFAULT".equalsIgnoreCase(charsetName)) {
      charsetName = systemVariables.getVariable(MySqlSystemVariables.CHARSET_NAME_SERVER);
    }
    return charsetName;
  }

  /**
   * Get the name of the character set for the give table name.
   *
   * @return the name of the character set for the given table, or null if not known ...
   */
  public String charsetForTable(TableId tableId) {
    final String defaultDatabaseCharset = tableId.catalog() != null ?
      charsetNameForDatabase().get(tableId.catalog()) : null;
    return defaultDatabaseCharset != null ? defaultDatabaseCharset : currentDatabaseCharset();
  }

  /**
   * Runs a function if all given object are not null.
   *
   * @param function        function to run; may not be null
   * @param nullableObjects object to be tested, if they are null.
   */
  public void runIfNotNull(Runnable function, Object... nullableObjects) {
    for (Object nullableObject : nullableObjects) {
      if (nullableObject == null) {
        return;
      }
    }
    function.run();
  }

  public MySqlValueConverters getConverters() {
    return converters;
  }

  public TableFilter getTableFilter() {
    return tableFilter;
  }


  /**
   * ===================== This is a diff from the original file ===========================
   * This function extracts the charset based on collation from the Charset class of MYSQL DRIVER CONNECTOR.
   * Debezium has used methods that may or may not be present in the given mysql connector.
   *
   * We will load the driver class `com.mysql.cj.CharsetMapping` using our classloader and check if the methods are
   * present.
   *
   * If not present, then return "DEFAULT" as charset.
   */

  /**
   * Obtains the charset name either form charset if present or from collation.
   *
   * @param charsetNode
   * @param collationNode
   * @return character set
   */
  public String extractCharset(MySqlParser.CharsetNameContext charsetNode,
                               MySqlParser.CollationNameContext collationNode) {
    String charsetName = "DEFAULT";
    if (charsetNode != null && charsetNode.getText() != null) {
      return withoutQuotes(charsetNode.getText());
    }

    if (collationNode != null && collationNode.getText() != null) {
      try {
        // Load the class.
        Class<?> charsetMappingClass = jdbcClassLoader.loadClass(CLASS_CHARSET_MAPPING);

        // Check methods and make accessible.
        Method getStaticCollationNameForCollationIndex = charsetMappingClass
          .getMethod("getStaticCollationNameForCollationIndex", Integer.class);
        getStaticCollationNameForCollationIndex.setAccessible(true);

        Method getStaticMysqlCharsetNameForCollationIndex = charsetMappingClass
          .getMethod("getStaticMysqlCharsetNameForCollationIndex", Integer.class);
        getStaticMysqlCharsetNameForCollationIndex.setAccessible(true);

        final String collationName = withoutQuotes(collationNode.getText()).toLowerCase();
        for (int index = 0; index < 1024; index++) {
          if (collationName.equals(
            getStaticCollationNameForCollationIndex.invoke(null, index)
          )) {
            charsetName = (String) getStaticMysqlCharsetNameForCollationIndex.invoke(null, index);
            break;
          }
        }
      } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        LOG.debug("Setting 'DEFAULT' as charset. Tried to load CharsetName from CollationName but failed to load " +
                    "the required class or methods : {}", e);
      }
    }
    return charsetName;
  }
}
