package com.cloudera.nav.ext.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents a Hive column that supports managed custom metadata, which is 
 * uniquely identified by the source id, database name, table name, and column name.
 * Note that the source type, entity type, and namespace should not be modified.
 */
@MClass(model="hv_column")
public class CustomHiveColumn extends CustomEntity {

  private final String DATABASE_NAME = "databaseName";
  private final String TABLE_NAME = "tableName";
  private final String COLUMN_NAME = "columnName";

  @MProperty
  private String databaseName;
  @MProperty
  private String tableName;
  @MProperty
  private String columnName;

  public CustomHiveColumn() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.FIELD);
  }

  public CustomHiveColumn(String sourceId, String db, String table, String column) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
    setTableName(table);
    setColumnName(column);
  }

  public CustomHiveColumn(String id) {
    this();
    setIdentity(id);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the column name. Aliases {@link CustomHiveColumn#getName}
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Changes the column name. Aliases {@link CustomHiveColumn#setName}
   * @param columnName
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(
        DATABASE_NAME, this.getDatabaseName(),
        TABLE_NAME, this.getTableName(),
        COLUMN_NAME, this.getColumnName());
  }
  
}
