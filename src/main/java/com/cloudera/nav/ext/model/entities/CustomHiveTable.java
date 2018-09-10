package com.cloudera.nav.ext.model.entities;

import java.util.Map;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.ImmutableMap;

/**
 * Represents a Hive table that supports managed custom metadata, which is
 * uniquely identified by the source id, database name, and table name.
 */
@MClass(model = "hv_table")
public class CustomHiveTable extends CustomEntity {

  private final String DATABASE_NAME = "databaseName";
  private final String TABLE_NAME = "tableName";

  @MProperty
  private String databaseName;
  
  @MProperty
  private String tableName;
  
  public CustomHiveTable() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.TABLE);
  }

  public CustomHiveTable(String sourceId, String db, String table) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
    setTableName(table);
  }

  public CustomHiveTable(String id) {
    this();
    setIdentity(id);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * @return the table name. This aliases getName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Change the table name. This aliases setName
   * @param tableName
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(
        DATABASE_NAME, this.getDatabaseName(),
        TABLE_NAME, this.getTableName());
  }
  
}
