package com.cloudera.nav.ext.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents a Hive database that supports managed custom metadata, which is 
 * uniquely identified by the source id and database name.
 */
@MClass(model = "hv_database")
public class CustomHiveDatabase extends CustomEntity {

  private final String DATABASE_NAME = "databaseName";

  @MProperty
  private String databaseName;

  public CustomHiveDatabase() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.DATABASE);
  }

  public CustomHiveDatabase(String id) {
    this();
    setIdentity(id);
  }

  public CustomHiveDatabase(String sourceId, String db) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(DATABASE_NAME, this.getDatabaseName());
  }
  
}
