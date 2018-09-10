package com.cloudera.nav.ext.client.writer;

import java.util.Map;

import com.cloudera.nav.ext.client.extraction.MetaExtractor;
import com.cloudera.nav.ext.model.entities.CustomHiveColumn;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.google.common.collect.Maps;

public class SetHiveColumnMetadata {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		MetaExtractor extractor = new MetaExtractor();
		String identity = (String) extractor.getHiveTable("default", "test_compress").next().get("identity");
		identity = (String) extractor.getHiveField("default", "test_compress", "name").next().get("identity");
		
		CustomHiveColumn entity = new CustomHiveColumn();
		entity.setIdentity(identity);
		entity.setDatabaseName("default"); //optional
		entity.setTableName("test_compress"); //optional
		entity.setColumnName("name"); //optional
		
		//Get the Hive Source
		Source hiveSource = client.getHMSSource();
		entity.setSourceId(hiveSource.getIdentity());
		
		entity.setAlias("name");
		entity.setDescription("name column");
		
		//Set tags for the entity
		entity.setTags("tag1", "tag2", "tag3");
		
		//Set user-defined properties
		Map<String, String> properties = Maps.newHashMap();
		properties.put("databaseName", "default");
		properties.put("tableName", "test_compress");
		entity.setProperties(properties);
		
		//Set managed properties
		entity.setCustomProperty("Basic_Property", "English_Name", "name column");
		entity.setCustomProperty("Basic_Property", "Column_Type", "属性字段");
		
		//Write metadata
		ResultSet results = plugin.write(entity);
		
		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
	}

}
