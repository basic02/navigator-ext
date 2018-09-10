package com.cloudera.nav.ext.client.writer;

import java.util.Map;

import com.cloudera.nav.ext.client.extraction.MetaExtractor;
import com.cloudera.nav.ext.model.entities.CustomHiveDatabase;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.google.common.collect.Maps;

public class SetHiveDatabaseMetadata {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		MetaExtractor extractor = new MetaExtractor();
		String identity = (String) extractor.getHiveDatabase("default").next().get("identity");
		
		CustomHiveDatabase entity = new CustomHiveDatabase();
		entity.setIdentity(identity);
		entity.setDatabaseName("default"); //optional
		
		//Get the Hive Source
		Source hiveSource = client.getHMSSource();
		entity.setSourceId(hiveSource.getIdentity());
		
		entity.setAlias("default database");
		entity.setDescription("default database");
		
		//Set tags for the entity
		entity.setTags("tag1", "tag2", "tag3");
		
		//Set user-defined properties
		Map<String, String> properties = Maps.newHashMap();
		properties.put("databaseName", "default");
		entity.setProperties(properties);
		
		//Set managed properties
		entity.setCustomProperty("Basic_Property1", "Database_Type", "ODS库");
		entity.setCustomProperty("Basic_Property1", "Status", "在用");
		entity.setCustomProperty("Basic_Property1", "English_Name", "ODS");
		entity.setCustomProperty("Basic_Property1", "Database_Charset", "GBK");
		
		//Write metadata
		ResultSet results = plugin.write(entity);
		
		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
	}

}
