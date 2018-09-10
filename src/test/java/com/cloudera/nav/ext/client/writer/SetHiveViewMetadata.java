package com.cloudera.nav.ext.client.writer;

import java.util.Map;

import com.cloudera.nav.ext.client.extraction.MetaExtractor;
import com.cloudera.nav.ext.model.entities.CustomHiveTable;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.google.common.collect.Maps;

public class SetHiveViewMetadata {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		MetaExtractor extractor = new MetaExtractor();
		String identity = (String) extractor.getHiveView("default", "metrics_v").next().get("identity");
		
		CustomHiveTable entity = new CustomHiveTable();
		entity.setIdentity(identity);
		entity.setDatabaseName("default"); //optional
		entity.setTableName("metrics_v"); //optional
		
		//Get the Hive Source
		Source hiveSource = client.getHMSSource();
		entity.setSourceId(hiveSource.getIdentity());
		
		entity.setAlias("metrics view");
		entity.setDescription("metrics view");
		
		//Set tags for the entity
		entity.setTags("tag1", "tag2", "tag3");
		
		//Set user-defined properties
		Map<String, String> properties = Maps.newHashMap();
		properties.put("databaseName", "default");
		properties.put("viewName", "metrics_v");
		entity.setProperties(properties);
		
		//Set managed properties
		entity.setCustomProperty("Basic_Property", "English_Name", "metrics view");
		entity.setCustomProperty("Basic_Property", "Status", "在用");
		entity.setCustomProperty("Basic_Property", "Table_Type", "事实表");
		entity.setCustomProperty("Basic_Property", "Is_View", Boolean.TRUE);
		
		//Write metadata
		ResultSet results = plugin.write(entity);
		
		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
	}

}
