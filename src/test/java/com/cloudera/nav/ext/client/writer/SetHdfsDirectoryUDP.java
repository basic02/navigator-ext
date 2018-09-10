package com.cloudera.nav.ext.client.writer;

import java.util.Map;

import com.cloudera.nav.ext.model.entities.CustomHdfsEntity;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.Maps;

public class SetHdfsDirectoryUDP {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		//Take the first one without checking
		Source fs = client.getSourcesForType(SourceType.HDFS).iterator().next();

		//Set user-defined properties for the entity to Navigator
		CustomHdfsEntity dir = new CustomHdfsEntity("/user/user2", EntityType.DIRECTORY, fs.getIdentity());
		dir.setAlias("User2 home dir");
		dir.setDescription("User2 home directory");
		
		//Set user-defined properties
		Map<String, String> properties = Maps.newHashMap();
		properties.put("Key1", "Value1");
		dir.setProperties(properties);
		
		//Write metadata
		ResultSet results = plugin.write(dir);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
		
		//-------------------------------------------------------------------
		
		//Add user-defined properties
		dir.setAlias("User2 home dir");
		dir.setDescription("User2 home directory");
		properties = Maps.newHashMap();
		properties.put("Key2", "Value2");
		dir.addProperties(properties);
		
		//Write metadata
		results = plugin.write(dir);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
	}

}
