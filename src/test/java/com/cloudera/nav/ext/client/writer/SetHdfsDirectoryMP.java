package com.cloudera.nav.ext.client.writer;

import com.cloudera.nav.ext.model.entities.CustomHdfsEntity;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;

public class SetHdfsDirectoryMP {
	
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
		
		//Set managed properties
		dir.setCustomProperty("Basic_Property", "Related_Applications", "App1", "App2");
		
		//Write metadata
		ResultSet results = plugin.write(dir);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
		
	}

}
