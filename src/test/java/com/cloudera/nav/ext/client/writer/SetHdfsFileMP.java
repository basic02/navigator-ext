package com.cloudera.nav.ext.client.writer;

import com.cloudera.nav.ext.model.entities.CustomHdfsEntity;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.Sets;

public class SetHdfsFileMP {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		//Take the first one without checking
		Source fs = client.getSourcesForType(SourceType.HDFS).iterator().next();

		//Set user-defined properties for the entity to Navigator
		CustomHdfsEntity file = new CustomHdfsEntity("/user/root/performance_evaluation/20180515004310/inputs/input.txt", 
								EntityType.FILE, fs.getIdentity());
		file.setAlias("input file");
		file.setDescription("input file for performance evaluation");
		
		//Set managed properties
		file.setCustomProperty("Basic_Property", "Related_Applications", Sets.newHashSet("App3"));
		
		//Write metadata
		ResultSet results = plugin.write(file);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
		
	}

}
