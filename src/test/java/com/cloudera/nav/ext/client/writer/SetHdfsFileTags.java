package com.cloudera.nav.ext.client.writer;

import com.cloudera.nav.ext.model.entities.CustomHdfsEntity;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.Sets;

/**
 * Tagging HDFS Files
 *
 * Tags is an important part of business metadata. This example uses the
 * Navigator plugin to tag an HDFS file as sensitive.
 * Users and applications can then use the tags to trigger actions such as
 * encryption, masking, and/or restrictions to permissions.
 */
public class SetHdfsFileTags {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		//Take the first one without checking
		Source fs = client.getSourcesForType(SourceType.HDFS).iterator().next();

		//Set tags for the entity to Navigator
		CustomHdfsEntity file = new CustomHdfsEntity("/user/root/performance_evaluation/20180515004310/inputs/input.txt", 
								EntityType.FILE, fs.getIdentity());
		file.setAlias("input file");
		file.setDescription("input file for performance evaluation");
		file.setTags(Sets.newHashSet("HAS_SENSITIVE_INFO", "CONTAINS_SOME_SUPER_SECRET_STUFF"));

		//Write metadata
		ResultSet results = plugin.write(file);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
		
		//-------------------------------------------------------------------
		
		//Add tags for the entity to Navigator
		file.setAlias("input file");
		file.setDescription("input file for performance evaluation");
		file.addTags(Sets.newHashSet("CONTAINS_PII_INFO"));

		//Write metadata
		results = plugin.write(file);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
	}

}
