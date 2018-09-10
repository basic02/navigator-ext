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
 * Tagging HDFS Directories
 *
 * Tags is an important part of business metadata. This example uses the
 * Navigator plugin to tag an HDFS directory as sensitive.
 * Users and applications can then use the tags to trigger actions such as
 * encryption, masking, and/or restrictions to permissions.
 */
public class SetHdfsDirectoryTags {
	
	public static void main(String[] args) {

		//Setup the plugin and api client
		NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		NavApiCient client = plugin.getClient();

		//Take the first one without checking
		Source fs = client.getSourcesForType(SourceType.HDFS).iterator().next();

		//Set tags for the entity to Navigator
		CustomHdfsEntity dir = new CustomHdfsEntity("/user/user2", EntityType.DIRECTORY, fs.getIdentity());
		dir.setAlias("User2 home dir");
		dir.setDescription("User2 home directory");
		dir.setTags(Sets.newHashSet("HAS_SENSITIVE_FILES", "CONTAINS_SOME_SUPER_SECRET_STUFF"));

		//Write metadata
		ResultSet results = plugin.write(dir);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
		
		//-------------------------------------------------------------------
		
		//Add tags for the entity to Navigator
		dir.setAlias("User2 home dir");
		dir.setDescription("User2 home dir");
		dir.addTags(Sets.newHashSet("CONTAINS_PII_INFO"));

		//Write metadata
		results = plugin.write(dir);

		if (results.hasErrors()) {
			throw new RuntimeException(results.toString());
		}
	}

}
