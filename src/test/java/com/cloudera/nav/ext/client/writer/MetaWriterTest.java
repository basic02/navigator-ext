package com.cloudera.nav.ext.client.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.cloudera.nav.ext.client.writer.MetaWriter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MetaWriterTest {
	
	public static void main(String[] args) throws IOException {
		MetaWriter writer = new MetaWriter();
		
		//Collection<String> tags = Sets.newHashSet("tag1", "tag2", "tag3");
		
		//Map<String, String> properties = Maps.newHashMap();
		//properties.put("Database", "default");
		//properties.put("Database_Type", "DM库");
		
		Map<String, Map<String, Object>> customProperties = Maps.newHashMap();
		Map<String, Object> basicProperties = Maps.newHashMap();
		//basicProperties.put("Database", "hr");
		basicProperties.put("Database_Type", "ODS库");
		basicProperties.put("Status", "停用");
		basicProperties.put("Database_Charset", "UTF-8");
		basicProperties.put("English_Name", "ODS");
		basicProperties.put("Number", "DB1");
		Collection<String> applications = Sets.newHashSet("app1", "app2", "app3");
		basicProperties.put("Related_Applications", applications);
		customProperties.put("Basic_Property", basicProperties);
		
		//entityUpdater.updateHdfsDirectory("/user/user1/oozie-oozi", "oozie", "oozie", tags, properties, customProperties, true);
		
		writer.updateHiveDatabase("default", "default database", "default database", null, null, customProperties, true);
		//entityUpdater.updateHiveTable("default", "metrics", "metric table", "metric table", tags, null, null, true);
		//entityUpdater.updateHiveField("default", "metrics", "host", "host column", "host column", tags, null, null, true);
		writer.flush();
	}

}
