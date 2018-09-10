package com.cloudera.nav.ext.client.extraction;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.model.entities.EntityType;

/**
 * A wrapper class to extract entities and relations via the easy-use methods.
 */
public class MetaExtractor {
	
	private static final Logger LOG = LoggerFactory.getLogger(MetaExtractor.class);
	
	private NavApiCient client; //Client to communicate with the Navigator REST API
	private MetadataExtractor extractor; //Metadata extractor
	
	/**
	 * Wraps navigator client and metadata extractor.
	 */
	public MetaExtractor() {
		this.client = NavigatorPlugin.fromConfigFile("navigator.conf").getClient();
		this.extractor = new MetadataExtractor(this.client, null);
	}
	
	/**
	 * Gets the metadata of specific HDFS files.
	 * 
	 * @param filePath	The full HDFS path of file
	 */
	public Iterator<Map<String, Object>> getHDFSFiles(String filePath) {
		LOG.info(String.format("Gets the metadata of the HDFS files: %s", filePath));
		return getHDFSEntities(EntityType.FILE.name(), filePath);
	}
	
	/**
	 * Gets the metadata of specific HDFS directories.
	 * 
	 * @param directoryPath	The full HDFS path of directory
	 */
	public Iterator<Map<String, Object>> getHDFSDirectories(String directoryPath) {
		LOG.info(String.format("Gets the metadata of the HDFS directories: %s", directoryPath));
		return getHDFSEntities(EntityType.DIRECTORY.name(), directoryPath);
	}
	
	/**
	 * Gets the metadata of specific HDFS entities (directories or files).
	 * 
	 * @param entityType	Entity type (DIRECTORY or FILE)
	 * @param path			The full HDFS path
	 */
	private Iterator<Map<String, Object>> getHDFSEntities(String entityType, String path) {
		StringBuffer query = new StringBuffer("sourceType:HDFS AND type:").append(entityType);
		query.append(" AND fileSystemPath:\"").append(path).append("\"");
		
		String parentPath = path.substring(0, path.lastIndexOf("/"));
		if (StringUtils.isEmpty(parentPath)) {
			parentPath = "/";
		}
		query.append(" AND parentPath:\"").append(parentPath).append("\"");
		
		LOG.info(String.format("Calls the extractMetadata method with the entitiesQuery: %s", query));
		Iterable<Map<String, Object>> hdfsAll =
				extractor.extractMetadata(null, null, query.toString(), null).getEntities();
		
		return hdfsAll.iterator();
	}
	
	/**
	 * Gets the metadata of specific Hive database.
	 * 
	 * @param databaseName	Database name
	 */
	public Iterator<Map<String, Object>> getHiveDatabase(String databaseName) {
		LOG.info(String.format("Gets the metadata of the Hive database: %s", databaseName));
		return getHiveEntities(EntityType.DATABASE.name(), databaseName, null);
	}
	
	/**
	 * Gets the metadata of specific Hive tables.
	 * 
	 * @param databaseName	Database name
	 * @param tableName		Table name
	 */
	public Iterator<Map<String, Object>> getHiveTable(String databaseName, String tableName) {
		LOG.info(String.format("Gets the metadata of the Hive table: %s.%s", databaseName, tableName));
		return getHiveEntities(EntityType.TABLE.name(), tableName, StringUtils.isEmpty(databaseName) ? null : "/" + databaseName);
	}
	
	/**
	 * Gets the metadata of specific Hive views.
	 * 
	 * @param databaseName	Database name
	 * @param viewName		View name
	 */
	public Iterator<Map<String, Object>> getHiveView(String databaseName, String viewName) {
		LOG.info(String.format("Gets the metadata of the Hive view: %s.%s", databaseName, viewName));
		return getHiveEntities("VIEW", viewName, StringUtils.isEmpty(databaseName) ? null : "/" + databaseName);
	}
	
	/**
	 * Gets the metadata of specific Hive fields.
	 * 
	 * @param databaseName	Database name
	 * @param tableName		Table name
	 * @param fieldName		Column name
	 */
	public Iterator<Map<String, Object>> getHiveField(String databaseName, String tableName, String fieldName) {
		String parentPath = null;
		if (!StringUtils.isEmpty(databaseName) && !StringUtils.isEmpty(tableName)) {
			parentPath = "/" + databaseName + "/" + tableName;
		}
		
		LOG.info(String.format("Gets the metadata of the Hive field: %s.%s.%s", databaseName, tableName, fieldName));
		return getHiveEntities(EntityType.FIELD.name(), fieldName, parentPath);
	}
	
	/**
	 * Gets the metadata of specific Hive entities.
	 * 
	 * @param entityType	Entity type (DATABASE, TABLE, VIEW, FIELD)
	 * @param name			Original name of Hive entity, e.g., database name, table name.
	 * @param parentPath	Parent path
	 */
	private Iterator<Map<String, Object>> getHiveEntities(String entityType, String name, String parentPath) {
		StringBuffer query = new StringBuffer("sourceType:HIVE AND type:").append(entityType);
		query.append(" AND originalName:").append(name);
		if (!StringUtils.isEmpty(parentPath)) {
			query.append(" AND parentPath:\"").append(parentPath).append("\"");
		}
		
		LOG.info(String.format("Calls the extractMetadata method with the entitiesQuery: %s", query));
		Iterable<Map<String, Object>> hiveAll =
				extractor.extractMetadata(null, null, query.toString(), null).getEntities();
		
		return hiveAll.iterator();
	}

}
