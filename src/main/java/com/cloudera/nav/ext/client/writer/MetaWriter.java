package com.cloudera.nav.ext.client.writer;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.nav.ext.client.extraction.MetaExtractor;
import com.cloudera.nav.ext.model.entities.CustomEntity;
import com.cloudera.nav.ext.model.entities.CustomHdfsEntity;
import com.cloudera.nav.ext.model.entities.CustomHiveColumn;
import com.cloudera.nav.ext.model.entities.CustomHiveDatabase;
import com.cloudera.nav.ext.model.entities.CustomHiveTable;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.collect.Sets;

/**
 * Updates user modifiable properties of an entity (a.k.a. business metadata).
 * All editable properties will be overridden with the given metadata. 
 * If tags or properties are left out they will remain same.
 */
public class MetaWriter {
	
	private static final Logger LOG = LoggerFactory.getLogger(MetaWriter.class);
	
	private NavigatorPlugin plugin; //Plugin used to write metadata to Navigator
    private NavApiCient client; //Client to communicate with the Navigator REST API
    
    private Collection<Entity> entities; //Entity batch
    
    /**
	 * Constructs a metadata writer.
	 */
	public MetaWriter() {
		this.plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		this.client = this.plugin.getClient();
		this.entities = Sets.newHashSet();
	}
	
	/**
	 * Updates the metadata of a HDFS file.
	 * 
	 * @param filePath			File system path of the HDFS file, like: /user/user1/fileA
	 * @param alias				Name of the HDFS file shown in Navigator
	 * @param description		Description of the HDFS file shown in Navigator
	 * @param tags				User-defined tags for the HDFS file (custom metadata)
	 * @param properties		User-defined properties for the HFS file (custom metadata)
	 * @param customProperties	Managed custom properties for the HDFS file (managed metadata)
	 * @param override			Whether to override existing tags, custom properties and managed custom properties
	 */
	public void updateHdfsFile(String filePath,
							   String alias,
							   String description,
							   Collection<String> tags,
							   Map<String, String> properties,
							   Map<String, Map<String, Object>> customProperties,
							   boolean override) {
		LOG.info(String.format("Updates the metadata of the HDFS file: %s", filePath));
		Entity entity = newHdfsEntity(EntityType.FILE, filePath, alias, description, 
									  tags, properties, customProperties, override);
		entities.add(entity);
	}
	
	/**
	 * Updates the metadata of a HDFS directory.
	 * 
	 * @param directoryPath		File system path of the HDFS directory, like: /user/user1
	 * @param alias				Name of the HDFS directory shown in Navigator
	 * @param description		Description of the HDFS directory shown in Navigator
	 * @param tags				User-defined tags for the HDFS directory (custom metadata)
	 * @param properties		User-defined properties for the HFS directory (custom metadata)
	 * @param customProperties	Managed custom properties for the HDFS directory (managed metadata)
	 * @param override			Whether to override existing tags, custom properties and managed custom properties
	 */
	public void updateHdfsDirectory(String directoryPath,
								    String alias,
								    String description,
								    Collection<String> tags,
								    Map<String, String> properties,
								    Map<String, Map<String, Object>> customProperties,
								    boolean override) {
		LOG.info(String.format("Updates the metadata of the HDFS directory: %s", directoryPath));
		Entity entity = newHdfsEntity(EntityType.DIRECTORY, directoryPath, alias, description, 
									  tags, properties, customProperties, override);
		entities.add(entity);
	}
	
	private Entity newHdfsEntity(EntityType type, 
								 String path, 
								 String alias, 
								 String description,
								 Collection<String> tags,
								 Map<String, String> properties,
								 Map<String, Map<String, Object>> customProperties,
								 boolean override) {
		MetaExtractor extractor = new MetaExtractor();
		
		//Takes the first one without checking
		Source fs = client.getSourcesForType(SourceType.HDFS).iterator().next();
		//Sets new properties to entity
		CustomHdfsEntity entity = new CustomHdfsEntity(path, type, fs.getIdentity());
		
		String identity = null;
		if (EntityType.DIRECTORY.equals(type)) {
			identity = (String) extractor.getHDFSDirectories(path).next().get("identity");
		} else {
			identity = (String) extractor.getHDFSFiles(path).next().get("identity");
		}
		entity.setIdentity(identity);
		
		if (StringUtils.isNotEmpty(alias)) {
			entity.setAlias(alias);
		}
		if (StringUtils.isNotEmpty(description)) {
			entity.setDescription(description);
		}
		if (override) {
			entity.setTags(tags);
		} else {
			if (CollectionUtils.isNotEmpty(tags)) {
				entity.addTags(tags);
			}
		}
		if (override) {
			entity.setProperties(properties);
		} else {
			if (MapUtils.isNotEmpty(properties)) {
				entity.addProperties(properties);
			}
		}
		if (MapUtils.isNotEmpty(customProperties)) {
			entity.setCustomProperties(customProperties);
		}
		
		LOG.info("Creates an HDFS entity object.");
		return entity;
	}
	
	/**
	 * Updates the metadata of a Hive database.
	 * 
	 * @param databaseName		Hive database name
	 * @param alias				Name of the Hive database shown in Navigator
	 * @param description		Description of the Hive database shown in Navigator
	 * @param tags				User-defined tags for the Hive database (custom metadata)
	 * @param properties		User-defined properties for the Hive database (custom metadata)
	 * @param customProperties	Managed custom properties for the Hive database (managed metadata)
	 * @param override			Whether to override existing tags, custom properties and managed custom properties
	 */
	public void updateHiveDatabase(String databaseName,
								   String alias,
								   String description,
								   Collection<String> tags,
								   Map<String, String> properties,
								   Map<String, Map<String, Object>> customProperties,
								   boolean override) {
		LOG.info(String.format("Updates the metadata of the Hive database: %s", databaseName));
		Entity entity = newHiveEntity(EntityType.DATABASE.name(), databaseName, null, null, alias, 
									  description, tags, properties, customProperties, override);
		entities.add(entity);
	}
	
	/**
	 * Updates the metadata of a Hive table.
	 * 
	 * @param databaseName		Hive database name
	 * @param tableName			Hive table name
	 * @param alias				Name of the Hive table shown in Navigator
	 * @param description		Description of the Hive table shown in Navigator
	 * @param tags				User-defined tags for the Hive table (custom metadata)
	 * @param properties		User-defined properties for the Hive table (custom metadata)
	 * @param customProperties	Managed custom properties for the Hive table (managed metadata)
	 * @param override			Whether to override existing tags, custom properties and managed custom properties
	 */
	public void updateHiveTable(String databaseName,
								String tableName,
								String alias,
								String description,
								Collection<String> tags,
								Map<String, String> properties,
								Map<String, Map<String, Object>> customProperties,
								boolean override) {
		LOG.info(String.format("Updates the metadata of the Hive table: %s.%s", databaseName, tableName));
		Entity entity = newHiveEntity(EntityType.TABLE.name(), databaseName, tableName, null, alias, 
									  description, tags, properties, customProperties, override);
		entities.add(entity);
	}
	
	/**
	 * Updates the metadata of a Hive view.
	 * 
	 * @param databaseName		Hive database name
	 * @param viewName			Hive view name
	 * @param alias				Name of the Hive view shown in Navigator
	 * @param description		Description of the Hive view shown in Navigator
	 * @param tags				User-defined tags for the Hive view (custom metadata)
	 * @param properties		User-defined properties for the Hive view (custom metadata)
	 * @param customProperties	Managed custom properties for the Hive view (managed metadata)
	 * @param override			Whether to override existing tags, custom properties and managed custom properties
	 */
	public void updateHiveView(String databaseName,
							   String viewName,
							   String alias,
							   String description,
							   Collection<String> tags,
							   Map<String, String> properties,
							   Map<String, Map<String, Object>> customProperties,
							   boolean override) {
		LOG.info(String.format("Updates the metadata of the Hive view: %s.%s", databaseName, viewName));
		Entity entity = newHiveEntity("VIEW", databaseName, viewName, null, alias, 
									  description, tags, properties, customProperties, override);
		entities.add(entity);
	}
	
	/**
	 * Updates the metadata of a Hive field.
	 * 
	 * @param databaseName		Hive database name
	 * @param tableName			Hive table name
	 * @param fieldName			Hive field name
	 * @param alias				Name of the Hive field shown in Navigator
	 * @param description		Description of the Hive field shown in Navigator
	 * @param tags				User-defined tags for the Hive field (custom metadata)
	 * @param properties		User-defined properties for the Hive field (custom metadata)
	 * @param customProperties	Managed custom properties for the Hive field (managed metadata)
	 * @param override			Whether to override existing tags, custom properties and managed custom properties
	 */
	public void updateHiveField(String databaseName,
								String tableName,
								String fieldName,
								String alias,
								String description,
								Collection<String> tags,
								Map<String, String> properties,
								Map<String, Map<String, Object>> customProperties,
								boolean override) {
		LOG.info(String.format("Updates the metadata of the Hive field: %s.%s.%s", databaseName, tableName, fieldName));
		Entity entity = newHiveEntity(EntityType.FIELD.name(), databaseName, tableName, fieldName, alias, 
									  description, tags, properties, customProperties, override);
		entities.add(entity);
	}

	private Entity newHiveEntity(String type, 
								 String databaseName,
								 String tableName,
								 String fieldName,
								 String alias, 
								 String description,
								 Collection<String> tags,
								 Map<String, String> properties,
								 Map<String, Map<String, Object>> customProperties,
								 boolean override) {
		Entity entity = null;
		String identity = null;
		MetaExtractor extractor = new MetaExtractor();
		
		if (EntityType.DATABASE.name().equals(type)) {
			entity = new CustomHiveDatabase();
			((CustomHiveDatabase) entity).setDatabaseName(databaseName);
			
			identity = (String) extractor.getHiveDatabase(databaseName).next().get("identity");
		} else if (EntityType.TABLE.name().equals(type)) {
			entity = new CustomHiveTable();
			((CustomHiveTable) entity).setDatabaseName(databaseName);
			((CustomHiveTable) entity).setTableName(tableName);
			
			identity = (String) extractor.getHiveTable(databaseName, tableName).next().get("identity");
		} else if ("VIEW".equals(type)) {
			entity = new CustomHiveTable();
			((CustomHiveTable) entity).setDatabaseName(databaseName);
			((CustomHiveTable) entity).setTableName(tableName);
			
			identity = (String) extractor.getHiveView(databaseName, tableName).next().get("identity");
		} else if (EntityType.FIELD.name().equals(type)) {
			entity = new CustomHiveColumn();
			
			((CustomHiveColumn) entity).setDatabaseName(databaseName);
			((CustomHiveColumn) entity).setTableName(tableName);
			((CustomHiveColumn) entity).setColumnName(fieldName);
			
			identity = (String) extractor.getHiveField(databaseName, tableName, fieldName).next().get("identity");
		}
		
		entity.setIdentity(identity);
		
		//Gets the Hive Source
		Source hiveSource = client.getHMSSource();
		entity.setSourceId(hiveSource.getIdentity());
		
		if (StringUtils.isNotEmpty(alias)) {
			entity.setAlias(alias);
		}
		if (StringUtils.isNotEmpty(description)) {
			entity.setDescription(description);
		}
		if (override) {
			entity.setTags(tags);
		} else {
			if (CollectionUtils.isNotEmpty(tags)) {
				entity.addTags(tags);
			}
		}
		if (override) {
			entity.setProperties(properties);
		} else {
			if (MapUtils.isNotEmpty(properties)) {
				entity.addProperties(properties);
			}
		}
		
	    if (customProperties != null && !customProperties.isEmpty()) {
	    	((CustomEntity) entity).setCustomProperties(customProperties);
	    }
	    
	    LOG.info("Creates an Hive entity object.");
		return entity;
	}
	
	/**
	 * Flushes the writer. Metadata batch is written to Navigator only after flushing.
	 */
	public void flush() {
		if (CollectionUtils.isNotEmpty(entities)) {
			LOG.info("Writes metadata to Navigator.");
			//Write metadata
			ResultSet results = plugin.write(entities);
			entities.clear();
			if (results.hasErrors()) {
				throw new RuntimeException(results.toString());
			}
		}
	}

}
