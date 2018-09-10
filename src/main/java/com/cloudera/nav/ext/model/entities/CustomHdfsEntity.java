package com.cloudera.nav.ext.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A custom HDFS entity (directories or files) that supports managed custom metadata.
 * Note that the source type and namespace should not be modified.
 */
@MClass(model = "fselement", validTypes = {EntityType.DIRECTORY, EntityType.FILE})
public class CustomHdfsEntity extends CustomEntity {
	
	@MProperty
	private String fileSystemPath;
	
	private final String FILE_SYSTEM_PATH = "fileSystemPath";
	
	public CustomHdfsEntity() {
		setSourceType(SourceType.HDFS);
	}
	
	public CustomHdfsEntity(String fileSystemPath, EntityType type, String sourceId) {
		this();
		setSourceId(sourceId);
		setFileSystemPath(fileSystemPath);
		setEntityType(type);
	}
	
	public CustomHdfsEntity(String id) {
		this();
		setIdentity(id);
	}
	
	/**
	 * Either the entity id must be present or the file system path must be
	 * present for a CustomHdfsEntity to be valid. The source id must also be
	 * present for the CustomHdfsEntity to be valid.
	 */
	@Override
	public void validateEntity() {
		if ((Strings.isNullOrEmpty(this.getIdentity()) &&
				Strings.isNullOrEmpty(this.getFileSystemPath())) ||
				Strings.isNullOrEmpty(this.getSourceId())) {
			throw new IllegalArgumentException(
					"Either the Entity Id or file system path used" +
					" to generate the id must be present along with the source id");
		}
	}
	
	public void setFileSystemPath(String fileSystemPath) {
		this.fileSystemPath = fileSystemPath;
	}
	
	public String getFileSystemPath() {
		return this.fileSystemPath;
	}
	
	@Override
	public Map<String, String> getIdAttrsMap() {
		return ImmutableMap.of(FILE_SYSTEM_PATH, this.getFileSystemPath());
	}
	
}
