package com.cloudera.nav.ext.client.registry;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.model.MetadataModel;
import com.cloudera.nav.sdk.model.custom.CustomProperty;
import com.cloudera.nav.sdk.model.custom.CustomPropertyType;
import com.cloudera.nav.sdk.model.custom.Namespace;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Adds custom models.
 */
public class MetaModelRegister {
	
	private static final Logger LOG = LoggerFactory.getLogger(MetaModelRegister.class);

	private NavigatorPlugin plugin; //Plugin used to write metadata to Navigator
	private NavApiCient client; //Client to communicate with the Navigator REST API

	private Set<Namespace> namespaces;
	private Set<CustomProperty> properties;
	private Map<String, Set<String>> mappings;

	/**
	 * Constructs a metadata model register.
	 */
	public MetaModelRegister() {
		this.plugin = NavigatorPlugin.fromConfigFile("navigator.conf");
		this.client = this.plugin.getClient();
		this.namespaces = Sets.newHashSet();
		this.properties = Sets.newHashSet();
		this.mappings = Maps.newHashMap();
	}
	
	/**
	 * Adds a namespace.
	 * 
	 * @param namespaceName	Namespace name
	 * @param displayName	Namespace display name
	 * @param description	Namespace description
	 */
	public void addNamespace(String namespaceName, String displayName, String description) {
		LOG.info(String.format("Adds a namespace: %s", namespaceName));
		Namespace ns = Namespace.newNamespace(namespaceName);
		ns.setDisplayName(displayName);
		ns.setDescription(description);
		namespaces.add(ns);
	}

	/**
	 * Adds a property.
	 * 
	 * @param namespaceName	Namespace name
	 * @param propertyName	Property name
	 * @param displayName	Property display name
	 * @param description	Property description
	 * @param propertyType	Property type
	 * @param multiValued	Is multi-valued?
	 * @param values		Enum values
	 * @param maxLength		Maximum length constraint
	 * @param pattern		The pattern specified by a regular expression
	 */
	public void addProperty(String namespaceName,
							String propertyName,
							String displayName, 
							String description,
							CustomPropertyType propertyType,
							boolean multiValued, 
							String[] values,
							Integer maxLength,
							String pattern) {
		LOG.info(String.format("Adds a property: %s.%s", namespaceName, propertyName));
		CustomProperty property = 
				CustomProperty.newProperty(namespaceName, propertyName, propertyType, multiValued, values);
		property.setDisplayName(displayName);
		property.setDescription(description);
		property.setMaxLength(maxLength);
		property.setPattern(pattern);
		properties.add(property);
	}
	
	/**
	 * Map<package.class, Set<namespace.property>>
	 * 
	 * @param packageName	Package name
	 * @param className		Meta class name
	 * @param namespaceName	Namespace name
	 * @param propertyName	Property name
	 */
	public void addMapping(String packageName, String className, String namespaceName, String propertyName) {
		LOG.info(String.format("Adds a mapping from %s.%s to %s.%s", packageName, className, namespaceName, propertyName));
		Preconditions.checkArgument(StringUtils.isNotEmpty(packageName));
		Preconditions.checkArgument(StringUtils.isNotEmpty(className));
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName));
		Preconditions.checkArgument(StringUtils.isNotEmpty(propertyName));
		
		String fullClassName = packageName + "." + className;
		String fullPropertyName = namespaceName + "." + propertyName;
		
		if (mappings.containsKey(fullClassName)) {
			Set<String> fullPropertyNames = mappings.get(fullClassName);
			if (!fullPropertyNames.contains(fullPropertyName)) {
				fullPropertyNames.add(fullPropertyName);
			}
		} else {
			Set<String> fullPropertyNames = Sets.newHashSet(fullPropertyName);
			mappings.put(fullClassName, fullPropertyNames);
		}
	}
	
	/**
	 * Commits current batch.
	 */
	public void commit() {
		LOG.info("Commits current batch.");
		MetadataModel model = new MetadataModel();
		model.setNamespaces(namespaces);
		model.setProperties(properties);
		model.setMappings(mappings);

		client.registerModels(model);
		clear();
	}
	
	/**
	 * Clears current batch.
	 */
	public void clear() {
		LOG.info("Clears current batch.");
		namespaces.clear();
		properties.clear();
		mappings.clear();
	}
	
}
