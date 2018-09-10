package com.cloudera.nav.ext.model.entities;

//import java.util.Collection;
//import java.util.Iterator;
import java.util.Map;

//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.collections.MapUtils;

import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An abstract base class for creating entities with managed custom metadata. 
 * Concrete sub-classes that need to be persisted should have the @MClass annotation.
 * Only fields with getter methods annotated with @MProperty will be written.
 * Relationships with other Entities should be expressed in the form of a
 * field with a getter method annotated by @MRelation
 */
public abstract class CustomEntity extends Entity {
	
	//Managed custom properties
	@MProperty
	private Map<String, Map<String, Object>> customProperties;
		
	/**
	 * Get managed custom properties.
	 * 
	 * @return new, removed, and override managed properties
	 */
	public Map<String, Map<String, Object>> getCustomProperties() {
		if (customProperties == null) {
			customProperties = Maps.newHashMap();
		}
		return customProperties;
	}
	
	/**
	 * Set managed custom properties.
	 * 
	 * @param customProperties
	 */
	public void setCustomProperties(Map<String, Map<String, Object>> customProperties) {
		this.customProperties = customProperties;
	}
	
	/**
	 * Set one managed custom property.
	 * 
	 * @param namespace 	Namespace
	 * @param propertyName	Property name
	 * @param propertyValue	Property value
	 */
	public void setCustomProperty(String namespace, String propertyName, Object propertyValue) {
		if (getCustomProperties().containsKey(namespace)) {
			getCustomProperties().get(namespace).put(propertyName, propertyValue);
		} else {
			Map<String, Object> properties = Maps.newHashMap();
			properties.put(propertyName, propertyValue);
			getCustomProperties().put(namespace, properties);
		}
	}
	
	/**
	 * Set one managed custom multi-valued property.
	 * 
	 * @param namespace 		Namespace
	 * @param propertyName		Property name
	 * @param propertyValues	Multiple property values
	 */
	public void setCustomProperty(String namespace, String propertyName, Object... propertyValues) {
		if (getCustomProperties().containsKey(namespace)) {
			getCustomProperties().get(namespace).put(propertyName, Sets.newHashSet(propertyValues));
		} else {
			Map<String, Object> properties = Maps.newHashMap();
			properties.put(propertyName, Sets.newHashSet(propertyValues));
			getCustomProperties().put(namespace, properties);
		}
	}
	
	/**
	 * Add/update managed custom properties.
	 * 
	 * @param customProperties
	 */
	/*public void addCustomProperties(Map<String, Map<String, Object>> customProperties) {
		if (MapUtils.isNotEmpty(customProperties)) {
			Iterator<String> iter = customProperties.keySet().iterator();
			while (iter.hasNext()) {
				String namespace = iter.next();
				Map<String, Object> properties = customProperties.get(namespace);
				
				if (getCustomProperties().containsKey(namespace)) {
					getCustomProperties().get(namespace).putAll(properties);
				} else {
					getCustomProperties().put(namespace, properties);
				}
			}
		}	
	}*/
	
	/**
	 * Remove existing managed custom properties.
	 * 
	 * @param keys
	 */
	/*public void removeCustomProperties(Collection<String> keys) {
		if (CollectionUtils.isNotEmpty(keys)) {
			for (String key : keys) {
				Iterator<Map<String, Object>> propertiesIter = getCustomProperties().values().iterator();
				while (propertiesIter.hasNext()) {
					propertiesIter.next().remove(key);
				}
			}
		}
	}*/
	
}
