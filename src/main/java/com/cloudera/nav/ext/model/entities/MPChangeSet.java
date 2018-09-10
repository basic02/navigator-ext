package com.cloudera.nav.ext.model.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

/**
 * Add, remove, override managed custom properties
 */
public class MPChangeSet {

	@JsonProperty("add")
	private Map<String, Map<String, Object>> newCustomProperties;
	
	@JsonProperty("del")
	private Set<String> removeCustomProperties;
	
	@JsonProperty("set")
	private Map<String, Map<String, Object>> overrideCustomProperties;
	
	public MPChangeSet() {
		newCustomProperties = Maps.newHashMap();
		removeCustomProperties = Sets.newHashSet();
		overrideCustomProperties = null;
	}
	
	public void reset() {
		newCustomProperties.clear();
		removeCustomProperties.clear();
		overrideCustomProperties = null;
	}
	
	/**
	 * Add specified properties to be added.
	 * 
	 * @param customProperties
	 */
	public void addCustomProperties(Map<String, Map<String, Object>> customProperties) {
		if (MapUtils.isNotEmpty(customProperties)) {
			Iterator<String> namespaceIter = customProperties.keySet().iterator();
			while (namespaceIter.hasNext()) {
				String namespace = namespaceIter.next();
				Map<String, Object> properties = customProperties.get(namespace);
				
				if (newCustomProperties.containsKey(namespace)) {
					newCustomProperties.get(namespace).putAll(properties);
				} else {
					newCustomProperties.put(namespace, properties);
				}
				
				Set<String> keys = properties.keySet();
				removeCustomProperties.removeAll(keys);
				/*for (String key : keys) {
					String concatedKey = namespace + "." + key;
					removeCustomProperties.remove(concatedKey);
				}*/
				
				if (overrideCustomProperties != null && overrideCustomProperties.containsKey(namespace)) {
					Map<String, Object> overrideProps = overrideCustomProperties.get(namespace);
					overrideProps = Maps.newHashMap(Maps.difference(overrideProps, properties).entriesOnlyOnLeft());
					overrideCustomProperties.put(namespace, overrideProps);
				}
			} //while
		}
	}
	
	/**
	 * Add specified properties to be removed.
	 * 
	 * @param keys
	 */
	public void removeCustomProperties(Collection<String> keys) {
		if (CollectionUtils.isNotEmpty(keys)) {
			removeCustomProperties.addAll(keys);
			
			for (String k : keys) {
				Iterator<Map<String, Object>> propertiesIter = newCustomProperties.values().iterator();
				while (propertiesIter.hasNext()) {
					propertiesIter.next().remove(k);
				}
			}
      
			if (overrideCustomProperties != null) {
				for (String k : keys) {
					Iterator<Map<String, Object>> propertiesIter = overrideCustomProperties.values().iterator();
					while (propertiesIter.hasNext()) {
						propertiesIter.next().remove(k);
					}
				}
			}
		}
	}
	
	/**
	 * Replace existing properties with specified properties.
	 * 
	 * @param customProperties
	 */
	public void setCustomProperties(Map<String, Map<String, Object>> customProperties) {
		if (customProperties == null) {
			overrideCustomProperties = null;
		} else {
			Iterator<String> namespaceIter = customProperties.keySet().iterator();
			while (namespaceIter.hasNext()) {
				String namespace = namespaceIter.next();
				Map<String, Object> properties = customProperties.get(namespace);
				
				Set<String> keys = properties.keySet();
				removeCustomProperties.removeAll(keys);
				/*for (String key : keys) {
					String concatedKey = namespace + "." + key;
					removeCustomProperties.remove(concatedKey);
				}*/
				
				if (newCustomProperties.containsKey(namespace)) {
					Map<String, Object> newProps = newCustomProperties.get(namespace);
					newProps = Maps.newHashMap(Maps.difference(newProps, properties).entriesOnlyOnLeft());
					newCustomProperties.put(namespace, newProps);
				}
			} //while
			
      		overrideCustomProperties = Maps.newHashMap(customProperties);
		}
	}
	
	public Map<String, Map<String, Object>> getNewCustomProperties() {
		return newCustomProperties;
	}
	
	public void setNewCustomProperties(Map<String, Map<String, Object>> newCustomProperties) {
		if (newCustomProperties == null) {
			newCustomProperties = Maps.newHashMap();
		}
		this.newCustomProperties = newCustomProperties;
	}
	
	public Map<String, Map<String, Object>> getOverrideCustomProperties() {
		return overrideCustomProperties;
	}
	
	public void setOverrideCustomProperties(Map<String, Map<String, Object>> overrideCustomProperties) {
		this.overrideCustomProperties = overrideCustomProperties;
	}
	
	public boolean hasOverrides() {
		return overrideCustomProperties != null;
	}
	
	public Set<String> getRemoveCustomProperties() {
		return removeCustomProperties;
	}
	
	public void setRemoveCustomProperties(Set<String> removeCustomProperties) {
		if (removeCustomProperties == null) {
			removeCustomProperties = Sets.newHashSet();
		}
		this.removeCustomProperties = removeCustomProperties;
	}
	
	public static MPChangeSet copyOf(MPChangeSet properties) {
		MPChangeSet rs = new MPChangeSet();
		rs.overrideCustomProperties = Maps.newHashMap(properties.getOverrideCustomProperties());
		rs.newCustomProperties = Maps.newHashMap(properties.getNewCustomProperties());
		rs.removeCustomProperties = Sets.newHashSet(properties.getRemoveCustomProperties());
		return rs;
	}
  
}
