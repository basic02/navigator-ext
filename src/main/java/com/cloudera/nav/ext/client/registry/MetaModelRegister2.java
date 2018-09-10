package com.cloudera.nav.ext.client.registry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.model.custom.CustomProperty;
import com.cloudera.nav.sdk.model.custom.CustomPropertyType;
import com.cloudera.nav.sdk.model.custom.MetaClass;
import com.cloudera.nav.sdk.model.custom.MetaClassPackage;
import com.cloudera.nav.sdk.model.custom.Namespace;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Gets information about models and adds custom models.
 */
public class MetaModelRegister2 {
	
	private static final Logger LOG = LoggerFactory.getLogger(MetaModelRegister2.class);
	
	private ClientConfig config; //Client configuration

	/**
	 * Constructs a metadata model register.
	 */
	public MetaModelRegister2() {
		this.config = NavigatorPlugin.fromConfigFile("navigator.conf").getConfig();
	}
	
	/**
	 * Gets all source types, entity types, and properties.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/all
	 * 
	 * @return Returns source types, entity types, and properties.
	 */
	public Collection<Map<String, Object>> getAll() throws IOException {
		LOG.info("Gets all source types, entity types, and properties.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/all");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	//------------------------------------------
	
	/**
	 * Gets all packages (including default package).
	 * REST GET URL: http://host:port/api/{apiVersion}/models/packages
	 * 
	 * @return Returns all packages.
	 */
	public Collection<Map<String, Object>> getAllPackages() throws IOException {
		LOG.info("Gets all packages (including default package).");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets package by package name.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/packages/{name}
	 * 
	 * @param packageName	Package name
	 * @return Returns the package with the given name. 
	 */
	public Collection<Map<String, Object>> getPackage(String packageName) throws IOException {
		LOG.info("Gets package by package name: " + packageName);
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages/" + packageName);
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Creates a new package or returns an error if another package with the same name already exists.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/packages
	 * 
	 * @param packageName	Package name
	 * @param displayName	Package display name
	 * @param description	Package description
	 */
	public void createPackage(String packageName, String displayName, String description) throws IOException {
		LOG.info("Creates a new package with name: " + packageName);
		MetaClassPackage metapackage = MetaClassPackage.newPackage(packageName);
		metapackage.setDisplayName(displayName);
		metapackage.setDescription(description);
		createPackage(metapackage);
	}
	
	/**
	 * Creates a new package or returns an error if another package with the same name already exists.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/packages
	 * 
	 * @param metapackage Meta class package object
	 */
	public void createPackage(MetaClassPackage metapackage) throws IOException {
		Preconditions.checkArgument(metapackage != null, "Augument metapackage cannot be null");
		
		LOG.info("Creates a new package or returns an error if another package with the same name already exists.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages");
	    
		String postData = constructPackageBody(metapackage);
		sendRequest(apiUrl, HttpMethod.POST.name(), postData);
	}
	
	private String constructPackageBody(MetaClassPackage metapackage) {
		StringBuilder postStr = new StringBuilder("{");

		postStr.append("\"name\" : ");
		postStr.append("\"").append(metapackage.getName()).append("\"");
		if (StringUtils.isNotEmpty(metapackage.getDisplayName())) {
			postStr.append(", \"displayName\" : ");
			postStr.append("\"").append(metapackage.getDisplayName()).append("\"");
		}
		if (StringUtils.isNotEmpty(metapackage.getDescription())) {
			postStr.append(", \"description\" : ");
			postStr.append("\"").append(metapackage.getDescription()).append("\"");
		}
		postStr.append(", \"external\" : ");
		postStr.append(String.valueOf(metapackage.isExternal()));
		postStr.append(", \"defaultNamespace\" : ");
		postStr.append(String.valueOf(metapackage.isDefaultPackage()));
		
		postStr.append("}");
		
		return postStr.toString();
	}
	
	//------------------------------------------
	
	/**
	 * Bulk API to retrieve all available MetaClasses, both default and custom.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/classes
	 * 
	 * @return Returns all classes.
	 */
	public Collection<Map<String, Object>> getAllClasses() throws IOException {
		LOG.info("Retrieves all available MetaClasses, both default and custom.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/classes");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets meta-classes under the given package.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes
	 * 
	 * @param packageName	Package name
	 * @return Returns all classes under the given package.
	 */
	public Collection<Map<String, Object>> getAllClasses(String packageName) throws IOException {
		LOG.info("Gets meta-classes under the given package: " + packageName);
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages/" + packageName + "/classes");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets package names and class names of all classes associated with specified property.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties/{name}/classes
	 * 
	 * @param namespaceName	Namespace name
	 * @param propertyName	Property name
	 * @return Returns all classes associated with specified property. 
	 */
	public Collection<Map<String, Object>> getAllClasses(String namespaceName, String propertyName) throws IOException {
		LOG.info(String.format(
				"Gets package names and class names of all classes associated with specified property: %s.%s", 
				namespaceName, 
				propertyName));
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + namespaceName + "/properties/" + propertyName + "/classes");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets a meta class for the given name under the given package.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes/{class}
	 * 
	 * @param packageName	Package name
	 * @param className		Class name
	 * @return Returns the class for the given name under the given the given package.
	 */
	public Collection<Map<String, Object>> getClass(String packageName, String className) throws IOException {
		LOG.info(String.format("Gets a meta class for the given name under the given package: %s.%s", 
					packageName, className));
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages/" + packageName + "/classes/" + className);
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Creates a custom metadata class or returns an error if another class with the same name under the same package exists.
	 * Creating a class under then system nav package is not allowed and will result in an error
	 * REST POST URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes
	 * 
	 * @param className		Meta class name
	 * @param packageName	Meta package name
	 * @param displayName	Class display name
	 * @param description	Class description
	 */
	public void createClass(String className, String packageName, String displayName, String description) throws IOException {
		LOG.info(String.format("Creates a custom metadata class: %s.%s", packageName, className));
		MetaClass metaclass = MetaClass.newClass(packageName, className);
		metaclass.setDisplayName(displayName);
		metaclass.setDescription(description);
		createClass(metaclass);
	}
	
	/**
	 * Creates a custom metadata class or returns an error if another class with the same name under the same package exists. 
	 * Creating a class under then system nav package is not allowed and will result in an error
	 * REST POST URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes
	 * 
	 * @param metaclass	Meta class object
	 */
	public void createClass(MetaClass metaclass) throws IOException {
		Preconditions.checkArgument(metaclass != null, "Augument metaclass cannot be null");
		
		LOG.info("Creates a custom metadata class or returns an error if another class with the same name under the same package exists.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages/" + metaclass.getPackageName() + "/classes");
	    
		String postData = constructMetaclassBody(metaclass);
		sendRequest(apiUrl, HttpMethod.POST.name(), postData);
	}
	
	private String constructMetaclassBody(MetaClass metaclass) {
		StringBuilder postStr = new StringBuilder("{");

		postStr.append("\"name\" : ");
		postStr.append("\"").append(metaclass.getName()).append("\"");
		if (StringUtils.isNotEmpty(metaclass.getDisplayName())) {
			postStr.append(", \"displayName\" : ");
			postStr.append("\"").append(metaclass.getDisplayName()).append("\"");
		}
		if (StringUtils.isNotEmpty(metaclass.getDescription())) {
			postStr.append(", \"description\" : ");
			postStr.append("\"").append(metaclass.getDescription()).append("\"");
		}
		
		postStr.append("}");
		
		return postStr.toString();
	}
	
	//------------------------------------------
	
	/**
	 * Gets all namespaces (including default namespace).
	 * REST GET URL: http://host:port/api/{apiVersion}/models/namespaces
	 * 
	 * @return Returns all namespaces.
	 */
	public Collection<Map<String, Object>> getAllNamespaces() throws IOException {
		LOG.info("Gets all namespaces (including default namespace).");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets namespace by name.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}
	 * 
	 * @param namespaceName Namespace name
	 * @return Returns the namespace with the given name
	 */
	public Collection<Map<String, Object>> getNamespace(String namespaceName) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName), "Augument namespaceName cannot be null");
		
		LOG.info("Gets namespace by name: " + namespaceName);
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + namespaceName);
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Deletes empty custom property namespace. An error will result if the namespace has properties associated with it.
	 * REST DELETE URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}
	 * 
	 * @param namespaceName Namespace name
	 */
	public void deleteNamespace(String namespaceName) throws IOException {
		LOG.info("Deletes empty custom property namespace: " + namespaceName);
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + namespaceName);
	    
		sendRequest(apiUrl, HttpMethod.DELETE.name(), null);
	}
	
	/**
	 * Creates a new namespace or returns an error if another namespace with the same name already exists.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/namespaces
	 * 
	 * @param namespaceName Namespace name
	 * @param displayName	Namespace display name
	 * @param description	Namespace description
	 */
	public void createNamespace(String namespaceName, String displayName, String description) throws IOException {
		LOG.info("Creates a new namespace: " + namespaceName);
		Namespace namespace = Namespace.newNamespace(namespaceName);
		namespace.setDisplayName(displayName);
		namespace.setDescription(description);
		createNamespace(namespace);
	}
	
	/**
	 * Creates a new namespace or returns an error if another namespace with the same name already exists.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/namespaces
	 * 
	 * @param namespace Namespace object
	 */
	public void createNamespace(Namespace namespace) throws IOException {
		Preconditions.checkArgument(namespace != null, "Augument namespace cannot be null");
		
		LOG.info("Creates a new namespace or returns an error if another namespace with the same name already exists.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces");
	    
		String postData = constructNamespaceBody(namespace);
		sendRequest(apiUrl, HttpMethod.POST.name(), postData);
	}
	
	private String constructNamespaceBody(Namespace namespace) {
		StringBuilder postStr = new StringBuilder("{");

		postStr.append("\"name\" : ");
		postStr.append("\"").append(namespace.getName()).append("\"");
		if (StringUtils.isNotEmpty(namespace.getDisplayName())) {
			postStr.append(", \"displayName\" : ");
			postStr.append("\"").append(namespace.getDisplayName()).append("\"");
		}
		if (StringUtils.isNotEmpty(namespace.getDescription())) {
			postStr.append(", \"description\" : ");
			postStr.append("\"").append(namespace.getDescription()).append("\"");
		}
		postStr.append(", \"external\" : ");
		postStr.append(String.valueOf(namespace.isExternal()));
		postStr.append(", \"defaultNamespace\" : ");
		postStr.append(String.valueOf(namespace.isDefaultNamespace()));
		
		postStr.append("}");
		
		return postStr.toString();
	}
	
	//------------------------------------------
	
	/**
	 * Bulk API to retrieve all custom properties.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/properties
	 * 
	 * @return Returns all properties.
	 */
	public Collection<Map<String, Object>> getAllProperties() throws IOException {
		LOG.info("Retrieves all custom properties.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/properties");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets custom properties for namespace.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties
	 * 
	 * @param namespaceName Namespace name
	 * @return Returns all properties under the given namespace.
	 */
	public Collection<Map<String, Object>> getAllProperties(String namespaceName) throws IOException {
		LOG.info("Gets custom properties for namespace: " + namespaceName);
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + namespaceName + "/properties");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets custom properties associated with given class.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes/{class}/properties
	 * 
	 * @param packageName Package name
	 * @param className	  Class name
	 * @return Returns all properties associated with the given class.
	 */
	public Collection<Map<String, Object>> getAllProperties(String packageName, String className) throws IOException {
		LOG.info(String.format("Gets custom properties associated with given class: %s.%s", packageName, className));
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages/" + packageName + "/classes/" + className + "/properties");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	/**
	 * Gets custom property.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties/{property}
	 * 
	 * @param namespaceName Namespace name
	 * @param propertyName  Property name
	 * @return Returns the property with the given name under the given namespace.
	 */
	public Collection<Map<String, Object>> getProperty(String namespaceName, String propertyName) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName), "Augument namespaceName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(propertyName), "Augument propertyName cannot be null");
		
		LOG.info(String.format("Gets custom property: %s.%s", namespaceName, propertyName));
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + namespaceName + "/properties/" + propertyName);
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	} 
	
	/**
	 * Creates a new custom property or returns an error if a field with the same name for the same namespace+package exist.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties
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
	public void createProperty(String namespaceName,
							   String propertyName,
							   String displayName, 
							   String description,
							   CustomPropertyType propertyType,
							   boolean multiValued, 
							   String[] values,
							   Integer maxLength,
							   String pattern) throws IOException {
		LOG.info(String.format("Creates a new custom property: %s.%s", namespaceName, propertyName));
		CustomProperty property = 
				CustomProperty.newProperty(namespaceName, propertyName, propertyType, multiValued, values);
		property.setDisplayName(displayName);
		property.setDescription(description);
		property.setMaxLength(maxLength);
		property.setPattern(pattern);
		createProperty(property);
	}
	
	/**
	 * Creates a new custom property or returns an error if a field with the same name for the same namespace+package exist.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties
	 * 
	 * @param property Property object
	 */
	public void createProperty(CustomProperty property) throws IOException {
		Preconditions.checkArgument(property != null, "Argument property cannot be null");
		
		LOG.info("Creates a new custom property or returns an error if a field with the same name for the same namespace+package exist.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + property.getNamespace() + "/properties");
		
		String postData = constructPropertyBody(property);
		sendRequest(apiUrl, HttpMethod.POST.name(), postData);
	}
	
	private String constructPropertyBody(CustomProperty property) {
		StringBuilder postStr = new StringBuilder("{");

		postStr.append("\"name\" : ");
		postStr.append("\"").append(property.getName()).append("\"");
		if (StringUtils.isNotEmpty(property.getDisplayName())) {
			postStr.append(", \"displayName\" : ");
			postStr.append("\"").append(property.getDisplayName()).append("\"");
		}
		if (StringUtils.isNotEmpty(property.getDescription())) {
			postStr.append(", \"description\" : ");
			postStr.append("\"").append(property.getDescription()).append("\"");
		}
		postStr.append(", \"namespace\" : ");
		postStr.append("\"").append(property.getNamespace()).append("\"");
		postStr.append(", \"type\" : ");
		postStr.append("\"").append(property.getPropertyType().name()).append("\"");
		postStr.append(", \"multiValued\" : ");
		postStr.append(String.valueOf(property.isMultiValued()));
		if (property.getMaxLength() != null) {
			postStr.append(", \"maxLength\" : ");
			postStr.append(String.valueOf(property.getMaxLength()));
		}
		if (StringUtils.isNotEmpty(property.getPattern())) {
			postStr.append(", \"pattern\" : ");
			postStr.append("\"").append(property.getPattern()).append("\"");
		}
		if (property.getEnumValues() != null && property.getEnumValues().size() > 0) {
			postStr.append(", \"enumValues\" : ");
			Set<String> values = Sets.newHashSet();
		    for (String value : property.getEnumValues()) {
		    	values.add("\"" + value + "\"");
		    }
		    postStr.append("[").append(Joiner.on(", ").join(values)).append("]");
		}
		
		postStr.append("}");
		
		return postStr.toString();
	}
	
	/**
	 * Updates disabled, display name, description, or validation criteria (enums, regex pattern, and max length).
	 * REST PUT URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties/{property}
	 * 
	 * @param namespaceName	Namespace name
	 * @param propertyName	Property name
	 * @param displayName	Property display name
	 * @param description	Property description
	 * @param disabled		Disabled?
	 * @param values		Enum values
	 * @param maxLength		Maximum length constraint
	 * @param pattern		The pattern specified by a regular expression
	 */
	public void updateProperty(String namespaceName,
							   String propertyName,
							   String displayName, 
							   String description,
							   boolean disabled, 
							   String[] values,
							   Integer maxLength,
							   String pattern) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName), "Argument namespaceName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(propertyName), "Argument propertyName cannot be null");
		
		LOG.info("Updates disabled, display name, description, or validation criteria (enums, regex pattern, and max length).");
		
		String apiUrl = joinUrlPath(
				joinUrlPath(config.getNavigatorUrl(),
						"api/v" + String.valueOf(config.getApiVersion())),
				"models/namespaces/" + namespaceName + "/properties/" + propertyName);

		StringBuilder postStr = new StringBuilder("{");
		
		postStr.append("\"disabled\" : ");
		postStr.append(String.valueOf(disabled));

		if (StringUtils.isNotEmpty(displayName)) {
			postStr.append(", \"displayName\" : ");
			postStr.append("\"").append(displayName).append("\"");
		}
		if (StringUtils.isNotEmpty(description)) {
			postStr.append(", \"description\" : ");
			postStr.append("\"").append(description).append("\"");
		}
		if (maxLength != null) {
			postStr.append(", \"maxLength\" : ");
			postStr.append(String.valueOf(maxLength));
		}
		if (StringUtils.isNotEmpty(pattern)) {
			postStr.append(", \"pattern\" : ");
			postStr.append("\"").append(pattern).append("\"");
		}
		if (values != null && values.length > 0) {
			postStr.append(", \"enumValues\" : ");
			Set<String> valueSet = Sets.newHashSet();
		    for (String value : values) {
		    	valueSet.add("\"" + value + "\"");
		    }
		    postStr.append("[").append(Joiner.on(", ").join(values)).append("]");
		}
		
		postStr.append("}");
		
		sendRequest(apiUrl, HttpMethod.PUT.name(), postStr.toString());
	}
	
	/**
	 * Adds the specified enum values to the property. Values already defined for the property will be ignored.
	 * REST PUT URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties/{property}
	 * 
	 * @param namespaceName Namespace name
	 * @param propertyName	Property name
	 * @param values		Enum values
	 */
	public void updateProperty(String namespaceName,
							   String propertyName,
							   String[] values) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName), "Argument namespaceName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(propertyName), "Argument propertyName cannot be null");
		Preconditions.checkArgument(values != null && values.length > 0, "Argument values cannot be empty");
		
		LOG.info("Adds the specified enum values to the property. Values already defined for the property will be ignored.");

		String apiUrl = joinUrlPath(
				joinUrlPath(config.getNavigatorUrl(),
						"api/v" + String.valueOf(config.getApiVersion())),
				"models/namespaces/" + namespaceName + "/properties/" + propertyName);

		StringBuilder postStr = new StringBuilder("{");
		postStr.append("\"enumValues\" : ");
		Set<String> valueSet = Sets.newHashSet();
		for (String value : values) {
			valueSet.add("\"" + value + "\"");
		}
		postStr.append("[").append(Joiner.on(", ").join(values)).append("]");
		postStr.append("}");
		
		sendRequest(apiUrl, HttpMethod.POST.name(), postStr.toString());
	}
	
	/**
	 * Convenience for disabling a custom property. A disabled property's value maybe deleted but not modified.
	 * REST DELETE URL: http://host:port/api/{apiVersion}/models/namespaces/{namespace}/properties/{property}
	 * 
	 * @param namespaceName Namespace name
	 * @param propertyName	Property name
	 */
	public void deleteProperty(String namespaceName, String propertyName) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName), "Argument namespaceName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(propertyName), "Argument propertyName cannot be null");
		
		LOG.info(String.format("Disables a custom property: %s.%s", namespaceName, propertyName));
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/namespaces/" + namespaceName + "/properties/" + propertyName);
	    
		sendRequest(apiUrl, HttpMethod.DELETE.name(), null);
	}
	
	//------------------------------------------
	
	/**
	 * Adds the specified property to the specified class.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes/{class}/properties
	 * 
	 * @param packageName	Package name
	 * @param className		Class name
	 * @param namespaceName	Namespace name
	 * @param propertyName	Property name
	 */
	public void createMapping(String packageName, 
							  String className, 
							  String namespaceName, 
							  String propertyName) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(packageName), "Augument packageName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(className), "Argument className cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(namespaceName), "Argument namespaceName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(propertyName), "Argument propertyName cannot be null");
		
		LOG.info(String.format("Adds the specified property %s.%s to the specified class %s.%s", 
				namespaceName, propertyName, packageName, className));
		createMappings(packageName, className, Sets.newHashSet(namespaceName + "." + propertyName));
	}
	
	/**
	 * Adds the specified properties to the specified class.
	 * REST POST URL: http://host:port/api/{apiVersion}/models/packages/{package}/classes/{class}/properties
	 * 
	 * @param packageName	Package name
	 * @param className		Class name
	 * @param properties	Set<namespace.property>
	 */
	public void createMappings(String packageName, 
							   String className, 
							   Set<String> properties) throws IOException {
		Preconditions.checkArgument(StringUtils.isNotEmpty(packageName), "Augument packageName cannot be null");
		Preconditions.checkArgument(StringUtils.isNotEmpty(className), "Argument className cannot be null");
		Preconditions.checkArgument(properties != null && !properties.isEmpty(), "Argument properties cannot be empty");
		
		LOG.info("Adds the specified properties to the specified class.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/packages/" + packageName + "/classes/" + className + "/properties");
	    String postData = constructMappingBody(properties);
		
		sendRequest(apiUrl, HttpMethod.POST.name(), postData);
	}
	
	private String constructMappingBody(Set<String> properties) {
		StringBuilder postStr = new StringBuilder("[");
		
		for (String property : properties) {
			postStr.append("{");
			
			String[] strs = property.split("\\.");
			String namespaceName = strs[0];
			String propertyName = strs[1];
			postStr.append("\"name\" : ");
			postStr.append("\"").append(propertyName).append("\"");
			postStr.append(", \"namespace\" : ");
			postStr.append("\"").append(namespaceName).append("\"");
			
			postStr.append("},");
		}
	
		postStr.deleteCharAt(postStr.length()-1);
		postStr.append("]");
		
		return postStr.toString();
	}
	
	/**
	 * Gets complete mapping of packages -> classes -> properties.
	 * REST GET URL: http://host:port/api/{apiVersion}/models/properties/mappings
	 * 
	 * @return Returns all mappings of classes -> properties.
	 */
	public Collection<Map<String, Object>> getAllMappings() throws IOException {
		LOG.info("Gets complete mapping of packages -> classes -> properties.");
		String apiUrl = joinUrlPath(
	            joinUrlPath(config.getNavigatorUrl(),
	                "api/v" + String.valueOf(config.getApiVersion())),
	                "models/properties/mappings");
	    
		return sendRequest(apiUrl, HttpMethod.GET.name(), null);
	}
	
	//------------------------------------------
	
	private Collection<Map<String, Object>> sendRequest(String apiUrl, String requestMethod, String postData) throws IOException {
		HttpURLConnection conn = null;
		OutputStream out = null;
		InputStream in = null;
		Collection<Map<String, Object>> models = null;
		
		try {
			URL url = new URL(apiUrl);
		    conn = (HttpURLConnection) url.openConnection();
		    String userpass = config.getUsername() + ":" + config.getPassword();
		    String basicAuth = "Basic " + new String(Base64.encodeBase64(userpass.getBytes()));
		    conn.addRequestProperty("Authorization", basicAuth);
		    conn.addRequestProperty("Content-Type", "application/json");
		    conn.addRequestProperty("Accept", "application/json");
		    conn.setDoOutput(true);
		    conn.setDoInput(true);
		    //conn.setReadTimeout(0);
		    conn.setRequestMethod(requestMethod);
		    
		    LOG.debug(String.format("Sends %s request to Navigator API: %s", requestMethod, apiUrl));
		    if (HttpMethod.POST.name().equals(requestMethod) || HttpMethod.PUT.name().equals(requestMethod)) {
		    	LOG.debug("Post data: " + postData);
		    	out = conn.getOutputStream();
		    	PrintWriter writer = new PrintWriter(out);
			    writer.write(postData);
			    writer.flush();
		    } else {
		    	conn.connect();
		    }
		    
		    if (conn.getResponseCode() >= HttpStatus.SC_BAD_REQUEST) {
		    	in = conn.getErrorStream();
		    } else {
		    	in = conn.getInputStream();
		    }
		    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		    StringBuilder sb = new StringBuilder();
		    String responseBody;
		    while ((responseBody = reader.readLine()) != null) {
		    	sb.append(responseBody);
		    }
		    responseBody = sb.toString().trim();
		    
		    if (conn.getResponseCode() >= HttpStatus.SC_BAD_REQUEST) {
		    	//Throw error message
		    	throw new IOException(String.format(
		              "Error sending request (code %s): %s %s", conn.getResponseCode(),
		              conn.getResponseMessage(), responseBody));
		    }
		    
		    if (!StringUtils.isEmpty(responseBody)) {
		    	LOG.debug("Response body: " + responseBody);
			}
		    
		    if (!responseBody.startsWith("[")) {
		    	responseBody = "[" + responseBody + "]";
		    }
		    
		    ObjectMapper mapper = new ObjectMapper();
		    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
		    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
		    models = mapper.readValue(responseBody, new TypeReference<Collection<Map<String, Object>>>() {});
		} catch (IOException ioe) {
			Throwables.propagate(ioe);
		} finally {
			if (out != null) {
				out.close();
			}
			if (in != null) {
				in.close();
			}
			IOUtils.close(conn);
		}
		
		return models;
	}
	
	private static String joinUrlPath(String base, String component) {
	    return base + (base.endsWith("/") ? "" : "/") + component;
	}

}
