package com.cloudera.nav.ext.client.registry;

import com.cloudera.nav.sdk.model.custom.CustomPropertyType;

public class MetaModelRegisterTest {
	
	public static void main(String[] args) {
		MetaModelRegister register = new MetaModelRegister();
		register.addNamespace("namespace111", null, "namespace111");
		register.addProperty("namespace111", 
							 "property111", 
							 "property111", 
							 "property111", 
							 CustomPropertyType.TEXT, 
							 true, 
							 null,
							 //new String[]{"finance", "supply chain", "development center"}, 
							 100, 
							 null);
		register.addMapping("nav", "hv_table", "namespace111", "property111");
		register.commit();
	}

}
