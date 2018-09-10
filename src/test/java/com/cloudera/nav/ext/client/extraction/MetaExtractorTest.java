package com.cloudera.nav.ext.client.extraction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.cloudera.nav.ext.client.extraction.MetaExtractor;

public class MetaExtractorTest {
	
	public static void main(String[] args) throws IOException {
		MetaExtractor extractor = new MetaExtractor();
		Iterator<Map<String, Object>> iter = extractor.getHiveView("default", "metrics_v");
		while (iter.hasNext()) {
			Map<String, Object> result = iter.next();
			System.out.println(result.toString());
		}
	}

}
