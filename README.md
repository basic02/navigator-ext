# Navigator SDK Extension Package

Navigator SDK provides Java development toolkit for metadata extraction and writing. But after using Navigator SDK, there are several limitations found:

1. The metadata model can only be registered through Java Annotation (@MClass, @MProperty, @MRelation).
2. The metadata write interface does not support writing managed metadata directly to the specified entity.
3. Metadata extraction interface is not easy to use. It needs to construct Solr query to query entity and relation.

Because of these limitations, an extension package, navigator-ext, was developed based on the Navigator SDK to extend the original SDK functionality and provide functionalities for model registration and metadata updates.
