package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by demory on 3/30/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalFeedSourceProperty extends Model {
    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(ExternalFeedSourceProperty.class);

    // constructor for data dump load
    public ExternalFeedSourceProperty() {}

    public ExternalFeedSourceProperty(FeedSource feedSource, String resourceType, String name, String value) {
        this.id = constructId(feedSource, resourceType, name);
        this.feedSourceId = feedSource.id;
        this.resourceType = resourceType;
        this.name = name;
        this.value = value;
    }

    public static String constructId(FeedSource feedSource, String resourceType, String name) {
        return feedSource.id + "_" + resourceType + "_" + name;
    }

    public String resourceType;

    public String feedSourceId;

    public String name;

    public String value;

    public static Map<String, String> propertiesToMap (List<ExternalFeedSourceProperty> properties) {
        Map<String, String> propTable = new HashMap<>();
        if (properties == null) {
            LOG.error("List of properties cannot be null.");
            return propTable;
        }
        properties.forEach(prop -> propTable.put(prop.name, prop.value));
        return propTable;
    }
}
