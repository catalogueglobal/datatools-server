package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by demory on 3/30/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalFeedSourceProperty extends Model {
    private static final long serialVersionUID = 1L;

    private static DataStore<ExternalFeedSourceProperty> propertyStore = new DataStore<>("externalFeedSourceProperties");

    // constructor for data dump load
    public ExternalFeedSourceProperty() {}

    public ExternalFeedSourceProperty(FeedSource feedSource, String resourceType, String name, String value) {
        this.id = feedSource.id + "_" + resourceType + "_" + name;
        this.feedSourceId = feedSource.id;
        this.resourceType = resourceType;
        this.name = name;
        this.value = value;
    }

    @JsonProperty
    public String getFeedSourceId() {
        return feedSourceId;
    }

    public String resourceType;

    private String feedSourceId;

    public String name;

    public String value;

    public void save () {
        save(true);
    }

    public void save (boolean commit) {
        if (commit)
            propertyStore.save(id, this);
        else
            propertyStore.saveWithoutCommit(id, this);
    }

    /**
     * Commit changes to the datastore
     */
    public static void commit () {
        propertyStore.commit();
    }

    public static ExternalFeedSourceProperty find(FeedSource source, String resourceType, String name) {
        return propertyStore.getById(source.id + "_" +resourceType + "_" + name);
    }

    /**
     * Get multiple fields for a feed source and resource type.  Used for mapping to POJOs as needed.
     */
    public static Map<String, String> findMultiple(FeedSource source, String resourceType) {
        Map<String, String> map = new HashMap<>();
        getAll().stream()
                .filter(field ->
                        Objects.equals(field.resourceType, resourceType) &&
                        Objects.equals(field.feedSourceId, source.id)
                )
                .forEach(field -> map.put(field.name, field.value));
        return map;
    }

    public static ExternalFeedSourceProperty updateOrCreate(FeedSource source, String resourceType, String name, String value) {
        ExternalFeedSourceProperty prop =
                ExternalFeedSourceProperty.find(source, resourceType, name);

        if(prop == null) {
            prop = new ExternalFeedSourceProperty(source, resourceType, name, value);
        }
        else prop.value = value;

        prop.save();

        return prop;
    }

    public static Collection<ExternalFeedSourceProperty> getAll () {
        return propertyStore.getAll();
    }

    public void delete() {
        propertyStore.delete(this.id);
    }

}
