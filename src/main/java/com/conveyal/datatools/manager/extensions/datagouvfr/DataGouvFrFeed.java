package com.conveyal.datatools.manager.extensions.datagouvfr;

import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by demory on 3/31/16.
 */

public class DataGouvFrFeed {

    public static final Logger LOG = LoggerFactory.getLogger(DataGouvFrFeedResource.class);

    @JsonProperty
    String dgf_id;

    @JsonProperty
    String url;

    @JsonProperty
    String dataset_url;

    @JsonProperty
    String feed_format;

    @JsonProperty
    String tags;

    @JsonProperty
    String title;

    @JsonIgnore
    String geometry;

    @JsonIgnore
    String type;

    @JsonIgnore
    String coordinates;

    @JsonProperty
    String license_name;

    @JsonProperty
    String created_at;

    @JsonProperty
    String updated_at;

    @JsonIgnore
    String changesets_imported_from_this_feed;

    @JsonIgnore
    String operators_in_feed;

    @JsonIgnore
    String gtfs_agency_id;

    @JsonIgnore
    String operator_onestop_id;

    @JsonIgnore
    String feed_onestop_id;

    @JsonIgnore
    String operator_url;

    @JsonIgnore
    String feed_url;

    public DataGouvFrFeed(JsonNode jsonMap){
        JsonNode resource = null;
        for (final JsonNode node : jsonMap.get("resources")) {
            if (node.get("format").asText().contentEquals("GTFS") ||
                    node.get("format").asText().contentEquals("gtfs")) {
                resource = node;
                break;
            }
        }
        if (resource == null) {
            for (final JsonNode node : jsonMap.get("resources")) {
                if (node.get("format").asText().contentEquals("ZIP") ||
                        node.get("format").asText().contentEquals("zip")) {
                    resource = node;
                    break;
                }
            }
        }
        if (resource == null) {
            for (final JsonNode node : jsonMap.get("resources")) {
                if (node.get("format").asText().contentEquals("CSV") ||
                        node.get("format").asText().contentEquals("csv")) {
                    resource = node;
                    break;
                }
            }
        }
        if (resource == null) {
            resource = jsonMap.get("resources").elements().next();
        }

        this.dataset_url = jsonMap.get("uri").asText();
        this.url = resource.get("url").asText();
        this.dgf_id = jsonMap.get("id").asText();
        this.feed_format = resource.get("format").asText();
        this.tags = jsonMap.get("tags").asText();
        this.license_name = jsonMap.get("license").asText();
        this.created_at = jsonMap.get("created_at").asText();
        this.updated_at = jsonMap.get("last_update").asText();
        this.title = jsonMap.get("title").asText();
    }

    public void mapFeedSource(FeedSource source){

        // set the
        source.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
        try {
            source.url = new URL(this.url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        source.save();
    }
}
