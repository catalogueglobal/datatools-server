package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.extensions.ExternalFeed;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by demory on 3/31/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class TransitLandFeed implements ExternalFeed {

    @JsonProperty
    public String onestop_id;

    @JsonProperty
    public String url;

    @JsonProperty
    public String feed_format;

//    @JsonProperty
//    public String tags;

    @JsonIgnore
    public String geometry;

    @JsonIgnore
    public String type;

    @JsonIgnore
    public String coordinates;

    @JsonProperty
    public String license_name;

    @JsonProperty
    public String license_url;

    @JsonProperty
    public String license_use_without_attribution;

    @JsonProperty
    public String license_create_derived_product;

    @JsonProperty
    public String license_redistribute;

    @JsonProperty
    public String license_attribution_text;

    @JsonProperty
    public String last_fetched_at;

    @JsonProperty
    public String last_imported_at;

    @JsonProperty
    public String latest_fetch_exception_log;

    @JsonProperty
    public String import_status;

    @JsonProperty
    public String created_at;

    @JsonProperty
    public String updated_at;

    @JsonProperty
    public String feed_versions_count;

    @JsonProperty
    public String feed_versions_url;

    @JsonProperty
    public String[] feed_versions;

    @JsonProperty
    public String active_feed_version;

    @JsonProperty
    public String import_level_of_active_feed_version;

    @JsonProperty
    public String created_or_updated_in_changeset_id;

    @JsonIgnore
    public String changesets_imported_from_this_feed;

    @JsonIgnore
    public String operators_in_feed;

    @JsonIgnore
    public String gtfs_agency_id;

    @JsonIgnore
    public String operator_onestop_id;

    @JsonIgnore
    public String feed_onestop_id;

    @JsonIgnore
    public String operator_url;

    @JsonIgnore
    public String feed_url;

    public TransitLandFeed() { }

    @Override
    public String getName() {
        return onestop_id;
    }

    @Override
    public String getId() {
        return onestop_id;
    }

    @Override
    public String getIdKey() {
        return "onestop_id";
    }

    @Override
    public String getFeedUrl() {
        return url;
    }

}