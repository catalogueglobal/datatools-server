package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.extensions.ExternalFeed;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by demory on 3/31/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class TransitLandFeed implements ExternalFeed {
    public String onestop_id;
    public String url;
    public String feed_format;
    public String license_name;
    public String license_url;
    public String license_use_without_attribution;
    public String license_create_derived_product;
    public String license_redistribute;
    public String license_attribution_text;
    public String last_fetched_at;
    public String last_imported_at;
    public String latest_fetch_exception_log;
    public String import_status;
    public String created_at;
    public String updated_at;
    public String feed_versions_count;
    public String feed_versions_url;
    public String[] feed_versions;
    public String active_feed_version;
    public String import_level_of_active_feed_version;
    public String created_or_updated_in_changeset_id;

    // The following fields are ignored because they are unlikely to be useful for the UI (for now).
    @JsonIgnore
    public String geometry;
    @JsonIgnore
    public String type;
    @JsonIgnore
    public String coordinates;
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