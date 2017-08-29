package com.conveyal.datatools.manager.extensions.transitfeeds;

import com.conveyal.datatools.manager.extensions.ExternalFeed;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by landon on 8/28/17.
 */
public class TransitFeedsFeed implements ExternalFeed {
    public String id;
    public String name;
    public String url;

    public TransitFeedsFeed (String id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getIdKey() {
        return "id";
    }

    @Override
    public String getFeedUrl() {
        return url;
    }
}
