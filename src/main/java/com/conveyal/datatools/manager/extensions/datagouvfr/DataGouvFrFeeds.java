package com.conveyal.datatools.manager.extensions.datagouvfr;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by demory on 3/31/16.
 */

public class DataGouvFrFeeds {
    @JsonProperty
    List<DataGouvFrFeed> feeds = new ArrayList<DataGouvFrFeed>();

//        @JsonProperty
//        Map

//        @JsonProperty
//        List<DataGouvFrFeed> feeds;

    @JsonProperty
    Map<String, String> meta;
}
