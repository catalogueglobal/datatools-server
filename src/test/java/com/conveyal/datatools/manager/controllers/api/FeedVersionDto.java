package com.conveyal.datatools.manager.controllers.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedVersionDto {
    public String feedSourceId;
    public String id;
    public String name;
    public int version;
}
