package com.conveyal.datatools.manager.controllers.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedSourceDto {
    public boolean deployable;
    public String id;
    public boolean isPublic;
    public String name;
    public String projectId;
    public String retrievalMethod;
    public String snapshotVersion;
    public String url;
}
