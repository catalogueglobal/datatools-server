package com.conveyal.datatools.manager.controllers.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProjectDto {
    public boolean autoFetchFeeds;
    public int autoFetchHour;
    public int autoFetchMinute;
    public String defaultLanguage;
    public String defaultTimeZone;
    public double defaultLocationLat;
    public double defaultLocationLon;
    public double east;
    public String id;
    public String name;
    public String organizationId;
    public double osmEast;
    public double osmNorth;
    public double osmSouth;
    public double osmWest;
    public double north;
    public double south;
    public boolean useCustomOsmBounds;
    public double west;
}
