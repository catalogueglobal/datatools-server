package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.manager.extensions.ExternalFeed;
import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by demory on 3/30/16.
 */

public class RtdCarrier implements ExternalFeed {

    @JsonProperty
    public String AgencyId;

    @JsonProperty
    public String AgencyName;

    @JsonProperty
    public String AgencyPhone;

    @JsonProperty
    public String RttAgencyName;

    @JsonProperty
    public String RttEnabled;

    @JsonProperty
    public String AgencyShortName;

    @JsonProperty
    public String AgencyPublicId;

    @JsonProperty
    public String AddressLat;

    @JsonProperty
    public String AddressLon;

    @JsonProperty
    public String DefaultRouteType;

    @JsonProperty
    public String CarrierStatus;

    @JsonProperty
    public String AgencyAddress;

    @JsonProperty
    public String AgencyEmail;

    @JsonProperty
    public String AgencyUrl;

    @JsonProperty
    public String AgencyFareUrl;

    @JsonProperty
    public String EditedBy;

    @JsonProperty
    public String EditedDate;

    public RtdCarrier() {
    }

    @Override
    public String getId() {
        return AgencyId;
    }

    @Override
    public String getIdKey() {
        return "AgencyId";
    }

    @Override
    public String getFeedUrl() {
        return null;
    }

    @Override
    public String getName() {
        if (this.AgencyName != null) {
            return this.AgencyName;
        } else if (this.AgencyShortName != null) {
            return this.AgencyShortName;
        } else {
            return this.AgencyId;
        }
    }

}