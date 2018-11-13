package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.manager.extensions.ExternalFeed;

import static com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource.AGENCY_ID;

/**
 * Created by demory on 3/30/16.
 */

public class RtdCarrier implements ExternalFeed {
    public String AgencyId;
    public String AgencyName;
    public String AgencyPhone;
    public String RttAgencyName;
    public String RttEnabled;
    public String AgencyShortName;
    public String AgencyPublicId;
    public String AddressLat;
    public String AddressLon;
    public String DefaultRouteType;
    public String CarrierStatus;
    public String AgencyAddress;
    public String AgencyEmail;
    public String AgencyUrl;
    public String AgencyFareUrl;
    public String EditedBy;
    public String EditedDate;

    public RtdCarrier() { }

    @Override
    public String getId() {
        return AgencyId;
    }

    @Override
    public String getIdKey() {
        return AGENCY_ID;
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