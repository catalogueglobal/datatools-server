package com.conveyal.datatools.manager.utils;

import com.conveyal.gtfs.loader.EntityPopulator;
import com.conveyal.gtfs.loader.StringField;
import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.loader.URLField;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Stop;
import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import static com.conveyal.gtfs.loader.EntityPopulator.getDoubleIfPresent;
import static com.conveyal.gtfs.loader.EntityPopulator.getIntIfPresent;
import static com.conveyal.gtfs.loader.EntityPopulator.getStringIfPresent;
import static com.conveyal.gtfs.loader.Requirement.OPTIONAL;
import static com.conveyal.gtfs.loader.Requirement.REQUIRED;

/**
 * Created by landon on 9/25/17.
 */
public final class AlgarveConstants {
    private AlgarveConstants() {
        // restrict instantiation
    }

    public static final String CALENDARS_TABLE = "dbo.PeriodosAnuais";
    public static final String CALENDAR_ID = "IdPeriodo"; // FIXME: not actually service_id
    public static final String START_DATE = "DataInicio";
    public static final String END_DATE = "DataFim";

    public static final String ROUTES_TABLE = "dbo.Carreiras";
    public static final String ROUTE_SHORT_NAME = "CodCarOperador";
    public static final String ROUTE_ID = "IdCarreira";

    public static final String NAME = "Designacao";
    public static final String STOPS_TABLE = "dbo.Paragem";
    public static final String STOP_ID = "IdParagem";

    public static final String LAT_LNG = "lat_lng";
    public static final String SHAPE = "shape.STAsText() as " + LAT_LNG;

    public static final String TRIPS_TABLE = "CarreiraCirculacaoFrequencia";
    public static final String SERVICE_ID = "IdPerFreq";
    public static final String TRIP_ID = "IdCarCircPerFreqSen";

    public static final String ARRIVAL_TIME = "OrdemPrevious";
    public static final String STOP_SEQUENCE = "IdCarSen";
}
