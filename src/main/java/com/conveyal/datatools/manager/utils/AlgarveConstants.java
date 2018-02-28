package com.conveyal.datatools.manager.utils;

/**
 * Holds constants for table and column names for Algarve feed imports.
 */
public final class AlgarveConstants {
    private AlgarveConstants() {
        // restrict instantiation
    }

    public static final String CALENDARS_TABLE = "dbo.PeriodosAnuais";
    public static final String CALENDAR_ID = "IdPeriodo"; // FIXME: not actually service_id
    public static final String FREQUENCY_ID = "IdFrequencia"; // FIXME: not actually service_id
    public static final String START_DATE = "DataInicio";
    public static final String END_DATE = "DataFim";

    public static final String ROUTES_TABLE = "dbo.Carreiras";
    public static final String FREQUENCIES_TABLE = "dbo.Frequencias";
    public static final String ROUTE_SHORT_NAME = "CodCarOperador";
    public static final String ROUTE_ID = "IdCarreira";

    public static final String NAME = "Designacao";
    public static final String STOPS_TABLE = "dbo.Paragem";
    public static final String STOP_ID = "IdParagem";

    public static final String LAT_LNG = "lat_lng";
    public static final String SHAPE = "shape.STAsText() as " + LAT_LNG;

    public static final String TRIPS_TABLE = "CarreiraCirculacaoFrequencia";
    public static final String SERVICE_ID = "IdPerFreq";
    public static final String CIRCULATION_ID = "IdCirculacao";
    public static final String TRIP_ID = "IdCarCircPerFreqSen";

    public static final String ARRIVAL_TIME = "OrdemPrevious";
    public static final String STOP_SEQUENCE = "IdCarSen";
}
