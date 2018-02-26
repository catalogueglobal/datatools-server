package com.conveyal.datatools.manager;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Agency;
import com.conveyal.gtfs.model.Calendar;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Route;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Stop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.model.Trip;
import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.mapdb.Fun;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.manager.utils.AlgarveConstants.*;

/**
 * Created by landon on 9/8/17.
 */
public class AlgarveMain {
    private static final String LIMIT = "TOP 100";
    public static final Logger LOG = LoggerFactory.getLogger(AlgarveMain.class);
    private static Connection connection;
    private static AmazonS3 s3;
    private static DataSource dataSource;

    /**
     * Arguments can be supplied to either connect to an existing restored database (3 args) OR load/restore db from a
     * .bak file and then connect to that database.
     *
     * Examples:
     * sql-server-domain-name.rds.amazonaws.com DB_USERNAME DB_PASSWORD [S3_BUCKET backup_filename.bak /path/to/backup/file/]
     *
     */
    public static void main(String[] args) throws SQLException, FactoryException, TransformException {


        // Step 1: Create new feed source or use pre-defined feed source ID to determine where to load new feed version
        String server = args[0];
        String username = args[1];
        String password = args[2];


        boolean loadIntoDb = args.length == 6;
        // if load args present, restore db from .bak file before doing anything else.
        if (loadIntoDb) {
            String bucket = args[3];
            String bakFile = args[4];
            String dir = args[5];
            loadBakFile(bucket, bakFile, dir);
            restoreSQLServerDBFromS3(bucket, bakFile, server, username, password);
        }

        GTFSFeed gtfsFeed = new GTFSFeed();


        // STEP 3: Iterate over tables and construct GTFS entities.
        connection = connectToSQLServer(server, username, password);

        // If loading into SQL server for the first time, add supplementary fields.
        if (loadIntoDb) addSupplementaryFields(server, username, password);

        loadTables(gtfsFeed);
        String filePath = "/Users/landon/Downloads/algarve.zip";
        LOG.info("Writing feed to {}", filePath);
        gtfsFeed.toFile(filePath);

    }

    private static void addSupplementaryFields(String server, String username, String password) throws SQLException {
        // TABLE PREPARATION FIXME: substitute table/column names
        LOG.info("Adding supplementary columns...");
        // Create new columns in two tables
        executeUpdate(String.format("ALTER TABLE %s ADD %s VARCHAR(50)", CALENDARS_TABLE, SERVICE_ID));
        executeUpdate(String.format("ALTER TABLE %s ADD %s VARCHAR(50)", TRIPS_TABLE, SERVICE_ID));
        executeUpdate(String.format("ALTER TABLE %s ADD %s VARCHAR(100)", TRIPS_TABLE, TRIP_ID));
        executeUpdate(String.format("ALTER TABLE %s ADD %s VARCHAR(50)", TRIPS_TABLE, STOP_SEQUENCE));

        // Set trip_id field value
        executeUpdate(String.format("UPDATE %s SET %s = %s * 1000 + %s", CALENDARS_TABLE, SERVICE_ID, CALENDAR_ID, FREQUENCY_ID));

        // Update CarreiraCirculacaoFrequencia with new value based on common frequency id
        executeUpdate(String.format("UPDATE ccf SET ccf.%s = pa.%s FROM %s ccf INNER JOIN %s pa ON ccf.%s = pa.%s", SERVICE_ID, SERVICE_ID, TRIPS_TABLE, CALENDARS_TABLE, FREQUENCY_ID, FREQUENCY_ID));

        // Generate "trip id"
        executeUpdate(String.format("UPDATE %s SET %s = CONCAT(%s, %s, %s, SentidoCirculacao)", TRIPS_TABLE, TRIP_ID, ROUTE_ID, SERVICE_ID, CIRCULATION_ID));
        executeUpdate(String.format("UPDATE %s SET IdCarSen = IdCarreira x 100 + SentidoCirculacao / Sentidotroco", TRIPS_TABLE));
    }

    private static void loadTables(GTFSFeed gtfsFeed) throws FactoryException, SQLException, TransformException {

        // FIXME: CRS may need to be defined differently. Transform has "lenient" param set to true because
        // Bursa-Wolf parameters are "missing"? http://docs.geotools.org/stable/userguide/faq.html#q-bursa-wolf-parameters-required
        CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:102165");
        MathTransform lisboaToWGS84 = CRS.findMathTransform(sourceCRS, DefaultGeographicCRS.WGS84, true);

        // LOAD STOPS
        // TODO: remove limit (set to 100 to prevent selecting all items)
        String stopFields = String.join(",", STOP_ID, SHAPE, NAME);
        String selectStops = String.format("SELECT %s %s FROM %s", LIMIT, stopFields, STOPS_TABLE);
        PreparedStatement preparedStatementStops = connection.prepareStatement(selectStops);
        ResultSet stopResults = preparedStatementStops.executeQuery();

        // iterate over stops from resultSet
        while (stopResults.next()) {
            String id = stopResults.getString(STOP_ID);
            String name = stopResults.getString(NAME);
            String geom = stopResults.getString(LAT_LNG);
            // geom string looks something like POINT (-41849.9930 89209.99109)
            // TODO: split on any number of occurrences (of whitespace/parens)
            String[] geomArray = geom.split("\\(|\\)|\\s");
            Coordinate coordinate = new Coordinate(Double.parseDouble(geomArray[3]), Double.parseDouble(geomArray[2]));
            Coordinate targetCoordinate = JTS.transform( coordinate, null, lisboaToWGS84 );
            Stop stop = new Stop();
            stop.stop_id = id;
            stop.stop_name = name;
            stop.stop_lon = targetCoordinate.x;
            stop.stop_lat = targetCoordinate.y;
            LOG.info("Importing stop id: {}, name: {}, lat_lng: {}, {}", id, name, stop.stop_lat, stop.stop_lon);
            gtfsFeed.stops.put(id, stop);
        }

        // LOAD AGENCY (there is only one and it is defined by constants rather than SQL results)
        Agency agency = new Agency();
        final String agencyId = "AMAL";
        agency.agency_id = agencyId;
        agency.agency_name = "Comunidade Intermunicipal do Algarve";
        agency.agency_timezone = "Europe/Lisbon";
        try {
            agency.agency_url = new URL("http://www.amal.pt"); // constant
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        LOG.info("Importing agency id: {}, name: {}", agencyId, agency.agency_name);
        gtfsFeed.agency.put(agencyId, agency);


        // LOAD ROUTES
        // TODO: remove limit (set to 100 to prevent selecting all items)
        String routeFields = String.join(",", ROUTE_ID, ROUTE_SHORT_NAME, NAME);
        String routeSelect = String.format("SELECT %s %s FROM %s", LIMIT, routeFields, ROUTES_TABLE);
        PreparedStatement routePreparedStatement = connection.prepareStatement(routeSelect);
        ResultSet routeResults = routePreparedStatement.executeQuery();

        // iterate over routes from resultSet
        while (routeResults.next()) {
            Route route = new Route();
            String id = routeResults.getString(ROUTE_ID);
            route.agency_id = agencyId; // From constant field above
            route.route_id = id;
            route.route_short_name = routeResults.getString(ROUTE_SHORT_NAME);
            route.route_long_name = routeResults.getString(NAME);
            LOG.info("Importing route id: {}, name: {}", id, route.route_short_name);
            gtfsFeed.routes.put(id, route);
        }

        // LOAD TRIPS
        // TODO: remove limit (set to 100 to prevent selecting all items)
        String tripFields = String.join(",", ROUTE_ID, TRIP_ID, SERVICE_ID);
        String tripSelect = String.format("SELECT %s %s FROM %s", LIMIT, tripFields, TRIPS_TABLE);
        PreparedStatement tripPreparedStatement = connection.prepareStatement(tripSelect);
        ResultSet tripResults = tripPreparedStatement.executeQuery();

        // iterate over trips from resultSet
        while (tripResults.next()) {
            Trip trip = new Trip();
            String id = tripResults.getString(TRIP_ID);
            trip.trip_id = id;
            trip.route_id = tripResults.getString(ROUTE_ID);
            trip.service_id = tripResults.getString(SERVICE_ID);
            LOG.info("Importing trip id: {}, route: {}", id, trip.route_id);
            gtfsFeed.trips.put(id, trip);
        }

        // LOAD CALENDARS
        // TODO: remove limit (set to 100 to prevent selecting all items)
        String calendarFields = String.join(",", SERVICE_ID, START_DATE, END_DATE);
        // TODO: make this a join select on calendars and frequencies
        String calendarSelect = String.format("SELECT %s %s FROM %s", LIMIT, calendarFields, CALENDARS_TABLE);
        PreparedStatement calendarPreparedStatement = connection.prepareStatement(calendarSelect);
        ResultSet calendarResults = calendarPreparedStatement.executeQuery();

        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        // iterate over calendars from resultSet
        while (calendarResults.next()) {
            Calendar calendar = new Calendar();
            String id = calendarResults.getString(SERVICE_ID);
            Service service = gtfsFeed.services.computeIfAbsent(id, Service::new);
            calendar.service_id = id;
            // TODO: parse DOW values
            calendar.start_date = Integer.parseInt(dateFormat.format(calendarResults.getDate(START_DATE)));
            calendar.end_date = Integer.parseInt(dateFormat.format(calendarResults.getDate(END_DATE)));
            LOG.info("Importing calendar id: {}, from: {}, to: {}", id, calendar.start_date, calendar.end_date);

            gtfsFeed.services.put(id, service);
        }

        // LOAD CALENDAR_DATES
        // TODO: remove limit (set to 100 to prevent selecting all items)
        String calendarDateFields = String.join(",", SERVICE_ID, START_DATE, END_DATE);
        // TODO: make this a join select on calendars and frequencies
        String calendarDateSelect = String.format("SELECT %s %s FROM %s", LIMIT, calendarDateFields, CALENDARS_TABLE);
        PreparedStatement calendarDatePreparedStatement = connection.prepareStatement(calendarDateSelect);
        ResultSet calendarDateResults = calendarDatePreparedStatement.executeQuery();

        // iterate over calendar dates from resultSet
        while (calendarDateResults.next()) {
            CalendarDate calendarDate = new CalendarDate();
            String id = calendarDateResults.getString(SERVICE_ID);
            Service service = gtfsFeed.services.computeIfAbsent(id, Service::new);
            calendarDate.service_id = id;
            // TODO: date field maintained in separate Holidays table?
//            calendarDate.date = ????
            //TODO: New field in Frequencias table H populated by the result of the following operation
            // IF N2 minus F2 = 1 THEN EQUALS 2, but IF N2 minus F2 = -1 THEN EQUALS 1
//            calendarDate.exception_type =
            LOG.info("Importing calendarDate id: {}, date: {}, type: {}", id, calendarDate.date, calendarDate.exception_type);

            gtfsFeed.services.put(id, service);
        }


        // LOAD STOP TIMES
        // TODO: remove limit (set to 100 to prevent selecting all items)
        String stopTimeFields = String.join(",", TRIP_ID, ARRIVAL_TIME, STOP_SEQUENCE);
        // TODO: make this a join select on calendars and RTrocoCarreira
        String stopTimeSelect = String.format("SELECT %s %s FROM %s", LIMIT, stopTimeFields, CALENDARS_TABLE);
        PreparedStatement stopTimePreparedStatement = connection.prepareStatement(stopTimeSelect);
        ResultSet stopTimeResults = stopTimePreparedStatement.executeQuery();

        // iterate over stopTimes from resultSet
        while (stopTimeResults.next()) {
            StopTime stopTime = new StopTime();
            stopTime.trip_id = stopTimeResults.getString(TRIP_ID);
            stopTime.arrival_time = stopTimeResults.getInt(ARRIVAL_TIME);
            stopTime.departure_time = stopTimeResults.getInt(ARRIVAL_TIME); // Same as arrival
            stopTime.stop_sequence = stopTimeResults.getInt(STOP_SEQUENCE);
            LOG.info("Importing stopTime trip_id: {}, sequence: {}", stopTime.trip_id, stopTime.stop_sequence);

            gtfsFeed.stop_times.put(new Fun.Tuple2(stopTime.trip_id, stopTime.stop_sequence), stopTime);
        }
    }

    private static void loadBakFile(String bucket, String bakFile, String dir) {
        // load .bak file to s3
        s3 = AmazonS3ClientBuilder.defaultClient();
        s3.putObject(bucket, bakFile, new File(dir + bakFile));
    }

    private static FeatureCollection<SimpleFeatureType, SimpleFeature> loadShapefile(String s) throws IOException {
        File file = new File(s);
        Map<String, Object> map = new HashMap<>();
        map.put("url", file.toURI().toURL());

        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];

        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE; // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")

        return source.getFeatures(filter);
    }

    private static void restoreSQLServerDBFromS3(String bucket, String bakFile, String server, String username, String password) throws SQLException {
        Connection connection = connectToSQLServer(server, username, password);

        // load database from .bak file
        String SPsql = "EXEC msdb.dbo.rds_restore_database ?,?";   // for stored proc taking 2 parameters
        PreparedStatement ps = connection.prepareStatement(SPsql);
        ps.setEscapeProcessing(true);
        ps.setQueryTimeout(20);
        ps.setString(1, "@restore_db_name='import_test'");
        ps.setString(2, String.format("@s3_arn_to_restore_from='arn:aws:s3:::%s/%s’", bucket, bakFile));
        ResultSet rs = ps.executeQuery();
        connection.close();
    }

    private static Connection connectToSQLServer(String server, String username, String password) throws SQLException {
        // connect to SQL Server
        dataSource = GTFS.createDataSource(
                String.format("jdbc:sqlserver://%s:1433;DatabaseName=import_test", server),
                username,
                password
        );
        connection = dataSource.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    private static DataStore connectToGeoDataStore(String server, String username, String password) throws IOException {


//        DataStore dataStore = connectToGeoDataStore(server, username, password);
//        Arrays.stream(dataStore.getTypeNames()).forEach(type -> System.out.println(type));
//        dataStore.getFeatureReader(new Query("dbo.Paragem"), )

        java.util.Map params = new java.util.HashMap();
        params.put( "dbtype", "sqlserver");
        params.put( "host", server);
        params.put( "port", 1433);
        params.put( "user", username);
        params.put( "passwd", password);

        DataStore dataStore = DataStoreFinder.getDataStore(params);
        return dataStore;
    }

    // TODO: Remove unused shapefile code? Spatial data already appears to be available in database.
//        FeatureCollection<SimpleFeatureType, SimpleFeature> stopCollection = loadShapefile("/Users/landon/Downloads/algarve_shp/paragems.shp");
//        FeatureCollection<SimpleFeatureType, SimpleFeature> streetCollection = loadShapefile("/Users/landon/Downloads/algarve_shp/trocos.shp");
//        try (FeatureIterator<SimpleFeature> stops = stopCollection.features()) {
//            while (stops.hasNext()) {
//                SimpleFeature feature = stops.next();
//                String stopId = ((Long) feature.getAttribute(STOP_KEY)).toString();
//                if (stopMap.containsKey(stopId)) {
//                    Stop stop = stopMap.get(stopId);
//                    Point stopPoint = (Point) feature.getDefaultGeometryProperty().getValue();
//                    stop.stop_lat = stopPoint.getY();
//                    stop.stop_lon = stopPoint.getX();
//                    System.out.print(stopId);
//                    System.out.print(": ");
//                    System.out.println(stop.stop_lon);
//                }
//
////                System.out.print(stopId);
////                System.out.print(": ");
////                System.out.println(feature.getDefaultGeometryProperty().getValue().getClass());
//            }
//        }

}