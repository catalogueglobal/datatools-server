package com.conveyal.datatools.manager;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Stop;
import com.vividsolutions.jts.geom.Coordinate;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.conveyal.datatools.manager.utils.AlgarveConstants.*;

/**
 * Created by landon on 9/8/17.
 */
public class AlgarveMain {
    private static final String LIMIT = "TOP 100";
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
    public static void main(String[] args) throws SQLException, IOException, FactoryException, TransformException {


        // Step 1: Create new feed source or use pre-defined feed source ID to determine where to load new feed version


        String server = args[0];
        String username = args[1];
        String password = args[2];

        // if load args present, restore db from .bak file before doing anything else.
        if (args.length == 6) {
            String bucket = args[3];
            String bakFile = args[4];
            String dir = args[5];
            loadBakFile(bucket, bakFile, dir);
            restoreSQLServerDBFromS3(bucket, bakFile, server, username, password);
        }

        GTFSFeed gtfsFeed = new GTFSFeed();


        // STEP 3: Iterate over tables and construct GTFS entities.
        connection = connectToSQLServer(server, username, password);

//        dataSource


        loadTable(gtfsFeed, "dbo.Paragem");

        gtfsFeed.toFile("/Users/landon/Downloads/algarve.zip");

    }

    private static void loadTable(GTFSFeed gtfsFeed, String table) throws FactoryException, SQLException, TransformException {

        // FIXME: CRS may need to be defined differently. Transform has "lenient" param set to true because
        // Bursa-Wolf parameters are "missing"? http://docs.geotools.org/stable/userguide/faq.html#q-bursa-wolf-parameters-required
        CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:102165");

        // TODO: remove limit (set to 100 to prevent selecting all items)
        String fields = String.join(",", STOP_ID, SHAPE, NAME);
        String selectSQL = String.format("SELECT %s %s FROM %s", LIMIT, fields, table);
        PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
        ResultSet results = preparedStatement.executeQuery();

        MathTransform transform = CRS.findMathTransform(sourceCRS, DefaultGeographicCRS.WGS84, true);

        // iterate over stops from resultSet and from shapefile
        while (results.next()) {
            String id = results.getString(STOP_ID);
            String name = results.getString(NAME);
            String geom = results.getString(LAT_LNG);
            // geom string looks something like POINT (-41849.9930 89209.99109)
            String[] geomArray = geom.split("\\(|\\)|\\s");
            Coordinate coordinate = new Coordinate(Double.parseDouble(geomArray[3]), Double.parseDouble(geomArray[2]));
            Coordinate targetCoordinate = JTS.transform( coordinate, null, transform );
            Stop stop = new Stop();
            stop.stop_id = id;
            stop.stop_name = name;
            stop.stop_lon = targetCoordinate.x;
            stop.stop_lat = targetCoordinate.y;
            gtfsFeed.stops.put(id, stop);
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