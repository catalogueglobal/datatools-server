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

    public static final String NAME = "Designacao";
    public static final String STOP_ID = "IdParagem";
    public static final String LAT_LNG = "lat_lng";
    public static final String SHAPE = "shape.STAsText() as " + LAT_LNG;

    // FIXME: use Table objects to build schema for Algarve
//    public static final STOPS = new Table();

    public static final EntityPopulator<Stop> STOP = (result, columnForName) -> {
        // FIXME: CRS may need to be defined differently. Transform has "lenient" param set to true because
        // Bursa-Wolf parameters are "missing"? http://docs.geotools.org/stable/userguide/faq.html#q-bursa-wolf-parameters-required
        CoordinateReferenceSystem sourceCRS = null;
        try {
            sourceCRS = CRS.decode("EPSG:102165");
        } catch (FactoryException e) {
            e.printStackTrace();
        }
        Stop stop = new Stop();

        String geom = result.getString(LAT_LNG);
        // geom string looks something like POINT (-41849.9930 89209.99109)
        String[] geomArray = geom.split("\\(|\\)|\\s");
        Coordinate coordinate = new Coordinate(Double.parseDouble(geomArray[3]), Double.parseDouble(geomArray[2]));
        MathTransform transform = null;
        Coordinate targetCoordinate = null;
        try {
            transform = CRS.findMathTransform(sourceCRS, DefaultGeographicCRS.WGS84, true);
            targetCoordinate = JTS.transform( coordinate, null, transform );
        } catch (FactoryException e) {
            e.printStackTrace();
        } catch (TransformException e) {
            e.printStackTrace();
        }

        stop.stop_id        = getStringIfPresent(result, "IdParagem", columnForName);
//        stop.stop_code      = getStringIfPresent(result, "stop_code", columnForName);
        stop.stop_name      = getStringIfPresent(result, "Designacao", columnForName);
        stop.stop_desc      = getStringIfPresent(result, "stop_desc", columnForName);
        stop.stop_lon = targetCoordinate.x;
        stop.stop_lat = targetCoordinate.y;
//        stop.zone_id        = getStringIfPresent(result, "zone_id", columnForName);
//        stop.parent_station = getStringIfPresent(result, "parent_station", columnForName);
//        stop.stop_timezone  = getStringIfPresent(result, "stop_timezone", columnForName);
//        stop.stop_url = null; //new URL(getStringIfPresent(result, "stop_url",  columnForName));
//        stop.location_type = getIntIfPresent(result, "location_type", columnForName);
//        stop.wheelchair_boarding = Integer.toString(getIntIfPresent(result, "wheelchair_boarding", columnForName));
        return stop;
    };
}
