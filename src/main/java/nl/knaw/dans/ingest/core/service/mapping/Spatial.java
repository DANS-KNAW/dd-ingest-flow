package nl.knaw.dans.ingest.core.service.mapping;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.w3c.dom.Node;

public class Spatial extends Base {

    /**
     * coordinate order y, x = latitude (DCX_SPATIAL_Y), longitude (DCX_SPATIAL_X)
     */
    final static String DEGREES_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/4326";

    /**
     * coordinate order x, y = longitude (DCX_SPATIAL_X), latitude (DCX_SPATIAL_Y)
     */
    final static String RD_SRS_NAME = "http://www.opengis.net/def/crs/EPSG/0/28992";

    final static String RD_SCHEME = "RD (in m.)";
    final static String LONLAT_SCHEME = "longitude/latitude (degrees)";

    static boolean isRd(Node node) {
        return getAttribute(node, "srsName")
            .map(item -> RD_SRS_NAME.equals(item.getTextContent()))
            .orElse(false);
    }

    static Point getPoint(Node node, boolean isRd) {
        var points = node.getTextContent().trim().split("\\s+");

        if (points.length >= 2) {
            // make sure that you have valid numbers here
            Double.parseDouble(points[0]);
            Double.parseDouble(points[1]);
        }

        if (isRd) {
            return new Point(points[0], points[1]);
        }
        else {
            /*
             * https://wiki.esipfed.org/CRS_Specification
             * urn:ogc:def:crs:EPSG::4326 has coordinate order latitude(north), longitude(east) = y x
             * we make this the default order
             */
            return new Point(points[1], points[0]);
        }
    }

    @Data
    @AllArgsConstructor
    static class Point {
        private String x;
        private String y;

    }
}
