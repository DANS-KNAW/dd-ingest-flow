package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_EAST;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_NORTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SOUTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_WEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpatialBoxTest extends BaseTest {

    @Test
    void testToEasyTsmSpatialBoxValueObject() throws Exception {
        var doc = readDocument("spatial.xml");
        var node = xmlReader.xpathToStream(doc, "//gml:boundedBy[1]")
            .findFirst().orElseThrow();

        var builder = new CompoundFieldBuilder(SPATIAL_BOX, true);
        SpatialBox.toEasyTsmSpatialBoxValueObject.build(builder, node);

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_SCHEME))
            .extracting("value")
            .containsOnly("longitude/latitude (degrees)");

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_NORTH))
            .extracting("value")
            .containsOnly("90.0");

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_EAST))
            .extracting("value")
            .containsOnly("179.999");

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_SOUTH))
            .extracting("value")
            .containsOnly("45.555");

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_WEST))
            .extracting("value")
            .containsOnly("-100.666");
    }

    @Test
    void testToEasyTsmSpatialBoxValueObjectWithRd() throws Exception {
        var doc = readDocument("spatial.xml");
        var node = xmlReader.xpathToStream(doc, "//gml:boundedBy[2]")
            .findFirst().orElseThrow();

        var builder = new CompoundFieldBuilder(SPATIAL_BOX, true);
        SpatialBox.toEasyTsmSpatialBoxValueObject.build(builder, node);

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_SCHEME))
            .extracting("value")
            .containsOnly("RD (in m.)");
    }

    @Test
    void testToEasyTsmSpatialBoxValueObjectWithInvalidPairs() throws Exception {
        var doc = readDocument("spatial.xml");
        var node = xmlReader.xpathToStream(doc, "//gml:boundedBy[3]")
            .findFirst().orElseThrow();

        var e = assertThrows(NumberFormatException.class, () -> SpatialBox.toEasyTsmSpatialBoxValueObject.build(null, node));
        assertTrue(e.getMessage().contains("469470,"));
    }

}