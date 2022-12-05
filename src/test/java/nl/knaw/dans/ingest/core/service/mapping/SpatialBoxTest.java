/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    void toEasyTsmSpatialBoxValueObject_should_create_correct_spatial_box_details_in_Json_object() throws Exception {
        var doc = readDocumentFromString(
            "        <gml:boundedBy xmlns:gml=\"http://www.opengis.net/gml\">\n"
                + "            <gml:Envelope>\n"
                + "                <gml:lowerCorner>45.555 -100.666</gml:lowerCorner>\n"
                + "                <gml:upperCorner>90.0 179.999</gml:upperCorner>\n"
                + "            </gml:Envelope>\n"
                + "        </gml:boundedBy>");

        var builder = new CompoundFieldBuilder(SPATIAL_BOX, true);
        SpatialBox.toEasyTsmSpatialBoxValueObject.build(builder, doc.getDocumentElement());
        var value = builder.build().getValue();

        assertThat(value)
            .extracting(x -> x.get(SPATIAL_BOX_SCHEME))
            .extracting("value")
            .containsOnly("longitude/latitude (degrees)");

        assertThat(value)
            .extracting(x -> x.get(SPATIAL_BOX_NORTH))
            .extracting("value")
            .containsOnly("90.0");

        assertThat(value)
            .extracting(x -> x.get(SPATIAL_BOX_EAST))
            .extracting("value")
            .containsOnly("179.999");

        assertThat(value)
            .extracting(x -> x.get(SPATIAL_BOX_SOUTH))
            .extracting("value")
            .containsOnly("45.555");

        assertThat(value)
            .extracting(x -> x.get(SPATIAL_BOX_WEST))
            .extracting("value")
            .containsOnly("-100.666");
    }

    @Test
    void toEasyTsmSpatialBoxValueObject_should_give_RD_in_m_as_spatial_box_scheme() throws Exception {
        var doc = readDocumentFromString(
            "        <gml:boundedBy xmlns:gml=\"http://www.opengis.net/gml\">\n"
                + "            <gml:Envelope srsName=\"http://www.opengis.net/def/crs/EPSG/0/28992\">\n"
                + "                <gml:lowerCorner>469470 209942</gml:lowerCorner>\n"
                + "                <gml:upperCorner>469890 209914</gml:upperCorner>\n"
                + "            </gml:Envelope>\n"
                + "        </gml:boundedBy>");

        var builder = new CompoundFieldBuilder(SPATIAL_BOX, true);
        SpatialBox.toEasyTsmSpatialBoxValueObject.build(builder, doc.getDocumentElement());

        assertThat(builder.build().getValue())
            .extracting(x -> x.get(SPATIAL_BOX_SCHEME))
            .extracting("value")
            .containsOnly("RD (in m.)");
    }

    @Test
    void toEasyTsmSpatialBoxValueObject_should_throw_exception_when_longitude_latitude_pair_is_given_incorrectly() throws Exception {
        var doc = readDocumentFromString(
            "        <gml:boundedBy xmlns:gml=\"http://www.opengis.net/gml\">\n"
                + "            <gml:Envelope>\n"
                + "                <gml:lowerCorner>469470, 209942</gml:lowerCorner>\n"
                + "                <gml:upperCorner>469890, 209914</gml:upperCorner>\n"
                + "            </gml:Envelope>\n"
                + "        </gml:boundedBy>");

        var e = assertThrows(NumberFormatException.class,
            () -> SpatialBox.toEasyTsmSpatialBoxValueObject.build(null, doc.getDocumentElement()));
        assertTrue(e.getMessage().contains("469470,"));
    }
}