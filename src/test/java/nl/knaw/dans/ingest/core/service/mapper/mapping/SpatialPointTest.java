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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_X;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_Y;
import static nl.knaw.dans.ingest.core.service.mapper.mapping.Spatial.RD_SCHEME;
import static nl.knaw.dans.ingest.core.service.mapper.mapping.Spatial.RD_SRS_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class SpatialPointTest extends BaseTest {
    @Test
    void toEasyTsmSpatialPointValueObject_should_create_correct_spatial_point_details_in_Json_object() throws Exception {
        var doc = readDocumentFromString(
            "      <spatial>\n"
                + "        <Point>52.08113 4.34510</Point>\n"
                + "      </spatial>");

        var builder = new CompoundFieldBuilder("", false);
        SpatialPoint.toEasyTsmSpatialPointValueObject.build(builder, doc.getDocumentElement());
        var result = builder.build();

        assertThat(result.getValue())
            .extracting(SPATIAL_POINT_SCHEME)
            .extracting("value")
            .containsOnly("longitude/latitude (degrees)");

        assertThat(result.getValue())
            .extracting(SPATIAL_POINT_X)
            .extracting("value")
            .containsOnly("4.34510");

        assertThat(result.getValue())
            .extracting(SPATIAL_POINT_Y)
            .extracting("value")
            .containsOnly("52.08113");
    }

    @Test
    void toEasyTsmSpatialPointValueObject_should_give_RD_in_m_as_spatial_point_scheme_and_coordinates_as_integers() throws Exception {
        var doc = readDocumentFromString(String.format(
            "      <spatial srsName=\"%s\">\n"
                + "        <Point>469470 209942</Point>\n"
                + "      </spatial>", RD_SRS_NAME));

        var builder = new CompoundFieldBuilder("", false);
        SpatialPoint.toEasyTsmSpatialPointValueObject.build(builder, doc.getDocumentElement());
        var result = builder.build();

        assertThat(result.getValue())
            .extracting(SPATIAL_POINT_SCHEME)
            .extracting("value")
            .containsOnly(RD_SCHEME);

        assertThat(result.getValue())
            .extracting(SPATIAL_POINT_X)
            .extracting("value")
            .containsOnly("469470");

        assertThat(result.getValue())
            .extracting(SPATIAL_POINT_Y)
            .extracting("value")
            .containsOnly("209942");
    }

    @Test
    void toEasyTsmSpatialPointValueObject_should_throw_exception_when_spatial_point_coordinates_are_given_incorrectly() throws Exception {
        var doc = readDocumentFromString(String.format(
            "      <spatial srsName=\"%s\">\n"
                + "        <Point>52.08113, 4.34510</Point>\n"
                + "      </spatial>", RD_SRS_NAME));

        var builder = new CompoundFieldBuilder("", false);
        var e = assertThrows(RuntimeException.class, () ->
            SpatialPoint.toEasyTsmSpatialPointValueObject.build(builder, doc.getDocumentElement())
        );

        assertTrue(e.getMessage().contains("52.08113,"));
    }
}