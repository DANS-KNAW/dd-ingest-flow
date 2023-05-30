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
package nl.knaw.dans.ingest.core.service.mapper;

import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_EAST;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_NORTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SOUTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_WEST;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_COVERAGE_CONTROLLED;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_COVERAGE_UNCONTROLLED;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_X;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_Y;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TEMPORAL_COVERAGE;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getControlledMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DansTemporalSpatialMetadataTest {

    @Test
    void TS001_dct_temporal_maps_to_temporal_coverage() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dct:temporal>Het Romeinse Rijk</dct:temporal>"
            + "        <dct:temporal>De Oudheid</dct:temporal>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        assertThat(getPrimitiveMultiValueField("dansTemporalSpatial", TEMPORAL_COVERAGE, result))
            .containsOnly("Het Romeinse Rijk", "De Oudheid");
    }

    @Test
    void TS002_point_with_srs_28992_maps_to_spatial_point_scheme_rd() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial srsName='http://www.opengis.net/def/crs/EPSG/0/28992'>"
            + "            <Point xmlns='http://www.opengis.net/gml'>"
            + "                <pos>126466 529006</pos>"
            + "            </Point>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var point = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_POINT, result);
        assertThat(point).extracting(SPATIAL_POINT_X).extracting("value")
            .containsOnly("126466");
        assertThat(point).extracting(SPATIAL_POINT_Y).extracting("value")
            .containsOnly("529006");
        assertThat(point).extracting(SPATIAL_POINT_SCHEME).extracting("value")
            .containsOnly("RD (in m.)");
    }

    @Test
    void TS002_point_with_invalid_srs_maps_to_spatial_point_scheme_degrees() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial srsName='XXX'>"
            + "            <Point xmlns='http://www.opengis.net/gml'>"
            + "                <pos>126466 529006</pos>"
            + "            </Point>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var point = getCompoundMultiValueField("dansTemporalSpatial", "dansSpatialPoint", result);
        assertThat(point).isNull();
    }

    @Test
    void TS002_point_without_srs_name_maps_to_spatial_point_scheme_degrees() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <Point xmlns='http://www.opengis.net/gml'>"
            + "                <pos>126466 529006</pos>"
            + "            </Point>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var point = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_POINT, result);
        assertThat(point).extracting(SPATIAL_POINT_X).extracting("value")
            .containsOnly("529006");
        assertThat(point).extracting(SPATIAL_POINT_Y).extracting("value")
            .containsOnly("126466");
        assertThat(point).extracting(SPATIAL_POINT_SCHEME).extracting("value")
            .containsOnly("longitude/latitude (degrees)");
    }

    @Test
    void TS002_point_with_single_number_throws_an_exception() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <Point xmlns='http://www.opengis.net/gml'>"
            + "                <pos>126466</pos>"
            + "            </Point>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var thrown = assertThatThrownBy(() -> mapDdmToDataset(doc, true));
        thrown.isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    void TS002_point_without_pos_is_ignored() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <Point xmlns='http://www.opengis.net/gml'>"
            + "            </Point>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var point = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_POINT, result);
        assertThat(point).isNull();
    }

    @Test
    void TS002_spatial_without_anything_is_ignored() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi("<dcx-gml:spatial></dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var point = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_POINT, result);
        assertThat(point).isNull();
    }

    @Test
    void TS003_spatial_point_with_srs_4326_maps_to_spatial_point_scheme_degrees() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial srsName='http://www.opengis.net/def/crs/EPSG/0/4326'>"
            + "            <Point xmlns='http://www.opengis.net/gml'>"
            + "                <pos>52.078663 4.288788</pos>"
            + "            </Point>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var point = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_POINT, result);
        assertThat(point).extracting(SPATIAL_POINT_X).extracting("value")
            .containsOnly("4.288788");
        assertThat(point).extracting(SPATIAL_POINT_Y).extracting("value")
            .containsOnly("52.078663");
        assertThat(point).extracting(SPATIAL_POINT_SCHEME).extracting("value")
            .containsOnly("longitude/latitude (degrees)");
    }

    @Test
    void TS004_spatial_box_with_srs_28992_maps_to_scheme_rd() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName='http://www.opengis.net/def/crs/EPSG/0/28992'>"
            + "                    <lowerCorner>102000 335000</lowerCorner>"
            + "                    <upperCorner>140000 628000</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var box = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_BOX, result);
        assertThat(box).extracting(SPATIAL_BOX_NORTH).extracting("value")
            .containsOnly("628000");
        assertThat(box).extracting(SPATIAL_BOX_EAST).extracting("value")
            .containsOnly("140000");
        assertThat(box).extracting(SPATIAL_BOX_SOUTH).extracting("value")
            .containsOnly("335000");
        assertThat(box).extracting(SPATIAL_BOX_WEST).extracting("value")
            .containsOnly("102000");
        assertThat(box).extracting(SPATIAL_BOX_SCHEME).extracting("value")
            .containsOnly("RD (in m.)");
    }

    @Test
    void TS005_spatial_box_with_srs_4326_mapsTo_spatial_box_scheme_degrees() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName='http://www.opengis.net/def/crs/EPSG/0/4326'>"
            + "                    <lowerCorner>51.46343658020442 3.5621054065986075</lowerCorner>"
            + "                    <upperCorner>53.23074335194507 6.563118076315912</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var box = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_BOX, result);
        assertThat(box).extracting(SPATIAL_BOX_NORTH).extracting("value")
            .containsOnly("53.23074335194507");
        assertThat(box).extracting(SPATIAL_BOX_EAST).extracting("value")
            .containsOnly("6.563118076315912");
        assertThat(box).extracting(SPATIAL_BOX_SOUTH).extracting("value")
            .containsOnly("51.46343658020442");
        assertThat(box).extracting(SPATIAL_BOX_WEST).extracting("value")
            .containsOnly("3.5621054065986075");
        assertThat(box).extracting(SPATIAL_BOX_SCHEME).extracting("value")
            .containsOnly("longitude/latitude (degrees)");
    }

    @Test
    void TS005_spatial_box_with_invalid_srs_name_maps_to_scheme_degrees() throws Exception {
        // TODO https://drivenbydata.atlassian.net/browse/DD-1305
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName='XXX'>"
            + "                    <lowerCorner>1 2</lowerCorner>"
            + "                    <upperCorner>3 4</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var box = getCompoundMultiValueField("dansTemporalSpatial", "dansSpatialBox", result);
        assertThat(box).isNull();
    }

    @Test
    void TS005_spatial_box_without_srs_name_maps_to_scheme_degrees() throws Exception {
        // TODO not explicit in https://drivenbydata.atlassian.net/browse/DD-1305
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName='http://www.opengis.net/def/crs/EPSG/0/4326'>"
            + "                    <lowerCorner>1 2</lowerCorner>"
            + "                    <upperCorner>3 4</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var box = getCompoundMultiValueField("dansTemporalSpatial", SPATIAL_BOX, result);
        assertThat(box).extracting(SPATIAL_BOX_NORTH).extracting("value")
            .containsOnly("3");
        assertThat(box).extracting(SPATIAL_BOX_EAST).extracting("value")
            .containsOnly("4");
        assertThat(box).extracting(SPATIAL_BOX_SOUTH).extracting("value")
            .containsOnly("1");
        assertThat(box).extracting(SPATIAL_BOX_WEST).extracting("value")
            .containsOnly("2");
        assertThat(box).extracting(SPATIAL_BOX_SCHEME).extracting("value")
            .containsOnly("longitude/latitude (degrees)");
    }

    @Test
    void TS005_spatial_box_with_single_number_in_upper_corner_throws_an_exception() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\">"
            + "                    <lowerCorner>1 2</lowerCorner>"
            + "                    <upperCorner>3</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");

        var thrown = assertThatThrownBy(() -> mapDdmToDataset(doc, true));
        thrown.isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    void TS005_spatial_box_with_single_number_in_lower_corner_throws_an_exception() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\">"
            + "                    <lowerCorner>2</lowerCorner>"
            + "                    <upperCorner>3 4</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var thrown = assertThatThrownBy(() -> mapDdmToDataset(doc, true));
        thrown.isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    void TS005_spatial_box_without_lower_corner_throws_an_exception() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\">"
            + "                    <upperCorner>3 4</upperCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var thrown = assertThatThrownBy(() -> mapDdmToDataset(doc, true));
        thrown.isInstanceOf(IllegalArgumentException.class);
        thrown.hasMessage("Missing gml:lowerCorner node in gml:Envelope");
    }

    @Test
    void TS005_spatial_box_without_upper_corner_throws_an_exception() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "                <Envelope srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\">"
            + "                    <lowerCorner>1 2</lowerCorner>"
            + "                </Envelope>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");
        var thrown = assertThatThrownBy(() -> mapDdmToDataset(doc, true));
        thrown.isInstanceOf(IllegalArgumentException.class);
        thrown.hasMessage("Missing gml:upperCorner node in gml:Envelope");
    }

    @Test
    void TS005_spatial_box_without_envelope_throws_an_exception() throws Exception {
        // TODO ignoring would be consistent with POINT without POS
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dcx-gml:spatial>"
            + "            <boundedBy xmlns='http://www.opengis.net/gml'>"
            + "            </boundedBy>"
            + "        </dcx-gml:spatial>")
            + "</ddm:DDM>");


        var thrown = assertThrows(IllegalArgumentException.class, () -> mapDdmToDataset(doc, true));
        assertEquals("Missing gml:Envelope node", thrown.getMessage());
    }

    @Test
    void TS006_dct_spatial_maps_to_spatial_coverage_controlled() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dct:spatial>South Africa</dct:spatial>"
            + "        <dct:spatial>Japan</dct:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var box = getControlledMultiValueField("dansTemporalSpatial", SPATIAL_COVERAGE_CONTROLLED, result);
        assertThat(box).containsOnly("South Africa", "Japan");
    }

    @Test
    void TS007_dct_spatial_with_surrounding_white_space_maps_to_trimmed_spatial_coverage() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-gml='http://easy.dans.knaw.nl/schemas/dcx/gml/'>"
            + minimalDdmProfile() + dcmi(""
            + "        <dct:spatial>"
            + "             Roman Empire"
            + "        </dct:spatial>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        assertThat(getPrimitiveMultiValueField("dansTemporalSpatial", SPATIAL_COVERAGE_UNCONTROLLED, result))
            .containsOnly("Roman Empire");
    }
}
