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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_ARTIFACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_COMPLEX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_PERIOD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_RAPPORT_NUMMER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_RAPPORT_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_VERWERVINGSWIJZE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_ZAAK_ID;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.toPrettyJsonString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DansArchaeologyMetadataTest {

    @Test
    void AR001_dct_identifier_archis_zaak_id_maps_to_archis_zaak_id() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + dcmi("<dct:identifier xsi:type='id-type:ARCHIS-ZAAK-IDENTIFICATIE'>123</dct:identifier>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ARCHIS_ZAAK_ID, result))
            .containsOnly("123");
    }

    @Test
    void AR002_dct_identifier_archis_number_maps_to_archis_number() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + dcmi("<dct:identifier xsi:type='id-type:ARCHIS-MONUMENT'>456</dct:identifier>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        var archisNr = getCompoundMultiValueField("dansArchaeologyMetadata", ARCHIS_NUMBER, result);
        assertThat(archisNr).extracting(ARCHIS_NUMBER_TYPE).extracting("value")
            .containsOnly("monument");
        assertThat(archisNr).extracting(ARCHIS_NUMBER_ID).extracting("value")
            .containsOnly("456");
    }

    @Test
    void AR003_AR004_ddm_reportNumber_maps_to_abr_report_type_and_number() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:reportNumber"
            + "                subjectScheme='ABR Rapporten'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/d6b2e162-3f49-4027-8f03-28194db2905e'"
            + "                reportNo='123-A'>"
            + "            BAAC 123-A"
            + "        </ddm:reportNumber>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        // AR003
        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_RAPPORT_TYPE, result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/d6b2e162-3f49-4027-8f03-28194db2905e");

        // AR004
        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_RAPPORT_NUMMER, result))
            .containsOnly("BAAC 123-A");
    }

    @Test
    void AR003_AR004_ddm_report_number_without_type_maps_to_abr_number() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:reportNumber"
            + "                subjectScheme='ABR Rapporten'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e'"
            + "                reportNo='123-A'>"
            + "            BAAC 123-A"
            + "        </ddm:reportNumber>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        // AR003
        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_RAPPORT_TYPE, result))
            .isNull();

        // AR004
        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_RAPPORT_NUMMER, result))
            .containsOnly("BAAC 123-A");
    }

    @Test
    void AR005_ddm_acquisitionMethod_maps_to_abr_verwervingswijze() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:acquisitionMethod"
            + "                subjectScheme='ABR verwervingswijzen'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/554ca1ec-3ed8-42d3-ae4b-47bcb848b238'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/967bfdf8-c44d-4c69-8318-34ed1ab1e784'>"
            + "            archeologisch: onderwaterarcheologie"
            + "        </ddm:acquisitionMethod>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_VERWERVINGSWIJZE, result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/967bfdf8-c44d-4c69-8318-34ed1ab1e784");
    }

    @Test
    void AR006_ddm_subject_maps_to_abr_complex() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:subject"
            + "                subjectScheme='ABR Complextypen'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/9a758542-8d0d-4afa-b664-104b938fe13e'>"
            + "            houtwinning"
            + "        </ddm:subject>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_COMPLEX, result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/9a758542-8d0d-4afa-b664-104b938fe13e");
    }

    @Test
    void AR006_old_abr_complex_maps_to_dansAbrArtifact() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:subject xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'\n"
            + "    schemeURI='https://data.cultureelerfgoed.nl/term/id/rn/a4a7933c-e096-4bcf-a921-4f70a78749fe'\n"
            + "    subjectScheme='Archeologisch Basis Register'\n"
            + "    valueURI='https://data.cultureelerfgoed.nl/term/id/rn/ea77d56e-1475-4e4c-94f5-489bd3d9a3e7'\n"
            + "    >\n"
            + "    ABR BASIS REGISTER OLD\n"
            + "</ddm:subject>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var s = toPrettyJsonString(result);
        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", "dansAbrArtifact", result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/ea77d56e-1475-4e4c-94f5-489bd3d9a3e7");
    }

    @Test
    void AR006_old_abr_complex_with_incomplete_URI_throws_an_exception() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:subject xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'\n"
            + "    schemeURI='https://data.cultureelerfgoed.nl/term/id/rn/a4a7933c-e096-4bcf-a921-4f70a78749fe'\n"
            + "    subjectScheme='Archeologisch Basis Register'\n"
            + "    valueURI='https:data.cultureelerfgoed.nl/term/id/rn/ea77d56e-1475-4e4c-94f5-489bd3d9a3e7'\n"
            + "    >\n"
            + "    ABR BASIS REGISTER OLD\n"
            + "</ddm:subject>")
            + "</ddm:DDM>");

        assertThatThrownBy(() -> mapDdmToDataset(doc, true))
            .isInstanceOf(NullPointerException.class); // TODO should be fixed
    }

    @Test
    void AR006_old_abr_complex_with_invalid_URI_throws_IllegalArgumentException() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:subject xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'\n"
            + "    schemeURI='https://data.cultureelerfgoed.nl/term/id/rn/a4a7933c-e096-4bcf-a921-4f70a78749fe'\n"
            + "    subjectScheme='Archeologisch Basis Register'\n"
            + "    valueURI='https:{//}data.cultureelerfgoed.nl/term/id/rn/ea77d56e-1475-4e4c-94f5-489bd3d9a3e7'\n"
            + "    >\n"
            + "    ABR BASIS REGISTER OLD\n"
            + "</ddm:subject>")
            + "</ddm:DDM>");
        assertThrows(IllegalArgumentException.class, () -> mapDdmToDataset(doc, true));
    }

    @Test
    void AR006_abr_complex_without_value_uri_is_ignored() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:subject"
            + "                subjectScheme='ABR Complextypen'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0'>"
            + "            houtwinning"
            + "        </ddm:subject>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true).getDatasetVersion().getMetadataBlocks();
        assertThat(result.get("dansArchaeologyMetadata").getFields()).isEmpty();
    }

    @Test
    void AR007_ddm_subject_maps_to_dansAbrArtifact() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:subject"
            + "                subjectScheme='ABR Artefacten'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/22cbb070-6542-48f0-8afe-7d98d398cc0b'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/5bd97bc0-697c-4128-b7b2-d2324bc4a2e1'>"
            + "            rammelaar"
            + "        </ddm:subject>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        assertThat(getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_ARTIFACT, result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/5bd97bc0-697c-4128-b7b2-d2324bc4a2e1");
    }

    @Test
    void AR008_ddm_temporal_maps_to_abr_period() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:temporal"
            + "                subjectScheme='ABR Periodes'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/9b688754-1315-484b-9c89-8817e87c1e84'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/5b253754-ddd0-4ae0-a5bb-555176bca858'>"
            + "            Midden Romeinse Tijd A"
            + "        </ddm:temporal>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        var field = getPrimitiveMultiValueField("dansArchaeologyMetadata", ABR_PERIOD, result);
        assertThat(field).containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/5b253754-ddd0-4ae0-a5bb-555176bca858");
    }

    @Test
    void AR008_abr_period_without_value_uri_is_ignored() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:temporal"
            + "                subjectScheme='ABR Periodes'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/9b688754-1315-484b-9c89-8817e87c1e84'"
            + "                >"
            + "            Midden Romeinse Tijd A"
            + "        </ddm:temporal>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true).getDatasetVersion().getMetadataBlocks();
        assertThat(result.get("dansArchaeologyMetadata").getFields()).isEmpty();
    }

}
