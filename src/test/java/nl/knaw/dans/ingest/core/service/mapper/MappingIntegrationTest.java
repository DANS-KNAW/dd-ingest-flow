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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.NOTES_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static org.assertj.core.api.Assertions.assertThat;

class MappingIntegrationTest {

    private Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(xml);
    }

    private Dataset mapDdmToDataset(Document ddm, boolean hasRestrictedOrNoneFiles) {
        final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpatial", "dansDataVaultMetadata");
        final VaultMetadata vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "otherId:something", "otherIdVersion", "swordToken");
        final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
        final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage
        ).toDataverseDataset(ddm, null, null, null, vaultMetadata, hasRestrictedOrNoneFiles);
    }

    private String toPrettyJsonString(Dataset result) throws JsonProcessingException {
        return new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
    }

    private String toCompactJsonString(Dataset result) throws JsonProcessingException {
        return new ObjectMapper()
            .writer()
            .writeValueAsString(result);
    }

    private final String rootAttributes = "xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
        + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
        + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
        + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n";

    private String ddmProfileWithAudiences(String... audience) {
        return ""
            + "    <ddm:profile>\n"
            + "        <dc:title xml:lang=\"en\">Title of the dataset</dc:title>\n"
            + "        <dc:description xml:lang=\"la\">Lorem ipsum.</dc:description>\n"
            + "        <dc:creator>Bergman, W.A.</dc:creator>\n"
            + "        <ddm:created>2012-12</ddm:created>\n"
            + Arrays.stream(audience).map(s -> "<ddm:audience>"+s+"</ddm:audience>")
                .collect(Collectors.joining("\n        ", "        ","\n"))
            + "        <ddm:accessRights xml:lang=\"en\">OPEN_ACCESS</ddm:accessRights>\n"
            + "    </ddm:profile>\n";
    }

    private final String minimalDCMI = ""
        + "    <ddm:dcmiMetadata>\n"
        + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
        + "        <ddm:description descriptionType=\"Other\">Author from description other</ddm:description>\n"
        + "    </ddm:dcmiMetadata>\n";

    @Test
    void DD_1216_description_type_other_maps_only_to_author_name() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
            + ddmProfileWithAudiences("D24000")
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + "        <ddm:description descriptionType=\"Other\">Author from description other</ddm:description>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var field = (CompoundMultiValueField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals("contributor")).findFirst().orElseThrow();
        var expected = "Author from description other";
        assertThat(field.getValue())
            .extracting(CONTRIBUTOR_NAME)
            .extracting("value")
            .containsOnly(expected);
        // not as description and author
        assertThat(toPrettyJsonString(result)).containsOnlyOnce(expected);
    }

    @Test
    void DD_1216_description_type_technical_info_maps_once_to_description() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + ddmProfileWithAudiences("D24000")
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:description>plain description</dct:description>\n"
                + "        <ddm:description descriptionType=\"TechnicalInfo\">technical description</ddm:description>\n"
                + "        <ddm:description descriptionType=\"NotKnown\">not known description type</ddm:description>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);
        assertThat(str).containsOnlyOnce("not known description type");
        assertThat(str).containsOnlyOnce("technical description");
        assertThat(str).containsOnlyOnce("Lorem ipsum");
        var field = (CompoundMultiValueField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(DESCRIPTION)).findFirst().orElseThrow();
        assertThat(field.getValue())
            .extracting(DESCRIPTION_VALUE)
            .extracting("value")
            .containsOnly("<p>plain description</p>", "<p>Lorem ipsum.</p>", "<p>technical description</p>", "<p>not known description type</p>");
    }

    @Test
    void DD_1216_description_type_series_information_maps_only_to_series() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + ddmProfileWithAudiences("D24000")
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <ddm:description descriptionType=\"SeriesInformation\">series 123</ddm:description>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);

        // TODO improve assertions after DD-1237 (note that the single compound field is an anonymous class)
        //  {"typeClass" : "compound", "typeName" : "series", "multiple" : false, "value" :
        //  {"seriesName" : {"typeClass" : "primitive", "typeName" : "seriesInformation", "multiple" : false, "value" : "<p>series 123</p>"}}
        //  }
        var str = toCompactJsonString(result);

        // not as description and series
        assertThat(str).containsOnlyOnce("<p>series 123</p>");

        // no square bracket
        assertThat(str).containsOnlyOnce("\"value\":{\"seriesInformation\"");
    }

    @Test
    void DD_1216_provenance_maps_to_notes() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + ddmProfileWithAudiences("D24000")
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:provenance>copied xml to csv</dct:provenance>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);

        assertThat(str).containsOnlyOnce("copied xml to csv");
        assertThat(str).doesNotContain("<p>copied xml to csv</p>");

        var field = (PrimitiveSingleValueField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(NOTES_TEXT)).findFirst().orElseThrow();
        assertThat(field.getValue()).isEqualTo("copied xml to csv");
    }

    @Test
    void DD_1265_subject_omits_other() throws Exception {
        var doc = readDocumentFromString(""
            +"<ddm:DDM " + rootAttributes + ">\n"
            + ddmProfileWithAudiences("D19200", "D11200", "D88200", "D40200", "D17200")
            + minimalDCMI
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getCitationSubject(result)).isEqualTo(List.of("Astronomy and Astrophysics","Law", "Mathematical Sciences"));
    }

    @Test
    void DD_1265_subject_is_other() throws Exception {
        var doc = readDocumentFromString(""
            +"<ddm:DDM " + rootAttributes + ">\n"
            + ddmProfileWithAudiences("D19200", "D88200")
            + minimalDCMI
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getCitationSubject(result)).isEqualTo(List.of("Other"));
    }

    private List<String> getCitationSubject(Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(SUBJECT))
            .map(t -> ((ControlledMultiValueField)t).getValue())
            .findFirst().orElseThrow();
    }

    @Test
    void DD_1216_DctAccesRights_maps_to_description() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + ddmProfileWithAudiences("D10000")
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:accessRights>Some story</dct:accessRights>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);

        assertThat(str).containsOnlyOnce("<p>Some story</p>");

        var field = (CompoundMultiValueField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(DESCRIPTION)).findFirst().orElseThrow();
        assertThat(field.getValue())
            .extracting(DESCRIPTION_VALUE)
            .extracting("value")
            .containsOnly("<p>Some story</p>", "<p>Lorem ipsum.</p>");
        assertThat(result.getDatasetVersion().getTermsOfAccess()).isEqualTo("N/a");
    }

    @Test
    void DD_1216_DctAccesRights_maps_to_termsofaccess() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + ddmProfileWithAudiences("D10000")
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:accessRights>Some story</dct:accessRights>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, true);
        assertThat(result.getDatasetVersion().getTermsOfAccess()).isEqualTo("Some story");
    }
}