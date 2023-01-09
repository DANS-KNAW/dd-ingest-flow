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
import nl.knaw.dans.ingest.core.service.VaultMetadata;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MappingIntegrationTest {

    private Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(xml);
    }

    private Dataset mapDdmToDataset(Document ddm) {
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
        ).toDataverseDataset(ddm, null, null, null, null, vaultMetadata);
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
    private final String ddmProfile = ""
        + "    <ddm:profile>\n"
        + "        <dc:title xml:lang=\"en\">Title of the dataset</dc:title>\n"
        + "        <dc:description xml:lang=\"la\">Lorem ipsum.</dc:description>\n"
        + "        <dc:creator>Bergman, W.A.</dc:creator>\n"
        + "        <ddm:created>2012-12</ddm:created>\n"
        + "        <ddm:available>2013-05-01</ddm:available>\n"
        + "        <ddm:audience>D24000</ddm:audience>\n"
        + "        <ddm:accessRights xml:lang=\"en\">OPEN_ACCESS</ddm:accessRights>\n"
        + "    </ddm:profile>\n";

    @Test
    void DD_1216_description_type_other_maps_only_to_author_name() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
            + ddmProfile
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + "        <ddm:description descriptionType=\"Other\">Author from description other</ddm:description>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc);
        var field = (CompoundField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals("contributor")).findFirst().orElseThrow();
        var expected = "Author from description other";
        assertThat(field.getValue())
            .extracting(AUTHOR_NAME)
            .extracting("value")
            .containsOnly(expected);
        // not as description and author
        assertThat(toPrettyJsonString(result)).containsOnlyOnce(expected);
    }

    @Test
    void DD_1216_description_type_technical_info_maps_once_to_description() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + ddmProfile
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:description>plain description</dct:description>\n"
                + "        <ddm:description descriptionType=\"TechnicalInfo\">technical description</ddm:description>\n"
                + "        <ddm:description descriptionType=\"NotKnown\">not known description type</ddm:description>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc);
        var str = toPrettyJsonString(result);
        assertThat(str).containsOnlyOnce("not known description type");
        assertThat(str).containsOnlyOnce("technical description");
        assertThat(str).containsOnlyOnce("Lorem ipsum");
        var field = (CompoundField) result.getDatasetVersion().getMetadataBlocks()
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
                + ddmProfile
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <ddm:description descriptionType=\"SeriesInformation\">series 123</ddm:description>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc);

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
}