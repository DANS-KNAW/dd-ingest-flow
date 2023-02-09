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
import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import org.junit.jupiter.api.BeforeEach;
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
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SERIES;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SERIES_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MappingIntegrationTest {

    private final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpatial", "dansDataVaultMetadata");
    private final XmlReader xmlReader = new XmlReaderImpl();

    private final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();
    private final VaultMetadata vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "otherId:something", "otherIdVersion", "swordToken");
    private final String ddmProfile = "    <ddm:profile>\n"
        + "        <dc:title xml:lang=\"en\">Title of the dataset</dc:title>\n"
        + "        <dc:description xml:lang=\"la\">Lorem ipsum.</dc:description>\n"
        + "        <dc:creator>Bergman, W.A.</dc:creator>\n"
        + "        <ddm:created>2012-12</ddm:created>\n"
        + "        <ddm:available>2013-05-01</ddm:available>\n"
        + "        <ddm:audience>D24000</ddm:audience>\n"
        + "        <ddm:accessRights xml:lang=\"en\">OPEN_ACCESS</ddm:accessRights>\n"
        + "    </ddm:profile>\n";

    private Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlString(xml);
    }

    private Dataset mapDdmToDataset(Document doc) {
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage
        ).toDataverseDataset(doc, null, null, null, null, vaultMetadata);
    }

    private String toJsonString(Dataset result) throws JsonProcessingException {
        return new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
    }

    @BeforeEach
    void setUp() {
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
    }

    @Test
    void DD_1216_description_type_other_maps_only_to_author_name() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\">\n"
            + ddmProfile
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + "        <dct:description descriptionType=\"Other\">Author from description other</dct:description>\n"
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
        assertEquals(2, toJsonString(result).split(expected).length);
    }

    @Test
    void DD_1216_description_type_technical_info_maps_once_to_description() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\">\n"
                + ddmProfile
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <dct:description descriptionType=\"TechnicalInfo\">technical description</dct:description>\n"
                + "        <dct:description descriptionType=\"NotKnown\">not known description type</dct:description>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc);
        var str = toJsonString(result);
        assertEquals(2, str.split("not known description type").length);
        assertEquals(2, str.split("technical description").length);
        assertEquals(2, str.split("Lorem ipsum").length);
        var field = (CompoundField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(DESCRIPTION)).findFirst().orElseThrow();
        assertThat(field.getValue())
            .extracting(DESCRIPTION_VALUE)
            .extracting("value")
            .containsOnly("<p>Lorem ipsum.</p>", "<p>technical description</p>", "<p>not known description type</p>");
    }
}