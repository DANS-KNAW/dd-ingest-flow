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
package nl.knaw.dans.ingest.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ingest.core.service.builder.ArchaeologyFieldBuilder;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DepositToDvDatasetMetadataMapperTest {

    private final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpation", "dansDataVaultMetadata");
    private final XmlReader xmlReader = new XmlReaderImpl();

    private final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();

    Document readDocument(String name) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlFile(Path.of(
            Objects.requireNonNull(getClass().getResource(String.format("/xml/%s", name))).getPath()
        ));
    }

    Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlString(xml);
    }

    DepositToDvDatasetMetadataMapper getMapper() {
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage
        );
    }

    @BeforeEach
    void setUp() {
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
    }

    @Test
    void to_dataverse_dataset() throws Exception {
        var mapper = getMapper();
        var doc = readDocument("dataset.xml");

        var vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "otherId:something", "otherIdVersion", "swordToken");

        var result = mapper.toDataverseDataset(doc, null, null, null, null, vaultMetadata);
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
    }

    @Test
    void test_get_acquisition_methods() throws Exception {
        var mapper = getMapper();
        var doc = readDocument("abrs.xml");

        var result = mapper.getAcquisitionMethods(doc);

        assertThat(result)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("Method 1");
    }

    @Test
    void processMetadataBlock_should_deduplicate_items_for_PrimitiveFieldBuilder() throws Exception {
        var fields = new HashMap<String, MetadataBlock>();
        var mapper = new DepositToDvDatasetMetadataMapper(true, Set.of("citation"), Map.of(), Map.of());
        var builder = new ArchaeologyFieldBuilder();
        builder.addArchisZaakId(Stream.of(
            "TEST",
            "TEST2",
            "TEST3",
            "TEST"
        ));

        mapper.processMetadataBlock(true, fields, "title", "name", builder);

        // the fourth item should be removed
        assertThat(fields.get("title").getFields())
            .extracting("value")
            .containsExactly(List.of("TEST", "TEST2", "TEST3"));
    }

    @Test
    void processMetadataBlock_should_deduplicate_items_for_CompoundFieldBuilder() throws Exception {
        var fields = new HashMap<String, MetadataBlock>();
        var mapper = new DepositToDvDatasetMetadataMapper(true, Set.of("citation"), Map.of(), Map.of());
        var builder = new ArchaeologyFieldBuilder();
        builder.addArchisZaakId(Stream.of(
            "TEST",
            "TEST2",
            "TEST3",
            "TEST"
        ));

        mapper.processMetadataBlock(true, fields, "title", "name", builder);

        // the fourth item should be removed
        assertThat(fields.get("title").getFields())
            .extracting("value")
            .containsExactly(List.of("TEST", "TEST2", "TEST3"));
    }
}