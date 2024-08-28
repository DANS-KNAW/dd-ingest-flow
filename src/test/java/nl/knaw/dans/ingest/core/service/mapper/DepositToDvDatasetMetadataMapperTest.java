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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.ingest.core.service.mapper.builder.ArchaeologyFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.mapping.AbrAcquisitionMethod;
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

    private final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpatial", "dansDataVaultMetadata");
    private final XmlReader xmlReader = new XmlReaderImpl();

    private final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> abrReportCodeToTerm = new HashMap<>();
    private final Map<String, String> abrAcquisitionMethodCodeToTerm = new HashMap<>();
    private final Map<String, String> abrComplexCodeToTerm = new HashMap<>();
    private final Map<String, String> abrPeriodCodeToTerm = new HashMap<>();
    private final Map<String, String> abrArtifactCodeToTerm = new HashMap<>();
    private final List<String> spatialCoverageCountryTerms = List.of("Netherlands", "United Kingdom", "Belgium", "Germany");
    private final Map<String, String> dataSuppliers = new HashMap<>();
    private final List<String> skipFields = List.of();

    Document readDocument(String name) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlFile(Path.of(
            Objects.requireNonNull(getClass().getResource(String.format("/xml/%s", name))).getPath()
        ));
    }

    DepositToDvDatasetMetadataMapper getMigrationMapper() {
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage,
            abrReportCodeToTerm, abrAcquisitionMethodCodeToTerm, abrComplexCodeToTerm,
            abrArtifactCodeToTerm, abrPeriodCodeToTerm, spatialCoverageCountryTerms, dataSuppliers, skipFields, true);
    }

    DepositToDvDatasetMetadataMapper getNonMigrationMapper() {
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage,
            abrReportCodeToTerm, abrAcquisitionMethodCodeToTerm, abrComplexCodeToTerm,
            abrArtifactCodeToTerm, abrPeriodCodeToTerm, spatialCoverageCountryTerms, dataSuppliers, skipFields, false);
    }

    @BeforeEach
    void setUp() {
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
        abrAcquisitionMethodCodeToTerm.put("ADXX", "https://data.cultureelerfgoed.nl/term/id/abr/09ae9be1-ee40-4112-beea-27672df67c18");
    }

    @Test
    void to_dataverse_dataset() throws Exception {
        var mapper = getMigrationMapper();
        var doc = readDocument("dataset.xml");

        var vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "otherId:something", "swordToken");

        var result = mapper.toDataverseDataset(doc, null, null, null, vaultMetadata, null, false, null, null);
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
    }

    @Test
    void toDataverseDataset_should_not_have_doi() throws Exception {
        var mapper = getMigrationMapper();
        var doc = readDocument("dataset-simple-with-doi.xml");

        var vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "doi:a/b", "swordToken");

        var result = mapper.toDataverseDataset(doc, null, null, null, vaultMetadata, null, false, null, null);
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);

        assertThat(str).doesNotContain("doi:a/b");
        assertThat(str).doesNotContain("10.17026/easy-dans-doi");
    }

    @Test
    void toDataverseDataset_should_include_otherId_from_vault_metadata() throws Exception {
        var mapper = getNonMigrationMapper();
        var doc = readDocument("dataset-simple-with-doi.xml");

        var vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "doi:a/b", "swordToken");

        var result = mapper.toDataverseDataset(doc, null, null, null, vaultMetadata, null, false, "org-id", null);
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);

        assertThat(str).contains("org-id");
        assertThat(str).doesNotContain("10.17026/easy-dans-doi");
    }

    @Test
    void test_get_acquisition_methods() throws Exception {
        var mapper = getMigrationMapper();
        var doc = readDocument("abrs.xml");

        var result = mapper.getAcquisitionMethods(doc).filter(AbrAcquisitionMethod::isVerwervingswijze);

        assertThat(result)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("Method 1");
    }

    @Test
    void processMetadataBlock_should_deduplicate_items_for_PrimitiveFieldBuilder() {
        var mapper = new DepositToDvDatasetMetadataMapper(true, Set.of("citation"), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), spatialCoverageCountryTerms, dataSuppliers,
            skipFields, true);
        var fields = new HashMap<String, MetadataBlock>();
        var builder = new ArchaeologyFieldBuilder();
        builder.addArchisZaakId(Stream.of(
            "TEST",
            "TEST2",
            "TEST3",
            "TEST"
        ));

        mapper.processMetadataBlock(true, fields, "title", "name", builder, skipFields);

        // the fourth item should be removed
        assertThat(fields.get("title").getFields())
            .extracting("value")
            .containsExactly(List.of("TEST", "TEST2", "TEST3"));
    }

    @Test
    void processMetadataBlock_should_deduplicate_items_for_CompoundFieldBuilder() {
        var fields = new HashMap<String, MetadataBlock>();
        var mapper = new DepositToDvDatasetMetadataMapper(true, Set.of("citation"), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), spatialCoverageCountryTerms, dataSuppliers,
            skipFields, true);
        var builder = new ArchaeologyFieldBuilder();
        builder.addArchisZaakId(Stream.of(
            "TEST",
            "TEST2",
            "TEST3",
            "TEST"
        ));

        mapper.processMetadataBlock(true, fields, "title", "name", builder, skipFields);

        // the fourth item should be removed
        assertThat(fields.get("title").getFields())
            .extracting("value")
            .containsExactly(List.of("TEST", "TEST2", "TEST3"));
    }
}