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
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import nl.knaw.dans.ingest.DdIngestFlowConfiguration;
import nl.knaw.dans.ingest.IngestFlowConfigReader;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.SingleValueField;
import nl.knaw.dans.lib.dataverse.model.user.AuthenticatedUser;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MappingTestHelper {
    public static final AuthenticatedUser mockedContact = new AuthenticatedUser();

    static {
        mockedContact.setDisplayName("D. O'Seven");
        mockedContact.setEmail("J.Bond@does.not.exist.dans.knaw.nl");
        mockedContact.setAffiliation("DANS");
    }

    public static final VaultMetadata mockedVaultMetadata = new VaultMetadata(
        "doi:10.17026/AR/6L7NBB",
        "urn:uuid:ced0be49-f863-4477-9473-23010526abf3",
        "urn:nbn:nl:ui:13-c7c5a4b2-539e-4b0c-831d-fe31eb197950",
        "otherId:something",
        "1.0",
        "swordToken");
    public static final String rootAttributes = "xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'\n"
        + "         xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n"
        + "         xmlns:dc='http://purl.org/dc/elements/1.1/'\n"
        + "         xmlns:dct='http://purl.org/dc/terms/'\n";
    public static final IngestFlowConfig config = getIngestFlowConfig();

    private static IngestFlowConfig getIngestFlowConfig() {
        IngestFlowConfig config;
        try {
            config = new YamlConfigurationFactory<>(DdIngestFlowConfiguration.class, Validators.newValidator(), Jackson.newObjectMapper(), "dw")
                .build(FileInputStream::new, "src/test/resources/debug-etc/config.yml")
                .getIngestFlow();
            config.setMappingDefsDir(Paths.get("src/test/resources/debug-etc"));
            IngestFlowConfigReader.readIngestFlowConfiguration(config);
        }
        catch (IOException | ConfigurationException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return config;
    }

    public static Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(xml);
    }

    static Dataset mapDdmToDataset(Document ddm, boolean restrictedFilesPresent) {
        return createMapper().toDataverseDataset(ddm, null, "2023-02-27", mockedContact, mockedVaultMetadata, restrictedFilesPresent, null);
    }

    static DepositToDvDatasetMetadataMapper createMapper() {
        return new DepositToDvDatasetMetadataMapper(
            true,
            Set.of("citation", "dansRights", "dansRelationMetadata", "dansArchaeologyMetadata", "dansTemporalSpatial", "dansDataVaultMetadata"),
            config.getIso1ToDataverseLanguage(),
            config.getIso2ToDataverseLanguage(),
            config.getSpatialCoverageCountryTerms()
        );
    }

    public static Document ddmWithCustomProfileContent(String content) throws ParserConfigurationException, IOException, SAXException {
        return readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D19200</ddm:audience>\n"
            + content
            + "    </ddm:profile>\n"
            + dcmi("")
            + "</ddm:DDM>\n");
    }

    public static String minimalDdmProfile() {
        return ""
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D24000</ddm:audience>"
            + "    </ddm:profile>\n";
    }

    public static String dcmi(String content) {
        return "<ddm:dcmiMetadata>\n"
            + "<dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + content
            + "</ddm:dcmiMetadata>\n";
    }

    public static String toPrettyJsonString(Dataset result) throws JsonProcessingException {
        return new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
    }

    public static List<String> getControlledMultiValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((ControlledMultiValueField) t).getValue())
            .findFirst().orElse(null);
    }

    public static List<Map<String, SingleValueField>> getCompoundMultiValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((CompoundMultiValueField) t).getValue())
            .findFirst().orElse(null);
    }

    public static Map<String, SingleValueField> getCompoundSingleValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((CompoundSingleValueField) t).getValue())
            .findFirst().orElse(null);
    }

    public static String getControlledSingleValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((ControlledSingleValueField) t).getValue())
            .findFirst().orElse(null);
    }

    public static List<String> getPrimitiveMultiValueField(String blockId, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(blockId).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(f -> (((PrimitiveMultiValueField) f).getValue()))
            .findFirst().orElse(null);
    }

    public static String getPrimitiveSingleValueField(String blockId, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(blockId).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(f -> (((PrimitiveSingleValueField) f).getValue()))
            .findFirst().orElse(null);
    }

    public static Map<String, List<String>> getFieldNamesOfMetadataBlocks(Dataset result) {
        var metadataBlocks = result.getDatasetVersion().getMetadataBlocks();
        Map<String, List<String>> fields = new HashMap<>();
        for (String blockName : metadataBlocks.keySet()) {
            fields.put(blockName, metadataBlocks.get(blockName).getFields()
                .stream().map(MetadataField::getTypeName).collect(Collectors.toList()));
        }
        return fields;
    }
}