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
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.SingleValueField;
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

class MappingTestHelper {

    static Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(xml);
    }

    static Dataset mapDdmToDataset(Document ddm, boolean filesThatAreAccessibleToNonePresentInDeposit, boolean filesThatAreRestrictedRequestPresentInDeposit) {
        final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpatial", "dansDataVaultMetadata");
        final VaultMetadata vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "otherId:something", "otherIdVersion", "swordToken");
        final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
        final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage, List.of("Netherlands", "United Kingdom", "Belgium", "Germany")
        ).toDataverseDataset(ddm, null, null, null, vaultMetadata, filesThatAreAccessibleToNonePresentInDeposit, filesThatAreRestrictedRequestPresentInDeposit);
    }

    static final String rootAttributes = "xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'\n"
        + "         xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n"
        + "         xmlns:dc='http://purl.org/dc/elements/1.1/'\n"
        + "         xmlns:dct='http://purl.org/dc/terms/'\n";

    static Document ddmWithCustomProfileContent(String content) throws ParserConfigurationException, IOException, SAXException {
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

    static String minimalDdmProfile() {
        return ""
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D24000</ddm:audience>"
            + "    </ddm:profile>\n";
    }

    static String dcmi(String content) {
        return "<ddm:dcmiMetadata>\n"
            + "<dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + content
            + "</ddm:dcmiMetadata>\n";
    }

    static String toPrettyJsonString(Dataset result) throws JsonProcessingException {
        return new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
    }

    static String toCompactJsonString(Dataset result) throws JsonProcessingException {
        return new ObjectMapper()
            .writer()
            .writeValueAsString(result);
    }

    static List<String> getControlledMultiValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((ControlledMultiValueField)t).getValue())
            .findFirst().orElse(null);
    }

    static List<Map<String, SingleValueField>> getCompoundMultiValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((CompoundMultiValueField)t).getValue())
            .findFirst().orElse(null);
    }

    static String getControlledSingleValueField(String block, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(block).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(t -> ((ControlledSingleValueField)t).getValue())
            .findFirst().orElse(null);
    }


    static List<String> getPrimitiveMultipleValueField(String blockId, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(blockId).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(f -> (((PrimitiveMultiValueField)f).getValue()))
            .findFirst().orElse(null);
    }

    static String getPrimitiveSingleValueField(String blockId, String fieldId, Dataset result) {
        return result.getDatasetVersion().getMetadataBlocks()
            .get(blockId).getFields().stream()
            .filter(f -> f.getTypeName().equals(fieldId))
            .map(f -> (((PrimitiveSingleValueField)f).getValue()))
            .findFirst().orElse(null);
    }
}