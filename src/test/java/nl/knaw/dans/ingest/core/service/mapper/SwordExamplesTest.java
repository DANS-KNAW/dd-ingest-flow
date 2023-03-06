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

import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.file.FileMeta;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapper;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mockedContact;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mockedVaultMetadata;
import static nl.knaw.dans.ingest.core.service.mapper.mapping.FileElement.toFileMeta;
import static org.assertj.core.api.Assertions.assertThat;

public class SwordExamplesTest {

    private final String examplesTagOrBranch = "38f0e8901b5ce57117445c4f819f6613eb1f05ee"; // 2023-02-28

    private Document parseSwordExampleXml(String path) throws SAXException, IOException, ParserConfigurationException {
        // TODO copy via maven plugin to target directory and make getFactory private again
        String uri = String.format("https://raw.githubusercontent.com/DANS-KNAW/dd-dans-sword2-examples/%s/src/main/resources/example-bags/valid/%s", examplesTagOrBranch, path);
        return new XmlReaderImpl().getFactory().newDocumentBuilder().parse(new InputSource(uri));
    }

    @Test
    public void all_mappings_produces_most_documented_sub_fields() throws Exception {

        var ddm = parseSwordExampleXml("all-mappings/metadata/dataset.xml");
        var dastaset = mapper().toDataverseDataset(ddm, null, "2023-02-27", mockedContact, mockedVaultMetadata, true, true);
        var fieldNames = getFieldNamesOfMetadataBlocks(dastaset);
        assertThat(fieldNames.get("citation")).hasSameElementsAs(List.of(
            "title", // CIT001
            "alternativeTitle", // CIT002
            "otherId", // CIT002A-4
            "author", // CIT006-7
            "datasetContact", // CIT008
            "dsDescription", // CIT009-12
            "subject", // CIT013
            "keyword", // CIT014-16
            "publication", // CIT017
            // TODO notesText // CIT017A
            "language", // CIT018
            "productionDate", // CIT019
            "contributor", // CIT020 + CIT021
            "grantNumber", // CIT022-23
            "distributor", // CIT024
            "distributionDate", // CIT025
            "dateOfDeposit", // CIT025A
            "dateOfCollection", // CIT026
            "series", // CIT027
            "dataSources")); // CIT028
        assertThat(fieldNames.get("dansRights")).hasSameElementsAs(List.of(
            "dansRightsHolder", // RIG000 + RIG001
            "dansPersonalDataPresent", // RIG002
            "dansMetadataLanguage")); // RIG003
        assertThat(fieldNames.get("dansRelationMetadata")).hasSameElementsAs(List.of(
            "dansAudience", // REL001
            "dansCollection", // REL002
            "dansRelation")); // REL003
        assertThat(fieldNames.get("dansArchaeologyMetadata")).hasSameElementsAs(List.of(
            "dansArchisZaakId", // AR001
            "dansArchisNumber", // AR002
            "dansAbrRapportType", // AR003
            "dansAbrRapportNummer", // AR004
            "dansAbrVerwervingswijze", // AR005
            "dansAbrComplex", // AR006
            "dansAbrArtifact", // AR007
            "dansAbrPeriod")); // AR008
        assertThat(fieldNames.get("dansTemporalSpatial")).hasSameElementsAs(List.of(
            "dansSpatialPoint", // TS002 + TS003
            "dansSpatialBox", // TS004 + TS005
            "dansTemporalCoverage", // TS001
            "dansSpatialCoverageControlled", // TS006
            "dansSpatialCoverageText")); // TS007
        assertThat(fieldNames.get("dansDataVaultMetadata")).hasSameElementsAs(List.of(
            // VLT001-2 dansDataversePid TODO not by mapping?
            "dansBagId", // VLT003
            "dansNbn", // VLT004
            "dansOtherId", // VLT005
            "dansOtherIdVersion", // VLT006
            "dansSwordToken")); // VLT007
        // TODO terms / restricted files // TRMnnn
        assertThat(dastaset.getDatasetVersion().getTermsOfAccess())
            .isEqualTo("Restricted files accessible under the following conditions: ...");

        var filesXml = parseSwordExampleXml("all-mappings/metadata/files.xml");
        var files = XPathEvaluator.nodes(filesXml, "/files:files/files:file")
            .map(node -> toFileMeta(node, true))
            .collect(Collectors.toList());

        files.stream().map(FileMeta::getDirectoryLabel);
        assertThat(files.stream().map(FileMeta::getLabel)) // FIL001
            .containsExactlyInAnyOrder("file1.txt", "file2.txt", "c_a_q_d_l_g_p_s_h_.txt");
        assertThat(files.stream().map(FileMeta::getDirectoryLabel))
            .containsExactlyInAnyOrder(null, "subdir", "subdir__"); // FIL002
        assertThat(files.stream().map(FileMeta::getDescription)) // FIL003 + FIL004
            .containsExactlyInAnyOrder(null,
                "original_filepath: \"subdir_υποφάκελο/c:a*q?d\"l<g>p|s;h#.txt\"; description: \"A file with a problematic name\"",
                "A file with a simple description");
        assertThat(files.stream().map(FileMeta::getRestrict)) // FIL005
            .containsExactlyInAnyOrder(true, true, true);
    }

    @Test
    public void audiences() throws Exception {

        var ddm = parseSwordExampleXml("audiences/metadata/dataset.xml");
        var result = mapper().toDataverseDataset(ddm, null, "2023-02-27", mockedContact, mockedVaultMetadata, true, true);
        var fieldNames = getFieldNamesOfMetadataBlocks(result);
        // only checking what adds to assertions of all_mappings
        assertThat(result.getDatasetVersion().getTermsOfAccess())
            .isEqualTo("N/a"); // same for embargoed, audiences, restricted-files-with-access-request ...
    }

    private Map<String, List<String>> getFieldNamesOfMetadataBlocks(Dataset result) {
        var metadataBlocks = result.getDatasetVersion().getMetadataBlocks();
        Map<String, List<String>> fields = new HashMap<>();
        for (String blockName : metadataBlocks.keySet())
            fields.put(blockName, metadataBlocks.get(blockName).getFields()
                .stream().map(MetadataField::getTypeName).collect(Collectors.toList()));
        return fields;
    }
}
