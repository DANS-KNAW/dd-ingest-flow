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

import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import java.util.Set;

import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.ddmWithCustomProfileContent;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getIngestFlowConfig;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static org.assertj.core.api.Assertions.assertThat;

public class DansDataVaultMetadataTest {

    private final DepositToDvDatasetMetadataMapper mapper = new DepositToDvDatasetMetadataMapper(
        true,
        Set.of("citation", "dansRights", "dansDataVaultMetadata"),
        null,
        null,
        null,
        getIngestFlowConfig().getUserMap(), // VLT008
        true
    );

    @Test
    public void VLT008_userId_should_map_to_dataSupplier_from_csv() throws Exception {
        var vaultMetadata = new VaultMetadata("", "", "", "", "", "", "USER001");

        var result = mapper.toDataverseDataset(ddmWithCustomProfileContent(""), null, null, null, vaultMetadata, false, null);
        assertThat(getPrimitiveSingleValueField("dansDataVaultMetadata", "dansDataSupplier", result))
            .isEqualTo("description of USER001");
    }

    @Test
    public void VLT008_dataSupplier_should_default_to_userId() throws Exception {
        var vaultMetadata = new VaultMetadata("", "", "", "", "", "", "xxx");
        var result = mapper.toDataverseDataset(ddmWithCustomProfileContent(""), null, null, null, vaultMetadata, false, null);
        assertThat(getPrimitiveSingleValueField("dansDataVaultMetadata", "dansDataSupplier", result))
            .isEqualTo("xxx");
    }

    @Test
    public void VLT005A_dataSupplier_should_default_to_userId() throws Exception {
        var vaultMetadata = new VaultMetadata("", "", "", "", "", "", "");
        String xml
            = "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<dct:identifier xsi:type='DOI'>10.17026/dans-x3g-jtm3</dct:identifier>")
            + "</ddm:DDM>";
        Document ddm = readDocumentFromString(xml);
        var result = mapper.toDataverseDataset(ddm, null, null, null, vaultMetadata, false, null);
        assertThat(getPrimitiveSingleValueField("dansDataVaultMetadata", "dansOtherId", result))
            .isEqualTo("10.17026/dans-x3g-jtm3");
    }
}
