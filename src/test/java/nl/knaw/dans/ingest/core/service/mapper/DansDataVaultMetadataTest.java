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

import java.util.Set;

import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.ddmWithCustomProfileContent;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getFieldNamesOfMetadataBlocks;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getIngestFlowConfig;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveSingleValueField;
import static org.assertj.core.api.Assertions.assertThat;

public class DansDataVaultMetadataTest {

    private final DepositToDvDatasetMetadataMapper mapper = new DepositToDvDatasetMetadataMapper(
        true,
        Set.of("citation", "dansRights", "dansDataVaultMetadata"),
        null,
        null,
        null,
        getIngestFlowConfig().getMigration().getDataSuppliers(), // VLT008
        true
    );

    @Test
    public void VLT008_userId_should_map_to_dataSupplier_from_config_yml() throws Exception {
        var vaultMetadata = new VaultMetadata("", "", "", "", "", "USER001");

        var result = mapper.toDataverseDataset(ddmWithCustomProfileContent(""), null, null, null, vaultMetadata, false, null, null);
        assertThat(getPrimitiveSingleValueField("dansDataVaultMetadata", "dansDataSupplier", result))
            .isEqualTo("The Organization Name");
    }

    @Test
    public void VLT008_dataSupplier_should_ignore_not_configured_userId() throws Exception {
        var vaultMetadata = new VaultMetadata("", "", "", "", "", "xxx");
        var result = mapper.toDataverseDataset(ddmWithCustomProfileContent(""), null, null, null, vaultMetadata, false, null, null);
        assertThat(getFieldNamesOfMetadataBlocks(result).get("dansArchaeologyMetadata"))
            .doesNotContain("dansDataSupplier");
    }
}
