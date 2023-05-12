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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlockSummary;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class DepositToDvDatasetMetadataMapperFactory {

    private final Map<String, String> iso1ToDataverseLanguage;
    private final Map<String, String> iso2ToDataverseLanguage;
    private final List<String> spatialCoverageCountryTerms;
    private final DataverseClient dataverseClient;

    public DepositToDvDatasetMetadataMapperFactory(Map<String, String> iso1ToDataverseLanguage, Map<String, String> iso2ToDataverseLanguage,
        List<String> spatialCoverageCountryTerms, DataverseClient dataverseClient) {
        this.iso1ToDataverseLanguage = iso1ToDataverseLanguage;
        this.iso2ToDataverseLanguage = iso2ToDataverseLanguage;
        this.spatialCoverageCountryTerms = spatialCoverageCountryTerms;
        this.dataverseClient = dataverseClient;
    }

    public DepositToDvDatasetMetadataMapper createMapper(boolean deduplicate, boolean isMigration) {
        return new DepositToDvDatasetMetadataMapper(
            deduplicate,
            getActiveMetadataBlocks(),
            iso1ToDataverseLanguage,
            iso2ToDataverseLanguage,
            spatialCoverageCountryTerms,
            isMigration
        );
    }

    Set<String> getActiveMetadataBlocks() {
        try {
            var result = dataverseClient.dataverse("root").listMetadataBlocks();
            return result.getData().stream().map(MetadataBlockSummary::getName).collect(Collectors.toSet());
        }
        catch (IOException | DataverseException e) {
            log.error("Unable to fetch active metadata blocks", e);
            throw new RuntimeException(e);
        }
    }
}
