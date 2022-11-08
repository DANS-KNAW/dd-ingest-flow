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
package nl.knaw.dans.ingest.core.legacy;

import better.files.File;
import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.DepositIngestTaskFactory;
import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.service.DansBagValidator;
import nl.knaw.dans.ingest.core.service.EventWriter;
import nl.knaw.dans.ingest.core.service.ZipFileHandler;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import scala.Option;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.xml.Elem;

import java.net.URI;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 * Wraps the legacy Scala ingest task factory
 */
public class DepositIngestTaskFactoryWrapper {
    private final DepositIngestTaskFactory factory;
    private final DataverseClient dataverseClient;
    private final DansBagValidator validator;

    public DepositIngestTaskFactoryWrapper(
        boolean isMigration,
        IngestFlowConfig ingestFlowConfig,
        DataverseClient dataverseClient,
        DataverseExtra dataverseExtra,
        DansBagValidator dansBagValidator) {

        this.dataverseClient = dataverseClient;
        this.validator = dansBagValidator;

        final Elem narcisClassification = DepositIngestTaskFactory.readXml(ingestFlowConfig.getMappingDefsDir().resolve("narcis_classification.xml").toFile());
        final Map<String, String> iso1ToDataverseLanguage = getMap(ingestFlowConfig, "iso639-1-to-dv.csv", "ISO639-1", "Dataverse-language");
        final Map<String, String> iso2ToDataverseLanguage = getMap(ingestFlowConfig, "iso639-2-to-dv.csv", "ISO639-2", "Dataverse-language");
        final Map<String, String> reportIdToTerm = getMap(ingestFlowConfig, "ABR-reports.csv", "URI-suffix", "Term");
        final Map<String, String> variantToLicense = getMap(ingestFlowConfig, "license-uri-variants.csv", "Variant", "Normalized");
        final List<URI> supportedLicenses = getUriList(ingestFlowConfig, "supported-licenses.txt");

        factory = null;
//        factory = new DepositIngestTaskFactory(
//            isMigration,
//            Option.apply(Pattern.compile(ingestFlowConfig.getFileExclusionPattern())),
//            new ZipFileHandler(ingestFlowConfig.getZipWrappingTempDir()),
//            ingestFlowConfig.getDepositorRole(),
//            false,
//            ingestFlowConfig.isDeduplicate(),
//            DepositIngestTaskFactory.getActiveMetadataBlocks(dataverseClient).get(),
//            Option.apply(validator),
//            dataverseClient,
//            dataverseExtra.getPublishAwaitUnlockMaxRetries(),
//            dataverseExtra.getPublishAwaitUnlockWaitTimeMs(),
//            narcisClassification,
//            iso1ToDataverseLanguage,
//            iso2ToDataverseLanguage,
//            variantToLicense,
//            supportedLicenses,
//            reportIdToTerm);
    }

    private Map<String, String> getMap(IngestFlowConfig ingestFlowConfig, String mappingCsv, String keyColumn, String valueColumn) {
        return DepositIngestTaskFactory
            .loadCsvToMap(File.apply(ingestFlowConfig.getMappingDefsDir().resolve(mappingCsv)),
                keyColumn,
                valueColumn).get();
    }

    private List<URI> getUriList(IngestFlowConfig ingestFlowConfig, String listFile) {
        return DepositIngestTaskFactory
            .loadTxtToUriList(File.apply(ingestFlowConfig.getMappingDefsDir().resolve(listFile)))
            .get();
    }

    public DepositImportTaskWrapper createIngestTask(Path depositDir, Path outboxDir, EventWriter eventWriter) {
        return new DepositImportTaskWrapper(factory.createDepositIngestTask(new Deposit(File.apply(depositDir)), File.apply(outboxDir)), eventWriter);
    }

}

