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
import nl.knaw.dans.easy.dd2d.ZipFileHandler;
import nl.knaw.dans.easy.dd2d.dansbag.DansBagValidator;
import nl.knaw.dans.easy.dd2d.migrationinfo.MigrationInfo;
import nl.knaw.dans.easy.dd2d.migrationinfo.MigrationInfoConfig;
import nl.knaw.dans.ingest.core.config.DataverseConfigScala;
import nl.knaw.dans.ingest.core.config.HttpServiceConfig;
import nl.knaw.dans.ingest.core.config.IngestConfig;
import nl.knaw.dans.ingest.core.service.EventWriter;
import nl.knaw.dans.lib.dataverse.DataverseInstance;
import nl.knaw.dans.lib.dataverse.DataverseInstanceConfig;
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

    public DepositIngestTaskFactoryWrapper(
        IngestConfig ingestConfig,
        DataverseConfigScala dataverseConfigScala,
        HttpServiceConfig migrationInfoConfig,
        HttpServiceConfig validationDansBagConfig) {

        final DataverseInstance dataverseInstance = new DataverseInstance(new DataverseInstanceConfig(
            DepositIngestTaskFactory.appendSlash(dataverseConfigScala.getHttp().getBaseUrl()),
            dataverseConfigScala.getApi().getApiKey(),
            Option.apply(dataverseConfigScala.getApi().getUnblockKey()),
            dataverseConfigScala.getHttp().getConnectionTimeoutMs(),
            dataverseConfigScala.getHttp().getReadTimeoutMs(),
            Integer.toString(dataverseConfigScala.getApi().getApiVersion()),
            dataverseConfigScala.getApi().getAwaitUnlockMaxRetries(),
            dataverseConfigScala.getApi().getAwaitUnlockWaitTimeMs()));

        final DansBagValidator validator = new DansBagValidator(
            DepositIngestTaskFactory.appendSlash(validationDansBagConfig.getBaseUrl()),
            validationDansBagConfig.getConnectionTimeoutMs(),
            validationDansBagConfig.getReadTimeoutMs());

        final MigrationInfo migrationInfo = new MigrationInfo(
            new MigrationInfoConfig(
                DepositIngestTaskFactory.appendSlash(migrationInfoConfig.getBaseUrl()),
                migrationInfoConfig.getConnectionTimeoutMs(),
                migrationInfoConfig.getReadTimeoutMs()),
            false // TODO: make configurable
        );

        final Elem narcisClassification = DepositIngestTaskFactory.readXml(ingestConfig.getMappingDefsDir().resolve("narcis_classification.xml").toFile());
        final Map<String, String> iso1ToDataverseLanguage = getMap(ingestConfig, "iso639-1-to-dv.csv", "ISO639-1", "Dataverse-language");
        final Map<String, String> iso2ToDataverseLanguage = getMap(ingestConfig, "iso639-2-to-dv.csv", "ISO639-2", "Dataverse-language");
        final Map<String, String> reportIdToTerm = getMap(ingestConfig, "ABR-reports.csv", "URI-suffix", "Term");
        final Map<String, String> variantToLicense = getMap(ingestConfig, "license-uri-variants.csv", "Variant", "Normalized");
        final List<URI> supportedLicenses = getUriList(ingestConfig, "supported-licenses.txt");

        factory = new DepositIngestTaskFactory(
            true,
            Option.apply(Pattern.compile(ingestConfig.getFileExclusionPattern())),
            new ZipFileHandler(File.apply(ingestConfig.getZipWrappingTempDir())),
            ingestConfig.getDepositorRole(),
            false,
            ingestConfig.isDeduplicate(),
            DepositIngestTaskFactory.getActiveMetadataBlocks(dataverseInstance).get(),
            Option.apply(validator),
            dataverseInstance,
            Option.apply(migrationInfo),
            dataverseConfigScala.getApi().getPublishAwaitUnlockMaxRetries(),
            dataverseConfigScala.getApi().getPublishAwaitUnlockWaitTimeMs(),
            narcisClassification,
            iso1ToDataverseLanguage,
            iso2ToDataverseLanguage,
            variantToLicense,
            supportedLicenses,
            reportIdToTerm);
    }

    private Map<String, String> getMap(IngestConfig ingestConfig, String mappingCsv, String keyColumn, String valueColumn) {
        return DepositIngestTaskFactory
            .loadCsvToMap(File.apply(ingestConfig.getMappingDefsDir().resolve(mappingCsv)),
                keyColumn,
                valueColumn).get();
    }

    private List<URI> getUriList(IngestConfig ingestConfig, String listFile) {
        return DepositIngestTaskFactory
            .loadTxtToUriList(File.apply(ingestConfig.getMappingDefsDir().resolve(listFile)))
            .get();
    }

    public DepositImportTaskWrapper createIngestTask(Path depositDir, Path outboxDir, EventWriter eventWriter) {
        return new DepositImportTaskWrapper(factory.createDepositIngestTask(new Deposit(File.apply(depositDir)), File.apply(outboxDir)), eventWriter);
    }

}
