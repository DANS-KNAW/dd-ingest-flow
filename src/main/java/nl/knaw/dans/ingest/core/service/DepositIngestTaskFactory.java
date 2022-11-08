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

import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.DepositToDvDatasetMetadataMapper;
import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.xml.Elem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DepositIngestTaskFactory {

    private final DataverseClient dataverseClient;
    private final DansBagValidator dansBagValidator;

    private final IngestFlowConfig ingestFlowConfig;
    private final DataverseExtra dataverseExtra;
    private final Elem narcisClassification;
    private final Map<String, String> iso1ToDataverseLanguage;
    private final Map<String, String> iso2ToDataverseLanguage;
    private final Map<String, String> reportIdToTerm;
    private final Map<String, String> variantToLicense;
    private final List<URI> supportedLicenses;
    private final boolean isMigration;

    public DepositIngestTaskFactory(boolean isMigration, DataverseClient dataverseClient,
        DansBagValidator dansBagValidator, IngestFlowConfig ingestFlowConfig,
        DataverseExtra dataverseExtra) throws IOException, URISyntaxException {
        this.isMigration = isMigration;
        this.dataverseClient = dataverseClient;
        this.dansBagValidator = dansBagValidator;
        this.ingestFlowConfig = ingestFlowConfig;
        this.dataverseExtra = dataverseExtra;
        this.narcisClassification = nl.knaw.dans.easy.dd2d.DepositIngestTaskFactory.readXml(ingestFlowConfig.getMappingDefsDir().resolve("narcis_classification.xml").toFile());
        this.iso1ToDataverseLanguage = getMap(ingestFlowConfig, "iso639-1-to-dv.csv", "ISO639-1", "Dataverse-language");
        this.iso2ToDataverseLanguage = getMap(ingestFlowConfig, "iso639-2-to-dv.csv", "ISO639-2", "Dataverse-language");
        this.reportIdToTerm = getMap(ingestFlowConfig, "ABR-reports.csv", "URI-suffix", "Term");
        this.variantToLicense = getMap(ingestFlowConfig, "license-uri-variants.csv", "Variant", "Normalized");
        this.supportedLicenses = getUriList(ingestFlowConfig, "supported-licenses.txt");
    }

    scala.collection.immutable.Map<String, String> toScalaMap(Map<String, String> input) {

        var tuples = input.entrySet().stream()
            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

        return (scala.collection.immutable.Map<String, String>) Map$.MODULE$.apply(JavaConverters.asScalaBuffer(tuples).toSeq());
    }

    public DepositIngestTask createDepositIngestTask(Deposit deposit, File outboxDir) {
        var depositToDvDatasetMetadataMapper = new DepositToDvDatasetMetadataMapper(
            false,
            nl.knaw.dans.easy.dd2d.DepositIngestTaskFactory.getActiveMetadataBlocks(dataverseClient).get(),
            narcisClassification,
            toScalaMap(iso1ToDataverseLanguage),
            toScalaMap(iso2ToDataverseLanguage),
            toScalaMap(reportIdToTerm)
        );

        var zipFileHandler = new ZipFileHandler(ingestFlowConfig.getZipWrappingTempDir());
        var fileExclusionPattern = Option.apply(Pattern.compile(ingestFlowConfig.getFileExclusionPattern()));

        if (this.isMigration) {
            return new DepositMigrationTask(
                depositToDvDatasetMetadataMapper,
                deposit,
                dataverseClient,
                ingestFlowConfig.getDepositorRole(),
                fileExclusionPattern,
                zipFileHandler,
                variantToLicense,
                supportedLicenses,
                dansBagValidator,
                dataverseExtra.getPublishAwaitUnlockMaxRetries(),
                dataverseExtra.getPublishAwaitUnlockWaitTimeMs(),
                outboxDir.toPath()
            );
        }
        else {
            return new DepositIngestTask(
                depositToDvDatasetMetadataMapper,
                deposit,
                dataverseClient,
                ingestFlowConfig.getDepositorRole(),
                fileExclusionPattern,
                zipFileHandler,
                variantToLicense,
                supportedLicenses,
                dansBagValidator,
                dataverseExtra.getPublishAwaitUnlockMaxRetries(),
                dataverseExtra.getPublishAwaitUnlockWaitTimeMs(),
                outboxDir.toPath()
            );
        }

    }

    private Map<String, String> getMap(IngestFlowConfig ingestFlowConfig, String mappingCsv, String keyColumn, String valueColumn) throws IOException {
        return loadCsvToMap(ingestFlowConfig.getMappingDefsDir().resolve(mappingCsv),
            keyColumn,
            valueColumn);
    }

    private List<URI> getUriList(IngestFlowConfig ingestFlowConfig, String listFile) throws URISyntaxException, IOException {
        var uris = FileUtils.readLines(ingestFlowConfig.getMappingDefsDir().resolve(listFile).toFile(), StandardCharsets.UTF_8);
        var result = new ArrayList<URI>();

        for (var u : uris) {
            result.add(new URI(u));
        }

        return result;
    }

    public DepositIngestTask createIngestTask(Path depositDir, Path outboxDir, EventWriter eventWriter) {
        var deposit = new Deposit(better.files.File.apply(depositDir));
        return createDepositIngestTask(deposit, outboxDir.toFile());
    }

    private Map<String, String> loadCsvToMap(Path path, String keyColumn, String valueColumn) throws IOException {
        try (var parser = CSVParser.parse(path, StandardCharsets.UTF_8, CSVFormat.RFC4180.withFirstRecordAsHeader())) {
            var result = new HashMap<String, String>();

            for (var record : parser) {
                result.put(record.get(keyColumn), record.get(valueColumn));
            }

            return result;
        }
    }
}
