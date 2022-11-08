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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.DepositToDvDatasetMetadataMapper;
import nl.knaw.dans.easy.dd2d.RejectedDepositException;
import nl.knaw.dans.easy.dd2d.mapping.Amd;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import scala.Option;
import scala.util.Try;
import scala.xml.Node;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
public class DepositMigrationTask extends DepositIngestTask {
    public DepositMigrationTask(DepositToDvDatasetMetadataMapper datasetMetadataMapper, Deposit deposit, DataverseClient dataverseClient, String depositorRole, Option<Pattern> fileExclusionPattern,
        ZipFileHandler zipFileHandler, Map<String, String> variantToLicense, List<URI> supportedLicenses, DansBagValidator dansBagValidator, int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries, Path outboxDir) {

        super(
            datasetMetadataMapper, deposit, dataverseClient, depositorRole, fileExclusionPattern, zipFileHandler, variantToLicense, supportedLicenses, dansBagValidator,
            publishAwaitUnlockMillisecondsBetweenRetries, publishAwaitUnlockMaxNumberOfRetries, outboxDir);
    }

    @Override
    void checkDepositType() {
        var deposit = getDeposit();

        if (deposit.doi() == null || deposit.doi().isEmpty()) {
            throw new IllegalArgumentException("Deposit for migrated dataset MUST have deposit property identifier.doi set");
        }

            /*
            def checkMinimumFieldsForImport(): Try[Unit] = {
                val missing = new mutable.ListBuffer[String]()
            if (StringUtils.isBlank(dataversePid)) missing.append("dataversePid")
            if (StringUtils.isBlank(dataverseNbn)) missing.append("dataverseNbn")
            if (missing.nonEmpty) Failure(new RuntimeException(s"Not enough Data Vault Metadata for import deposit, missing: ${ missing.mkString(", ") }"))
            else Success(())
    }
             */
    }

    @Override
    DatasetCreator newDatasetCreator(Dataset dataset, String depositorRole) {
        // TODO this is ugly, and duplicates the parent; replace
        var deposit = getDeposit();

        // the only difference from the parent is the "isMigration" property
        return new DatasetCreator(dataverseClient, true, dataset, deposit, new ObjectMapper(), variantToLicense, supportedLicenses, publishAwaitUnlockMillisecondsBetweenRetries,
            publishAwaitUnlockMaxNumberOfRetries, fileExclusionPattern.getOrElse(() -> null), zipFileHandler, depositorRole);
    }

    @Override
    DatasetEditor newDatasetUpdater(Dataset dataset) {
        // TODO deduplicate all this logic
        var deposit = getDeposit();
        var blocks = dataset.getDatasetVersion().getMetadataBlocks();

        return new DatasetUpdater(dataverseClient, true, dataset, deposit, variantToLicense, supportedLicenses, publishAwaitUnlockMillisecondsBetweenRetries,
            publishAwaitUnlockMaxNumberOfRetries,
            fileExclusionPattern.getOrElse(() -> null), zipFileHandler, new ObjectMapper(), blocks);
    }

    @Override
    void checkPersonalDataPresent(Try<Option<Node>> optionTry) {
        if (optionTry.get().isEmpty()) {
            throw new RejectedDepositException(getDeposit(), "Migration deposit MUST have an agreements.xml", null);
        }
    }

    @Override
    Option<String> getDateOfDeposit() {
        var deposit = getDeposit();
        var amd = deposit.tryOptAmd().get();
        return amd.flatMap(Amd::toDateOfDeposit);
    }

    @Override
    void publishDataset(String persistentId) throws Exception {
        try {
            var deposit = getDeposit();
            var amd = deposit.tryOptAmd();

            if (amd.isFailure()) {
                throw new Exception(String.format("no AMD found for %s", persistentId));
            }

            var date = amd.get().flatMap(Amd::toPublicationDate);

            if (date.isEmpty()) {
                throw new IllegalArgumentException(String.format("no publication date found in AMD for %s", persistentId));
            }

            var dataset = dataverseClient.dataset(persistentId);

            dataset.releaseMigrated(date.get(), true);
            dataset.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);
        }
        catch (IOException | DataverseException e) {
            log.error("Unable to publish dataset", e);
        }
    }

    @Override
    void postPublication(String persistentId) {
        // do nothing
    }
}

