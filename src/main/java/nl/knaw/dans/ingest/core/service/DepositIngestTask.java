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
import nl.knaw.dans.validatedansbag.api.ValidateCommand;
import nl.knaw.dans.ingest.core.DepositState;
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.sequencing.TargetedTask;
import nl.knaw.dans.ingest.core.service.exception.FailedDepositException;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import nl.knaw.dans.ingest.core.service.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import nl.knaw.dans.lib.dataverse.model.user.AuthenticatedUser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DepositIngestTask implements TargetedTask, Comparable<DepositIngestTask> {

    private static final Logger log = LoggerFactory.getLogger(DepositIngestTask.class);
    protected final DataverseClient dataverseClient;
    protected final String depositorRole;
    protected final Pattern fileExclusionPattern;
    protected final ZipFileHandler zipFileHandler;
    protected final Map<String, String> variantToLicense;
    protected final List<URI> supportedLicenses;
    protected final DansBagValidator dansBagValidator;
    protected final DepositToDvDatasetMetadataMapperFactory datasetMetadataMapperFactory;
    protected final int publishAwaitUnlockMillisecondsBetweenRetries;
    protected final int publishAwaitUnlockMaxNumberOfRetries;
    protected final Path outboxDir;
    private final Deposit deposit;

    private final EventWriter eventWriter;

    private final DepositManager depositManager;

    public DepositIngestTask(
        DepositToDvDatasetMetadataMapperFactory datasetMetadataMapperFactory,
        Deposit deposit,
        DataverseClient dataverseClient,
        String depositorRole,
        Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler,
        Map<String, String> variantToLicense,
        List<URI> supportedLicenses,
        DansBagValidator dansBagValidator,
        int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries,
        Path outboxDir,
        EventWriter eventWriter,
        DepositManager depositManager
    ) {
        this.datasetMetadataMapperFactory = datasetMetadataMapperFactory;
        this.deposit = deposit;
        this.dataverseClient = dataverseClient;

        this.depositorRole = depositorRole;
        this.fileExclusionPattern = fileExclusionPattern;

        this.zipFileHandler = zipFileHandler;
        this.variantToLicense = variantToLicense;
        this.supportedLicenses = supportedLicenses;
        this.dansBagValidator = dansBagValidator;
        this.publishAwaitUnlockMillisecondsBetweenRetries = publishAwaitUnlockMillisecondsBetweenRetries;
        this.publishAwaitUnlockMaxNumberOfRetries = publishAwaitUnlockMaxNumberOfRetries;
        this.outboxDir = outboxDir;
        this.eventWriter = eventWriter;
        this.depositManager = depositManager;
    }

    public Deposit getDeposit() {
        return this.deposit;
    }

    @Override
    public void run() {
        writeEvent(TaskEvent.EventType.START_PROCESSING, TaskEvent.Result.OK, null);

        try {
            doRun();
            updateDepositFromResult(DepositState.PUBLISHED, "The deposit was successfully ingested in the Data Station and will be automatically archived");
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.OK, null);
        }
        catch (RejectedDepositException e) {
            log.error("deposit was rejected", e);
            updateDepositFromResult(DepositState.REJECTED, e.getMessage());
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.REJECTED, e.getMessage());
        }
        catch (Throwable e) {
            log.error("deposit failed", e);
            updateDepositFromResult(DepositState.FAILED, e.getMessage());
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.FAILED, e.getMessage());
        }
    }

    void moveDepositToOutbox(OutboxSubDir subDir) throws IOException {
        var deposit = getDeposit();
        var target = this.outboxDir.resolve(subDir.getValue())
            .resolve(deposit.getDir().getFileName());

        Files.move(deposit.getDir(), target);
    }

    void updateDepositFromResult(DepositState depositState, String message) {
        deposit.setState(depositState);
        deposit.setStateDescription(message);

        try {
            depositManager.saveDeposit(deposit);

            switch (depositState) {
                case PUBLISHED:
                    moveDepositToOutbox(OutboxSubDir.PROCESSED);
                    break;
                case REJECTED:
                    moveDepositToOutbox(OutboxSubDir.REJECTED);
                    break;
                case FAILED:
                    moveDepositToOutbox(OutboxSubDir.FAILED);
                    break;
            }
        }
        catch (IOException e) {
            log.error("Unable to move directory for deposit {}", deposit.getDir(), e);
        }
        catch (InvalidDepositException e) {
            log.error("Unable to save deposit for deposit {}", deposit.getDir(), e);
        }
    }

    @Override
    public String getTarget() {
        return Optional.ofNullable(getDeposit().getDoi()).orElse("");
    }

    @Override
    public void writeEvent(TaskEvent.EventType eventType, TaskEvent.Result result, String message) {
        eventWriter.write(getDepositId(), eventType, result, message);
    }

    private UUID getDepositId() {
        return UUID.fromString(deposit.getDepositId());
    }

    void doRun() throws Exception {
        // do some checks
        checkDepositType();
        validateDeposit();

        var deposit = getDeposit();
        // get metadata
        var dataverseDataset = getMetadata();
        var isUpdate = deposit.isUpdate();

        log.debug("Is update: {}", isUpdate);

        var persistentId = isUpdate
            ? newDatasetUpdater(dataverseDataset).performEdit()
            : newDatasetCreator(dataverseDataset, depositorRole).performEdit();

        publishDataset(persistentId);
        postPublication(persistentId);
    }

    void checkDepositType() {
        var deposit = getDeposit();
        var hasDoi = StringUtils.isNotBlank(deposit.getDoi());

        if (hasDoi) {
            throw new IllegalArgumentException("Deposits must not have an identifier.doi property unless they are migrated");
        }
    }

    void validateDeposit() {

        var deposit = getDeposit();

        if (dansBagValidator != null) {
            var result = dansBagValidator.validateBag(
                deposit.getBagDir(), ValidateCommand.PackageTypeEnum.DEPOSIT, 1);

            if (!result.getIsCompliant()) {
                var violations = result.getRuleViolations().stream()
                    .map(r -> String.format("- [%s] %s", r.getRule(), r.getViolation()))
                    .collect(Collectors.joining("\n"));

                throw new RejectedDepositException(deposit, String.format(
                    "Bag was not valid according to Profile Version %s. Violations: %s",
                    result.getProfileVersion(), violations)
                );
            }
        }
    }

    void postPublication(String persistentId) throws IOException, DataverseException, InterruptedException {
        waitForReleasedState(persistentId);
        savePersistentIdentifiersInDepositProperties(persistentId);
    }

    void savePersistentIdentifiersInDepositProperties(String persistentId) throws IOException, DataverseException {
        var dataset = dataverseClient.dataset(persistentId);
        var deposit = getDeposit();
        dataset.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);

        deposit.setDoi(persistentId);

        var version = dataset.getVersion();
        var data = version.getData();
        var metadata = data.getMetadataBlocks().get("dansDataVaultMetadata");

        var urn = metadata.getFields().stream()
            .filter(f -> f.getTypeName().equals("dansNbn"))
            .map(f -> (PrimitiveSingleValueField) f)
            .map(PrimitiveSingleValueField::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(String.format("Dataset %s did not obtain a URN:NBN", persistentId)));

        deposit.setUrn(urn);
    }

    String getDatasetState(String persistentId) throws IOException, DataverseException {
        var version = dataverseClient.dataset(persistentId).getLatestVersion();
        return version.getData().getLatestVersion().getVersionState();
    }

    void waitForReleasedState(String persistentId) throws InterruptedException, IOException, DataverseException {
        var numberOfTimesTried = 0;
        var state = getDatasetState(persistentId);
        var deposit = getDeposit();

        while (!"RELEASED".equals(state) && numberOfTimesTried < publishAwaitUnlockMaxNumberOfRetries) {
            Thread.sleep(publishAwaitUnlockMillisecondsBetweenRetries);

            state = getDatasetState(persistentId);
            numberOfTimesTried += 1;
        }

        if (!"RELEASED".equals(state)) {
            throw new FailedDepositException(deposit, "Dataset did not become RELEASED within the wait period");
        }
    }

    void publishDataset(String persistentId) throws Exception {

        try {
            var dataset = dataverseClient.dataset(persistentId);

            dataset.publish(UpdateType.major, true);
            dataset.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);
        }
        catch (IOException | DataverseException e) {
            log.error("Unable to publish dataset", e);
        }
    }

    DatasetEditor newDatasetUpdater(Dataset dataset, boolean isMigration) {
        var deposit = getDeposit();
        var blocks = dataset.getDatasetVersion().getMetadataBlocks();

        return new DatasetUpdater(
            dataverseClient,
            isMigration,
            dataset,
            deposit,
            variantToLicense,
            supportedLicenses,
            publishAwaitUnlockMillisecondsBetweenRetries,
            publishAwaitUnlockMaxNumberOfRetries,
            fileExclusionPattern,
            zipFileHandler,
            new ObjectMapper(),
            blocks
        );
    }

    DatasetEditor newDatasetUpdater(Dataset dataset) {
        return newDatasetUpdater(dataset, false);
    }

    DatasetEditor newDatasetCreator(Dataset dataset, String depositorRole, boolean isMigration) {
        var deposit = getDeposit();

        return new DatasetCreator(
            dataverseClient,
            isMigration,
            dataset,
            deposit,
            new ObjectMapper(),
            variantToLicense,
            supportedLicenses,
            publishAwaitUnlockMillisecondsBetweenRetries,
            publishAwaitUnlockMaxNumberOfRetries,
            fileExclusionPattern,
            zipFileHandler,
            depositorRole
        );
    }

    DatasetEditor newDatasetCreator(Dataset dataset, String depositorRole) {
        return newDatasetCreator(dataset, depositorRole, false);
    }

    Dataset getMetadata() {
        var date = getDateOfDeposit();
        var contact = getDatasetContact();
        var deposit = getDeposit();

        // TODO think about putting assertions inside a getter method
        checkPersonalDataPresent(deposit.getAgreements());

        var mapper = datasetMetadataMapperFactory.createMapper(false);

        return mapper.toDataverseDataset(
            deposit.getDdm(),
            deposit.getOtherDoiId(),
            deposit.getAgreements(),
            date.orElse(null),
            contact.orElse(null),
            deposit.getVaultMetadata()
        );
    }

    void checkPersonalDataPresent(Document agreements) {
        // do nothing
    }

    Optional<String> getDateOfDeposit() {
        return Optional.empty();
    }

    Optional<AuthenticatedUser> getDatasetContact() {
        var deposit = getDeposit();
        return Optional.ofNullable(deposit.getDepositorUserId())
            .filter(StringUtils::isNotBlank)
            .map(userId -> {
                try {
                    return dataverseClient.admin().listSingleUser(userId).getData();
                }
                catch (IOException | DataverseException e) {
                    log.error("Unable to fetch user with id {}", userId, e);
                    throw new RuntimeException(e);
                }
            });
    }

    @Override
    public int compareTo(DepositIngestTask depositIngestTask) {
        return getCreatedInstant().compareTo(depositIngestTask.getCreatedInstant());
    }

    protected Instant getCreatedInstant() {
        return getDeposit().getBagCreated();
    }
}
