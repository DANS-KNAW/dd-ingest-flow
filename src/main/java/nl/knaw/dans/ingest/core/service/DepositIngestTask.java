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
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.TaskEvent.Result;
import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.deposit.DepositManager;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.DepositLocation;
import nl.knaw.dans.ingest.core.domain.DepositState;
import nl.knaw.dans.ingest.core.domain.OutboxSubDir;
import nl.knaw.dans.ingest.core.exception.DepositorValidatorException;
import nl.knaw.dans.ingest.core.exception.FailedDepositException;
import nl.knaw.dans.ingest.core.exception.InvalidDatasetStateException;
import nl.knaw.dans.ingest.core.exception.InvalidDepositException;
import nl.knaw.dans.ingest.core.exception.InvalidDepositorRoleException;
import nl.knaw.dans.ingest.core.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.exception.TargetBlockedException;
import nl.knaw.dans.ingest.core.sequencing.TargetedTask;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.ingest.core.validation.DepositorAuthorizationValidator;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.user.AuthenticatedUser;
import nl.knaw.dans.validatedansbag.api.ValidateCommand;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DepositIngestTask implements TargetedTask, Comparable<DepositIngestTask> {

    private static final Logger log = LoggerFactory.getLogger(DepositIngestTask.class);
    protected final String depositorRole;
    protected final Pattern fileExclusionPattern;
    protected final ZipFileHandler zipFileHandler;
    protected final Map<String, String> variantToLicense;
    protected final List<URI> supportedLicenses;
    protected final DansBagValidator dansBagValidator;
    protected final DepositToDvDatasetMetadataMapperFactory datasetMetadataMapperFactory;
    protected final Path outboxDir;
    protected final DepositLocation depositLocation;
    protected final DatasetService datasetService;
    private final EventWriter eventWriter;

    private final DepositManager depositManager;
    private final BlockedTargetService blockedTargetService;

    private final DepositorAuthorizationValidator depositorAuthorizationValidator;
    protected Deposit deposit;

    public DepositIngestTask(
        DepositToDvDatasetMetadataMapperFactory datasetMetadataMapperFactory,
        DepositLocation depositLocation,
        String depositorRole,
        Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler,
        Map<String, String> variantToLicense,
        List<URI> supportedLicenses,
        DansBagValidator dansBagValidator,
        Path outboxDir,
        EventWriter eventWriter,
        DepositManager depositManager,
        DatasetService datasetService,
        BlockedTargetService blockedTargetService,
        DepositorAuthorizationValidator depositorAuthorizationValidator
    ) {
        this.datasetMetadataMapperFactory = datasetMetadataMapperFactory;

        this.depositorRole = depositorRole;
        this.fileExclusionPattern = fileExclusionPattern;

        this.zipFileHandler = zipFileHandler;
        this.variantToLicense = variantToLicense;
        this.supportedLicenses = supportedLicenses;
        this.dansBagValidator = dansBagValidator;
        this.outboxDir = outboxDir;
        this.eventWriter = eventWriter;
        this.depositManager = depositManager;
        this.blockedTargetService = blockedTargetService;
        this.depositLocation = depositLocation;
        this.datasetService = datasetService;
        this.depositorAuthorizationValidator = depositorAuthorizationValidator;
    }

    public Deposit getDeposit() {
        return this.deposit;
    }

    @Override
    public void run() {
        writeEvent(TaskEvent.EventType.START_PROCESSING, TaskEvent.Result.OK, null);

        // TODO this is really ugly, fix it at some point
        try {
            this.deposit = depositManager.readDeposit(depositLocation);
        }
        catch (InvalidDepositException e) {
            try {
                moveDepositToOutbox(depositLocation.getDir(), OutboxSubDir.FAILED);
            }
            catch (IOException ex) {
                log.error("Unable to move deposit directory to 'failed' outbox", ex);
            }

            writeEvent(TaskEvent.EventType.END_PROCESSING, Result.FAILED, e.getMessage());
            return;
        }

        try {
            doRun();
            updateDepositFromResult(DepositState.PUBLISHED, "The deposit was successfully ingested in the Data Station and will be automatically archived");
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.OK, null);
        }
        catch (RejectedDepositException e) {
            log.error("deposit was rejected", e);
            updateDepositFromResult(DepositState.REJECTED, e.getMessage());
            blockTarget(e.getMessage(), DepositState.REJECTED);
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.REJECTED, e.getMessage());
        }
        catch (TargetBlockedException e) {
            log.error("deposit was rejected because a previous deposit with the same target failed", e);
            updateDepositFromResult(DepositState.FAILED, e.getMessage());
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.FAILED, e.getMessage());
        }
        catch (Throwable e) {
            log.error("deposit failed", e);
            updateDepositFromResult(DepositState.FAILED, e.getMessage());
            blockTarget(e.getMessage(), DepositState.FAILED);
            writeEvent(TaskEvent.EventType.END_PROCESSING, TaskEvent.Result.FAILED, e.getMessage());
        }
    }

    void moveDepositToOutbox(Path path, OutboxSubDir subDir) throws IOException {
        var target = this.outboxDir.resolve(subDir.getValue());
        depositManager.moveDeposit(path, target);
    }

    void updateDepositFromResult(DepositState depositState, String message) {
        deposit.setState(depositState);
        deposit.setStateDescription(message);

        try {
            depositManager.updateAndMoveDeposit(deposit, getTargetPath(depositState));
        }
        catch (IOException e) {
            log.error("Unable to move directory for deposit {}", deposit.getDir(), e);
        }
        catch (InvalidDepositException e) {
            log.error("Unable to save deposit for deposit {}", deposit.getDir(), e);
        }
    }

    Path getTargetPath(DepositState depositState) {
        switch (depositState) {
            case PUBLISHED:
                return this.outboxDir.resolve(OutboxSubDir.PROCESSED.getValue());
            case REJECTED:
                return this.outboxDir.resolve(OutboxSubDir.REJECTED.getValue());
            case FAILED:
                return this.outboxDir.resolve(OutboxSubDir.FAILED.getValue());
            default:
                throw new IllegalArgumentException(String.format(
                    "Unexpected deposit state '%s' found; not sure where to move it",
                    depositState
                ));

        }
    }

    @Override
    public String getTarget() {
        return depositLocation.getTarget();
    }

    @Override
    public Path getDepositPath() {
        return depositLocation.getDir();
    }

    @Override
    public void writeEvent(TaskEvent.EventType eventType, TaskEvent.Result result, String message) {
        eventWriter.write(getDepositId(), eventType, result, message);
    }

    private UUID getDepositId() {
        return UUID.fromString(depositLocation.getDepositId());
    }

    void doRun() throws Exception {
        var deposit = getDeposit();
        var isUpdate = deposit.isUpdate();

        if (isUpdate) {
            log.debug("Figuring out the doi for deposit {}", deposit.getDepositId());
            var dataverseDoi = resolveDoi(deposit);
            deposit.setDataverseDoi(dataverseDoi);

            // only check for blocked targets if this is an update to an existing dataset
            checkBlockedTarget();
        }

        // do some checks
        checkDepositType();
        validateDepositorRoles();
        validateDeposit();

        // now create or update the dataset with dataverse
        createOrUpdateDataset(isUpdate);
    }

    void createOrUpdateDataset(boolean isUpdate) throws Exception {
        // get metadata
        var dataverseDataset = getMetadata();

        log.debug("Is update: {}", isUpdate);

        var persistentId = isUpdate
            ? newDatasetUpdater(dataverseDataset).performEdit()
            : newDatasetCreator(dataverseDataset, depositorRole).performEdit();

        publishDataset(persistentId);
        postPublication(persistentId);
    }

    void validateDepositorRoles() {
        try {
            depositorAuthorizationValidator.validateDepositorAuthorization(deposit);
        }
        catch (InvalidDepositorRoleException e) {
            throw new RejectedDepositException(deposit, e.getMessage());
        }
        catch (DepositorValidatorException e) {
            throw new FailedDepositException(deposit, e.getMessage());
        }
    }

    void checkBlockedTarget() throws TargetBlockedException {
        var deposit = getDeposit();

        if (!deposit.isUpdate()) {
            log.debug("Deposit is not an update, no need to check for previously failed versions");
            return;
        }

        var target = deposit.getDataverseDoi();
        var depositId = deposit.getDepositId();

        if (blockedTargetService.isBlocked(target)) {
            throw new TargetBlockedException(String.format(
                "Deposit with id %s and target %s is blocked by a previous deposit", depositId, target
            ));
        }
    }

    void blockTarget(String message, DepositState depositState) {
        var deposit = getDeposit();
        var target = deposit.getDataverseDoi();

        if (target == null) {
            log.warn("Target for deposit {} is null, unable to block target. This probably means it is the first version of a deposit and can be ignored", deposit);
            return;
        }

        try {
            blockedTargetService.blockTarget(
                deposit.getDepositId(),
                target,
                depositState.toString(),
                message
            );
        }
        catch (TargetBlockedException e) {
            log.warn("Target {} is already blocked", target);
        }
    }

    void checkDepositType() {
        var hasDoi = StringUtils.isNotBlank(deposit.getDoi());

        if (hasDoi) {
            throw new IllegalArgumentException("Deposits must not have an identifier.doi property unless they are migrated");
        }
    }

    void validateDeposit() {
        var result = dansBagValidator.validateBag(
            deposit.getBagDir(), ValidateCommand.PackageTypeEnum.DEPOSIT, 1);

        if (result.getIsCompliant()) {
            try {
                ManifestHelper.ensureSha1ManifestPresent(deposit.getBag());
            }
            catch (Exception e) {
                log.error("could not add SHA1 manifest", e);
                throw new FailedDepositException(deposit, e.getMessage());
            }
        }
        else {
            var violations = result.getRuleViolations().stream()
                .map(r -> String.format("- [%s] %s", r.getRule(), r.getViolation()))
                .collect(Collectors.joining("\n"));

            throw new RejectedDepositException(deposit, String.format(
                "Bag was not valid according to Profile Version %s. Violations: %s",
                result.getProfileVersion(), violations)
            );
        }
    }

    void postPublication(String persistentId) throws IOException, DataverseException, InterruptedException {
        try {
            datasetService.waitForState(persistentId, "RELEASED");
            savePersistentIdentifiersInDepositProperties(persistentId);
        }
        catch (InvalidDatasetStateException e) {
            throw new FailedDepositException(deposit, e.getMessage());
        }
    }

    void savePersistentIdentifiersInDepositProperties(String persistentId) throws IOException, DataverseException {
        var urn = datasetService.getDatasetUrnNbn(persistentId)
            .orElseThrow(() -> new IllegalStateException(String.format("Dataset %s did not obtain a URN:NBN", persistentId)));

        deposit.setDoi(persistentId);
        deposit.setUrn(urn);
    }

    void publishDataset(String persistentId) throws Exception {
        try {
            datasetService.publishDataset(persistentId);
        }
        catch (IOException | DataverseException e) {
            log.error("Unable to publish dataset", e);
            throw e;
        }
    }

    DatasetEditor newDatasetUpdater(Dataset dataset, boolean isMigration) {
        var blocks = dataset.getDatasetVersion().getMetadataBlocks();

        return new DatasetUpdater(
            isMigration,
            dataset,
            deposit,
            variantToLicense,
            supportedLicenses,
            fileExclusionPattern,
            zipFileHandler,
            new ObjectMapper(),
            blocks,
            datasetService
        );
    }

    DatasetEditor newDatasetUpdater(Dataset dataset) {
        return newDatasetUpdater(dataset, false);
    }

    DatasetEditor newDatasetCreator(Dataset dataset, String depositorRole, boolean isMigration) {
        return new DatasetCreator(
            isMigration,
            dataset,
            deposit,
            new ObjectMapper(),
            variantToLicense,
            supportedLicenses,
            fileExclusionPattern,
            zipFileHandler,
            depositorRole,
            datasetService
        );
    }

    DatasetEditor newDatasetCreator(Dataset dataset, String depositorRole) {
        return newDatasetCreator(dataset, depositorRole, false);
    }

    Dataset getMetadata() {
        var date = getDateOfDeposit();
        var contact = getDatasetContact();
        var accessibleToValues = XPathEvaluator
            .strings(deposit.getFilesXml(), "/files:files/files:file/files:accessibleToRights")
            .collect(Collectors.toList());

        var mapper = datasetMetadataMapperFactory.createMapper(false); // TODO: WHY IS THIS ALWAYS FALSE?
        return mapper.toDataverseDataset(
            deposit.getDdm(),
            deposit.getOtherDoiId(),
            date.orElse(null),
            contact.orElse(null),
            deposit.getVaultMetadata(),
            // TRM002
            accessibleToValues.contains("NONE"),
            accessibleToValues.contains("RESTRICTED_REQUEST") || accessibleToValues.contains("KNOWN"));
    }

    Optional<String> getDateOfDeposit() {
        return Optional.empty();
    }

    Optional<AuthenticatedUser> getDatasetContact() {
        return Optional.ofNullable(deposit.getDepositorUserId())
            .filter(StringUtils::isNotBlank)
            .map(userId -> datasetService.getUserById(userId)
                .orElseThrow(() -> new RuntimeException("Unable to fetch user with id " + userId)));
    }

    @Override
    public int compareTo(DepositIngestTask depositIngestTask) {
        return getCreatedInstant().compareTo(depositIngestTask.getCreatedInstant());
    }

    public OffsetDateTime getCreatedInstant() {
        return depositLocation.getCreated();
    }

    String resolveDoi(Deposit deposit) throws IOException, DataverseException {
        return getDoi("dansSwordToken", deposit.getVaultMetadata().getSwordToken());
    }

    String getDoi(String key, String value) throws IOException, DataverseException {
        var items = datasetService.searchDatasets(key, value);

        if (items.size() != 1) {
            throw new FailedDepositException(deposit, String.format(
                "Deposit is update of %s datasets; should always be 1!", items.size()
            ), null);
        }

        var doi = items.get(0).getGlobalId();
        log.debug("Deposit is update of dataset {}", doi);
        return doi;
    }
}
