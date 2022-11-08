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

import better.files.File;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.DepositToDvDatasetMetadataMapper;
import nl.knaw.dans.easy.dd2d.FailedDepositException;
import nl.knaw.dans.easy.dd2d.RejectedDepositException;
import nl.knaw.dans.ingest.api.ValidateCommand;
import nl.knaw.dans.ingest.core.DepositState;
import nl.knaw.dans.ingest.core.TaskEvent;
import nl.knaw.dans.ingest.core.sequencing.TargetedTask;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.util.Try;
import scala.xml.Node;

import java.io.IOException;
import java.net.URI;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DepositIngestTask implements TargetedTask {

    private static final Logger log = LoggerFactory.getLogger(DepositIngestTask.class);
    protected final DataverseClient dataverseClient;
    protected final String depositorRole;
    protected final Option<Pattern> fileExclusionPattern;
    //    private val datasetMetadataMapper = new DepositToDvDatasetMetadataMapper(deduplicate, activeMetadataBlocks, narcisClassification, iso1ToDataverseLanguage, iso2ToDataverseLanguage, repordIdToTerm)
    protected final ZipFileHandler zipFileHandler;
    protected final Map<String, String> variantToLicense;
    protected final List<URI> supportedLicenses;
    protected final DansBagValidator dansBagValidator;
    protected final DepositToDvDatasetMetadataMapper datasetMetadataMapper;
    protected final int publishAwaitUnlockMillisecondsBetweenRetries;
    protected final int publishAwaitUnlockMaxNumberOfRetries;
    protected final Path outboxDir;
    private final Deposit deposit;

    public DepositIngestTask(DepositToDvDatasetMetadataMapper datasetMetadataMapper, Deposit deposit, DataverseClient dataverseClient, String depositorRole, Option<Pattern> fileExclusionPattern,
        ZipFileHandler zipFileHandler, Map<String, String> variantToLicense, List<URI> supportedLicenses, DansBagValidator dansBagValidator, int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries, Path outboxDir) {
        this.datasetMetadataMapper = datasetMetadataMapper;
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
    }

    protected Deposit getDeposit() {
        return this.deposit;
    }

    @Override
    public void run() {
        try {
            doRun();
            updateDepositFromResult(DepositState.ARCHIVED, "The deposit was successfully ingested in the Data Station and will be automatically archived");
        }
        catch (RejectedDepositException e) {
            log.error("deposit was rejected", e);
            updateDepositFromResult(DepositState.REJECTED, e.msg());
        }
        catch (Exception e) {
            log.error("deposit failed", e);
            updateDepositFromResult(DepositState.FAILED, e.getMessage());
        }
    }

    void moveDepositToOutbox(OutboxSubDir subDir) {

        var deposit = getDeposit();
        var target = this.outboxDir.resolve(subDir.getValue());

        var linkOptions = JavaConverters.asScalaBuffer(new ArrayList<LinkOption>()).toSeq();
        deposit.dir().moveToDirectory(File.apply(target), linkOptions);
    }

    void updateDepositFromResult(DepositState depositState, String message) {
        deposit.setState(depositState.toString(), message);

        switch (depositState) {
            case ARCHIVED:
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

    @Override
    public String getTarget() {
        return getDeposit().doi();
    }

    @Override
    public void writeEvent(TaskEvent.EventType eventType, TaskEvent.Result result, String message) {

    }

    void doRun() throws Exception {
        // do some checks
        checkDepositType();
        validateDeposit();

        var deposit = getDeposit();
        // get metadata
        var dataverseDataset = getMetadata();
        var isUpdate = (boolean) deposit.isUpdate().get(); //.equals(Boolean.box(true));

        String persistentId = null;

        if (isUpdate) {
            persistentId = newDatasetUpdater(dataverseDataset).performEdit();
        }
        else {
            persistentId = newDatasetCreator(dataverseDataset, depositorRole).performEdit();
        }
        //        var editor = isUpdate ? newDatasetUpdater(dataverseDataset) : newDatasetCreator(dataverseDataset, depositorRole);
        //        var persistentId = editor.performEdit().get();

        publishDataset(persistentId);
        postPublication(persistentId);

        /*

    trace(())
    logger.info(s"Ingesting $deposit into Dataverse")
    for {
      _ <- checkDepositType()
      _ <- validateDeposit()
      dataverseDataset <- getMetadata
      isUpdate <- deposit.isUpdate
      _ = debug(s"isUpdate? = $isUpdate")
      editor = if (isUpdate) newDatasetUpdater(dataverseDataset)
               else newDatasetCreator(dataverseDataset, depositorRole)
      persistentId <- editor.performEdit()
      _ <- publishDataset(persistentId)
      _ <- postPublication(persistentId)
    } yield ()
         */

    }

    void checkDepositType() {
        var deposit = getDeposit();
        var hasDoi = Optional.ofNullable(deposit.doi())
            .map(d -> !d.isEmpty())
            .orElse(false);

        if (hasDoi) {
            throw new IllegalArgumentException("Deposits must not have an identifier.doi property unless they are migrated");
        }
    }

    void validateDeposit() {

        var deposit = getDeposit();

        if (dansBagValidator != null) {
            var result = dansBagValidator.validateBag(
                deposit.bagDir().path(), ValidateCommand.PackageTypeEnum.DEPOSIT, 1);

            if (!result.getIsCompliant()) {
                var violations = result.getRuleViolations().stream()
                    .map(r -> String.format("- [%s] %s", r.getRule(), r.getViolation()))
                    .collect(Collectors.joining("\n"));

                throw new RejectedDepositException(deposit, String.format(
                    "Bag was not valid according to Profile Version %s. Violations: %s",
                    result.getProfileVersion(), violations), null
                );
            }
        }
    }

    void postPublication(String persistentId) throws IOException, DataverseException, InterruptedException {
        waitForReleasedState(persistentId);
        savePersistentIdentifiersInDepositProperties(persistentId);
        /*
    trace(persistentId)
    for {
      _ <- waitForReleasedState(persistentId)
      _ <- savePersistentIdentifiersInDepositProperties(persistentId)
    } yield ()
         */
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
            throw new FailedDepositException(deposit, "Dataset did not become RELEASED within the wait period", null);
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

    DatasetEditor newDatasetUpdater(Dataset dataset) {
//        var tuples = variantToLicense.entrySet().stream()
//            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
//            .collect(Collectors.toList());
//
//        var scalaVariantToLicense = (scala.collection.immutable.Map<String, String>) Map$.MODULE$.apply(JavaConverters.asScalaBuffer(tuples).toSeq());
//        var scalaSupportedLicenses = JavaConverters.asScalaBuffer(supportedLicenses).toList();
        var deposit = getDeposit();
        var blocks = dataset.getDatasetVersion().getMetadataBlocks();

        return new DatasetUpdater(dataverseClient, false, dataset, deposit, variantToLicense, supportedLicenses, publishAwaitUnlockMillisecondsBetweenRetries, publishAwaitUnlockMaxNumberOfRetries,
            fileExclusionPattern.getOrElse(() -> null), zipFileHandler, new ObjectMapper(), blocks);
        //        return new DatasetUpdater(deposit, fileExclusionPattern, zipFileHandler, false, dataset.getDatasetVersion().getMetadataBlocks(), scalaVariantToLicense, scalaSupportedLicenses,
        //            dataverseClient, blocks);
    }

    DatasetEditor newDatasetCreator(Dataset dataset, String depositorRole) {
//        var tuples = variantToLicense.entrySet().stream()
//            .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
//            .collect(Collectors.toList());
//
//        var scalaVariantToLicense = (scala.collection.immutable.Map<String, String>) Map$.MODULE$.apply(JavaConverters.asScalaBuffer(tuples).toSeq());
//        var scalaSupportedLicenses = JavaConverters.asScalaBuffer(supportedLicenses).toList();
        var deposit = getDeposit();

        return new DatasetCreator(dataverseClient, false, dataset, deposit, new ObjectMapper(), variantToLicense, supportedLicenses, publishAwaitUnlockMillisecondsBetweenRetries,
            publishAwaitUnlockMaxNumberOfRetries, fileExclusionPattern.getOrElse(() -> null), zipFileHandler, depositorRole);
        //        return new DatasetCreator(deposit, fileExclusionPattern, zipFileHandler, depositorRole, false, dataset, scalaVariantToLicense, scalaSupportedLicenses, dataverseClient);
    }

    Dataset getMetadata() throws IOException, DataverseException {

        /*

      optDateOfDeposit <- getDateOfDeposit
      datasetContacts <- getDatasetContacts
      ddm <- deposit.tryDdm
      optAgreements <- deposit.tryOptAgreementsXml
      _ <- checkPersonalDataPresent(optAgreements)
      dataverseDataset <- datasetMetadataMapper.toDataverseDataset(ddm, deposit.getOptOtherDoiId, optAgreements, optDateOfDeposit, datasetContacts, deposit.vaultMetadata)
         */
        var date = getDateOfDeposit();
        var contacts = getDatasetContacts();
        var deposit = getDeposit();

        // TODO think about putting assertions inside a getter method
        checkPersonalDataPresent(deposit.tryOptAgreementsXml());

        return datasetMetadataMapper.toDataverseDataset(
            deposit.tryDdm().get(),
            deposit.getOptOtherDoiId(),
            deposit.tryOptAgreementsXml().get(),
            date,
            contacts,
            deposit.vaultMetadata()
        ).get();
    }

    void checkPersonalDataPresent(Try<Option<Node>> optionTry) {
        // do nothing
    }

    // TODO convert to Optional
    Option<String> getDateOfDeposit() {
        return Option.empty();
    }

    // TODO convert to java versions
    scala.collection.immutable.List<Map<String, MetadataField>> getDatasetContacts() throws IOException, DataverseException {
        var deposit = getDeposit();
        var user = dataverseClient.admin().listSingleUser(deposit.depositorUserId()).getData();
        var contacts = createDatasetContacts(user.getDisplayName(), user.getEmail(), user.getAffiliation());

        for (var x : contacts) {
            System.out.println("X: " + x);
        }
        return JavaConverters.asScalaBuffer(createDatasetContacts(user.getDisplayName(), user.getEmail(), user.getAffiliation())).toList();
    }

    List<Map<String, MetadataField>> createDatasetContacts(String displayName, String email, String affiliation) {
        var fields = new ArrayList<MetadataField>();
        fields.add(new PrimitiveSingleValueField("datasetContactName", displayName));
        fields.add(new PrimitiveSingleValueField("datasetContactEmail", email));

        if (affiliation != null) {
            fields.add(new PrimitiveSingleValueField("datasetContactAffiliation", affiliation));
        }

        var result = new HashMap<String, MetadataField>();

        for (var field : fields) {
            result.put(field.getTypeName(), field);
        }

        return List.of(result);
    }
}
