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

import nl.knaw.dans.ingest.core.TaskEvent.EventType;
import nl.knaw.dans.ingest.core.TaskEvent.Result;
import nl.knaw.dans.ingest.core.service.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.validatedansbag.api.ValidateOk;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DepositIngestTaskTest {

    final BlockedTargetService blockedTargetService = Mockito.mock(BlockedTargetService.class);
    final DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory = Mockito.mock(DepositToDvDatasetMetadataMapperFactory.class);
    final DataverseClient dataverseClient = Mockito.mock(DataverseClient.class);
    final ZipFileHandler zipFileHandler = Mockito.mock(ZipFileHandler.class);
    final DansBagValidator dansBagValidator = Mockito.mock(DansBagValidator.class);
    final EventWriter eventWriter = Mockito.mock(EventWriter.class);
    final DepositManager depositManager = Mockito.mock(DepositManager.class);

    @BeforeEach
    void setUp() {
        Mockito.reset(blockedTargetService);
        Mockito.reset(depositToDvDatasetMetadataMapperFactory);
        Mockito.reset(dataverseClient);
        Mockito.reset(zipFileHandler);
        Mockito.reset(dansBagValidator);
        Mockito.reset(eventWriter);
        Mockito.reset(depositManager);
        Mockito.reset(blockedTargetService);
    }

    DepositIngestTask getDepositIngestTask(String doi, String depositId, String isVersionOf) {
        var deposit = new Deposit();
        deposit.setDataverseDoi(doi);
        deposit.setDir(Path.of("path/to/", depositId));
        deposit.setIsVersionOf(isVersionOf);
        deposit.setUpdate(isVersionOf != null);

        var validateOk = new ValidateOk();
        validateOk.setIsCompliant(true);
        validateOk.setRuleViolations(List.of());

        Mockito.when(dansBagValidator.validateBag(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(validateOk);

        return new DepositIngestTask(
            depositToDvDatasetMetadataMapperFactory,
            deposit,
            dataverseClient,
            "role",
            null,
            zipFileHandler,
            Map.of(),
            List.of(),
            dansBagValidator,
            1000,
            1000,
            Path.of("outbox"),
            eventWriter,
            depositManager,
            blockedTargetService
        );
    }

    @Test
    void run_should_fail_deposit_if_isBlocked_returns_true_and_not_write_new_blocked_record_to_database() throws Exception {
        var depositId = UUID.fromString("4466a9d0-b835-4bff-81e2-ef104f8195d0");
        var task = getDepositIngestTask("doi:id", depositId.toString(), "version1");

        Mockito.doReturn(true)
            .when(blockedTargetService)
            .isBlocked(Mockito.anyString());

        try (var manifestHelpers = Mockito.mockStatic(ManifestHelper.class)) {
            manifestHelpers.when(() -> ManifestHelper.ensureSha1ManifestPresent(Mockito.any()))
                .thenAnswer(invocationOnMock -> invocationOnMock);

            var spiedTask = Mockito.spy(task);

            Mockito.doNothing()
                .when(spiedTask)
                .createOrUpdateDataset(Mockito.anyBoolean());

            Mockito.doReturn("doi:id")
                .when(spiedTask).resolveDoi(Mockito.any());

            spiedTask.run();
        }

        Mockito.verify(eventWriter)
            .write(depositId, EventType.END_PROCESSING, Result.FAILED, "Deposit with id 4466a9d0-b835-4bff-81e2-ef104f8195d0 and target doi:id is blocked by a previous deposit");

        Mockito.verify(blockedTargetService).isBlocked("doi:id");
        Mockito.verifyNoMoreInteractions(blockedTargetService);
    }

    @Test
    void run_should_block_deposit_if_deposit_is_rejected() throws Exception {
        var depositId = UUID.fromString("4466a9d0-b835-4bff-81e2-ef104f8195d0");
        var task = getDepositIngestTask("doi:id", depositId.toString(), "version1");

        var spiedTask = Mockito.spy(task);
        Mockito.doThrow(RejectedDepositException.class)
            .when(spiedTask).validateDeposit();

        Mockito.doNothing()
            .when(spiedTask)
            .createOrUpdateDataset(Mockito.anyBoolean());

        Mockito.doReturn("doi:id")
            .when(spiedTask).resolveDoi(Mockito.any());

        spiedTask.run();

        Mockito.verify(blockedTargetService).isBlocked("doi:id");
        Mockito.verify(blockedTargetService).blockTarget(depositId.toString(), "doi:id", "REJECTED", null);
        Mockito.verifyNoMoreInteractions(blockedTargetService);
    }

    @Test
    void run_should_not_block_deposit_if_deposit_is_rejected_but_not_an_update_of_existing_deposit() throws Exception {
        var depositId = UUID.fromString("4466a9d0-b835-4bff-81e2-ef104f8195d0");
        // if the doi is null, it will be assumed to be a new deposit that has not been created in dataverse yet
        var task = getDepositIngestTask(null, depositId.toString(), null);

        var spiedTask = Mockito.spy(task);
        Mockito.doThrow(RejectedDepositException.class)
            .when(spiedTask).validateDeposit();

        Mockito.doNothing()
            .when(spiedTask)
            .createOrUpdateDataset(Mockito.anyBoolean());

        Mockito.doReturn("doi:id")
            .when(spiedTask).resolveDoi(Mockito.any());

        spiedTask.run();

        // it was rejected
        Mockito.verify(eventWriter)
            .write(depositId, EventType.END_PROCESSING, Result.REJECTED, null);

        // but blockedTargetService was never invoked
        Mockito.verifyNoInteractions(blockedTargetService);
    }

    @Test
    void run_should_not_block_deposit_if_deposit_is_ok() throws Exception {
        var depositId = UUID.fromString("4466a9d0-b835-4bff-81e2-ef104f8195d0");
        // if the doi is null, it will be assumed to be a new deposit that has not been created in dataverse yet
        var task = getDepositIngestTask(null, depositId.toString(), null);

        var spiedTask = Mockito.spy(task);

        Mockito.doNothing()
            .when(spiedTask)
            .createOrUpdateDataset(Mockito.anyBoolean());

        Mockito.doReturn("doi:id")
            .when(spiedTask).resolveDoi(Mockito.any());

        try (var manifestHelpers = Mockito.mockStatic(ManifestHelper.class)) {
            manifestHelpers.when(() -> ManifestHelper.ensureSha1ManifestPresent(Mockito.any()))
                .thenAnswer(invocationOnMock -> invocationOnMock);

            spiedTask.run();
        }

        // it was successful
        Mockito.verify(eventWriter)
            .write(depositId, EventType.END_PROCESSING, Result.OK, null);

        // and blockedTargetService was never invoked
        Mockito.verifyNoInteractions(blockedTargetService);
    }
}
