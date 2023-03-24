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
package nl.knaw.dans.ingest.core.validation;

import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.exception.DepositorValidatorException;
import nl.knaw.dans.ingest.core.exception.InvalidDepositorRoleException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DepositorAuthorizationValidatorImplTest {

    @Test
    void validateDepositorAuthorization_should_not_throw_when_creator_role_matches() throws Exception {
        var datasetService = Mockito.mock(DatasetService.class);
        Mockito.when(datasetService.getDataverseRoleAssignments(Mockito.eq("user001")))
            .thenReturn(List.of("admin", "creator"));

        var validator = new DepositorAuthorizationValidatorImpl(datasetService, "creator", "updater");

        var deposit = new Deposit();
        deposit.setDepositorUserId("user001");
        deposit.setUpdate(false);

        assertDoesNotThrow(() -> validator.validateDepositorAuthorization(deposit));
    }

    @Test
    void validateDepositorAuthorization_should_not_throw_when_updater_role_matches() throws Exception {
        var datasetService = Mockito.mock(DatasetService.class);
        Mockito.when(datasetService.getDatasetRoleAssignments(Mockito.eq("user001"), Mockito.eq("dataset_id")))
            .thenReturn(List.of("updater"));

        var validator = new DepositorAuthorizationValidatorImpl(datasetService, "creator", "updater");

        var deposit = new Deposit();
        deposit.setDepositorUserId("user001");
        deposit.setUpdate(true);
        deposit.setDataverseDoi("dataset_id");

        assertDoesNotThrow(() -> validator.validateDepositorAuthorization(deposit));
    }

    @Test
    void validateDepositorAuthorization_should_throw_when_creator_role_is_missing() throws Exception {
        var datasetService = Mockito.mock(DatasetService.class);
        Mockito.when(datasetService.getDataverseRoleAssignments(Mockito.eq("user001")))
            .thenReturn(List.of("curator"));

        var validator = new DepositorAuthorizationValidatorImpl(datasetService, "creator", "updater");

        var deposit = new Deposit();
        deposit.setDepositorUserId("user001");
        deposit.setUpdate(false);

        assertThrows(InvalidDepositorRoleException.class,
            () -> validator.validateDepositorAuthorization(deposit));
    }

    @Test
    void validateDepositorAuthorization_should_throw_when_updater_role_is_missing() throws Exception {
        var datasetService = Mockito.mock(DatasetService.class);
        Mockito.when(datasetService.getDatasetRoleAssignments(Mockito.eq("user001"), Mockito.eq("dataset_id")))
            .thenReturn(List.of("curator"));

        var validator = new DepositorAuthorizationValidatorImpl(datasetService, "creator", "updater");

        var deposit = new Deposit();
        deposit.setDepositorUserId("user001");
        deposit.setUpdate(true);
        deposit.setDataverseDoi("dataset_id");

        assertThrows(InvalidDepositorRoleException.class,
            () -> validator.validateDepositorAuthorization(deposit));
    }

    @Test
    void validateDepositorAuthorization_should_throw_DepositorValidatorException_when_IOException_occurs() throws Exception {
        var datasetService = Mockito.mock(DatasetService.class);
        Mockito.doThrow(IOException.class)
            .when(datasetService)
            .getDataverseRoleAssignments(Mockito.any());

        var validator = new DepositorAuthorizationValidatorImpl(datasetService, "creator", "updater");

        var deposit = new Deposit();
        deposit.setDepositorUserId("user001");
        deposit.setUpdate(false);

        assertThrows(DepositorValidatorException.class,
            () -> validator.validateDepositorAuthorization(deposit));
    }
}