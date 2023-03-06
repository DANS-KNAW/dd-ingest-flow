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
import nl.knaw.dans.lib.dataverse.DataverseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DepositorAuthorizationValidatorImpl implements DepositorAuthorizationValidator {
    private static final Logger log = LoggerFactory.getLogger(DepositorAuthorizationValidatorImpl.class);

    private final DatasetService datasetService;
    private final String datasetCreatorRole;
    private final String datasetUpdaterRole;

    public DepositorAuthorizationValidatorImpl(DatasetService datasetService, String datasetCreatorRole, String datasetUpdaterRole) {
        this.datasetService = datasetService;
        this.datasetCreatorRole = datasetCreatorRole;
        this.datasetUpdaterRole = datasetUpdaterRole;
    }

    @Override
    public void validateDepositorAuthorization(Deposit deposit) throws InvalidDepositorRoleException, DepositorValidatorException {
        if (deposit.isUpdate()) {
            validateUpdaterRoles(deposit);
        }
        else {
            validateCreatorRoles(deposit);
        }
    }

    void validateCreatorRoles(Deposit deposit) throws InvalidDepositorRoleException, DepositorValidatorException {
        try {
            var roles = datasetService.getDataverseRoleAssignments(deposit.getDepositorUserId());
            log.debug("Roles for user {}: {}; expecting role {} to be present", deposit.getDepositorUserId(), roles, datasetCreatorRole);

            if (!roles.contains(datasetCreatorRole)) {
                throw new InvalidDepositorRoleException(String.format(
                    "Depositor %s does not have role %s on dataverse root", deposit.getDepositorUserId(), datasetCreatorRole
                ));
            }
        }
        catch (DataverseException | IOException e) {
            throw new DepositorValidatorException(e);
        }
    }

    void validateUpdaterRoles(Deposit deposit) throws InvalidDepositorRoleException, DepositorValidatorException {
        try {
            var doi = deposit.getDataverseDoi();
            var roles = datasetService.getDatasetRoleAssignments(deposit.getDepositorUserId(), doi);
            log.debug("Roles for user {} on deposit with doi {}: {}; expecting role {} to be present", deposit.getDepositorUserId(), doi, roles, datasetUpdaterRole);

            if (!roles.contains(datasetUpdaterRole)) {
                throw new InvalidDepositorRoleException(String.format(
                    "Depositor %s does not have role %s on dataset doi:%s", deposit.getDepositorUserId(), datasetUpdaterRole, doi
                ));
            }
        }
        catch (DataverseException | IOException e) {
            throw new DepositorValidatorException(e);
        }
    }
}
