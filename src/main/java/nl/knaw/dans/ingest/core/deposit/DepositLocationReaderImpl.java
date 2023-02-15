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
package nl.knaw.dans.ingest.core.deposit;

import gov.loc.repository.bagit.domain.Metadata;
import gov.loc.repository.bagit.exceptions.InvalidBagitFileFormatException;
import gov.loc.repository.bagit.exceptions.UnparsableVersionException;
import nl.knaw.dans.ingest.core.domain.DepositLocation;
import nl.knaw.dans.ingest.core.exception.InvalidDepositException;
import nl.knaw.dans.ingest.core.exception.MissingTargetException;
import nl.knaw.dans.ingest.core.io.BagDataManager;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.UUID;

public class DepositLocationReaderImpl implements DepositLocationReader {
    private final BagDirResolver bagDirResolver;

    private final BagDataManager bagDataManager;

    public DepositLocationReaderImpl(BagDirResolver bagDirResolver, BagDataManager bagDataManager) {
        this.bagDirResolver = bagDirResolver;
        this.bagDataManager = bagDataManager;
    }

    @Override
    public DepositLocation readDepositLocation(Path depositDir) throws InvalidDepositException, IOException {
        var bagDir = bagDirResolver.getBagDir(depositDir);

        try {
            var properties = bagDataManager.readDepositProperties(depositDir);
            var bagInfo = bagDataManager.readBagMetadata(bagDir);
            var depositId = getDepositId(depositDir);
            var target = getTarget(properties);
            var created = getCreated(bagInfo);

            return new DepositLocation(depositDir, target, depositId.toString(), created);
        }
        catch (ConfigurationException e) {
            throw new InvalidDepositException("Deposit.properties file could not be read", e);
        }
        catch (MissingTargetException e) {
            throw new InvalidDepositException(e.getMessage(), e);
        }
        catch (UnparsableVersionException | InvalidBagitFileFormatException | IOException e) {
            throw new InvalidDepositException("BagIt file(s) could not be read", e);
        }
    }

    String getTarget(Configuration properties) throws MissingTargetException {
        // the logic for the target should be
        // 1. if there is a dataverse.sword-token, use that
        // 2. otherwise, use identifier.doi
        var target = properties.getString("dataverse.sword-token");

        if (StringUtils.isBlank(target)) {
            target = properties.getString("identifier.doi");
        }

        if (StringUtils.isBlank(target)) {
            throw new MissingTargetException("No viable target found in deposit");
        }

        return target;
    }

    UUID getDepositId(Path path) throws InvalidDepositException {
        try {
            return UUID.fromString(path.getFileName().toString());
        }
        catch (IllegalArgumentException e) {
            throw new InvalidDepositException(String.format(
                "Deposit dir must be an UUID; found '%s'",
                path.getFileName()
            ), e);
        }
    }

    OffsetDateTime getCreated(Metadata bagInfo) throws InvalidDepositException {
        try {
            // the created date comes from bag-info.txt, with the Created property
            var createdItems = bagInfo.get("created");

            if (createdItems.size() < 1) {
                throw new InvalidDepositException("Missing 'created' property in bag-info.txt");
            }
            else if (createdItems.size() > 1) {
                throw new InvalidDepositException("Value 'created' should contain exactly 1 value in bag; " + createdItems.size() + " found");
            }

            return OffsetDateTime.parse(createdItems.get(0));
        }
        catch (DateTimeParseException e) {
            throw new InvalidDepositException("Error while parsing date", e);
        }
    }
}
