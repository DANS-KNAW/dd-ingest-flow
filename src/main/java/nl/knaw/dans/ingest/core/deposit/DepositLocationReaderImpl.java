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

import gov.loc.repository.bagit.exceptions.InvalidBagitFileFormatException;
import gov.loc.repository.bagit.exceptions.UnparsableVersionException;
import gov.loc.repository.bagit.reader.BagitTextFileReader;
import gov.loc.repository.bagit.reader.KeyValueReader;
import nl.knaw.dans.ingest.core.domain.DepositLocation;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class DepositLocationReaderImpl implements DepositLocationReader {
    private final BagDirResolver bagDirResolver;

    public DepositLocationReaderImpl(BagDirResolver bagDirResolver) {
        this.bagDirResolver = bagDirResolver;
    }

    @Override
    public DepositLocation readDepositLocation(Path path) throws InvalidDepositException, IOException {
        var bagDir = bagDirResolver.getValidBagDir(path);
        var depositId = getDepositId(path);
        var target = getTarget(path);
        var created = getCreated(bagDir);

        return new DepositLocation(path, target, depositId.toString(), created);
    }

    List<SimpleImmutableEntry<String, String>> getBagInfo(Path bagDir) throws InvalidBagitFileFormatException, IOException, UnparsableVersionException {
        var bagitInfo = BagitTextFileReader.readBagitTextFile(bagDir.resolve("bagit.txt"));
        var encoding = bagitInfo.getValue();

        return KeyValueReader.readKeyValuesFromFile(bagDir.resolve("bag-info.txt"), ":", encoding);
    }

    String getTarget(Path path) throws InvalidDepositException {
        try {
            var properties = getProperties(path);
            // the logic for the target should be
            // 1. if there is a dataverse.sword-token, use that
            // 2. otherwise, use identifier.doi
            var target = properties.getString("dataverse.sword-token");

            if (target == null) {
                target = properties.getString("identifier.doi");
            }

            if (target == null) {
                // set a default value?
                target = "";
            }

            return target;
        }
        catch (ConfigurationException e) {
            throw new InvalidDepositException("Deposit properties file could not be read", e);
        }
    }

    Configuration getProperties(Path bagDir) throws ConfigurationException {
        var propertiesFile = bagDir.resolve("deposit.properties");
        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(propertiesFile.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>
            (PropertiesConfiguration.class, null, true)
            .configure(paramConfig);

        return builder.getConfiguration();
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

    OffsetDateTime getCreated(Path path) throws InvalidDepositException {
        try {
            // the created date comes from bag-info.txt, with the Created property
            var bagInfo = getBagInfo(path);
            var createdItems = bagInfo.stream()
                .filter(s -> s.getKey().equalsIgnoreCase("created"))
                .collect(Collectors.toList());

            if (createdItems.size() < 1) {
                throw new InvalidDepositException("Missing 'created' property in bag-info.txt");
            }
            else if (createdItems.size() > 1) {
                throw new InvalidDepositException("Value 'created' should contain exactly 1 value in bag; " + createdItems.size() + " found");
            }

            return OffsetDateTime.parse(createdItems.get(0).getValue());
        }
        catch (DateTimeParseException e) {
            throw new InvalidDepositException("Error while parsing date", e);
        }
        catch (InvalidBagitFileFormatException | IOException | UnparsableVersionException e) {
            throw new InvalidDepositException("BagIt file(s) could not be read", e);
        }
    }
}
