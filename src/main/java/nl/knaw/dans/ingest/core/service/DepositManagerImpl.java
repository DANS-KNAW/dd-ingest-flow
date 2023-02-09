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

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.reader.BagReader;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.stream.Collectors;

public class DepositManagerImpl implements DepositManager {
    private final String FILENAME = "deposit.properties";
    private final BagReader bagReader = new BagReader();
    private final XmlReader xmlReader;

    public DepositManagerImpl(XmlReader xmlReader) {
        this.xmlReader = xmlReader;
    }

    private Path getDepositPath(Path path) {
        return path.resolve(FILENAME);
    }

    @Override
    public void saveDeposit(Deposit deposit) throws InvalidDepositException {
        var propertiesFile = getDepositPath(deposit.getDir());

        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(propertiesFile.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class, null, true).configure(
            paramConfig);

        try {
            var config = builder.getConfiguration();
            mapToConfig(config, deposit);
            builder.save();
        }
        catch (ConfigurationException cex) {
            throw new InvalidDepositException("Unable to save deposit properties", cex);
        }
    }

    private Path validateAndGetBagDir(Path path) throws InvalidDepositException, IOException {
        // first check if this is a directory
        if (!Files.isDirectory(path)) {
            throw new InvalidDepositException(String.format("%s is not a directory", path));
        }

        try (var substream = Files.list(path).filter(Files::isDirectory)) {
            var directories = substream.collect(Collectors.toList());

            // only 1 directory allowed, not 0 or more than 1
            if (directories.size() != 1) {
                throw new InvalidDepositException(String.format(
                    "%s has more or fewer than one subdirectory", path
                ));
            }

            // check for the presence of deposit.properties and bagit.txt
            if (!Files.exists(path.resolve("deposit.properties"))) {
                throw new InvalidDepositException(String.format(
                    "%s does not contain a deposit.properties file", path
                ));
            }

            var bagDir = directories.get(0);

            if (!Files.exists(bagDir.resolve("bagit.txt"))) {
                throw new InvalidDepositException(String.format(
                    "%s does not contain a bag", path
                ));
            }

            return bagDir;
        }
    }

    @Override
    public Deposit loadDeposit(Path path) throws InvalidDepositException, IOException {
        var bagDir = validateAndGetBagDir(path);
        var propertiesFile = getDepositPath(path);

        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(propertiesFile.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class, null, true).configure(
            paramConfig);

        try {
            var config = builder.getConfiguration();
            var bagInfo = bagReader.read(bagDir);

            var deposit = mapToDeposit(path, bagDir, config, bagInfo);

            deposit.setBag(bagInfo);
            deposit.setDdm(readOptionalXmlFile(deposit.getDdmPath()));
            deposit.setFilesXml(readOptionalXmlFile(deposit.getFilesXmlPath()));
            deposit.setAgreements(readOptionalXmlFile(deposit.getAgreementsXmlPath()));
            deposit.setAmd(readOptionalXmlFile(deposit.getAmdPath()));

            return deposit;
        }
        catch (Throwable cex) {
            throw new InvalidDepositException(cex.getMessage(), cex);
        }
    }

    Document readOptionalXmlFile(Path path) throws ParserConfigurationException, IOException, SAXException {
        if (Files.exists(path)) {
            return xmlReader.readXmlFile(path);
        }
        return null;
    }

    Deposit mapToDeposit(Path path, Path bagDir, Configuration config, Bag bag) {
        var deposit = new Deposit();
        deposit.setBagDir(bagDir);
        deposit.setDir(path);
        deposit.setDoi(config.getString("identifier.doi", ""));
        deposit.setUrn(config.getString("identifier.urn"));
        deposit.setId(config.getString("bag-store.bag-id"));
        deposit.setCreated(Optional.ofNullable(config.getString("creation.timestamp")).map(OffsetDateTime::parse).orElse(null));
        deposit.setDepositorUserId(config.getString("depositor.userId"));

        deposit.setDataverseIdProtocol(config.getString("dataverse.id-protocol", ""));
        deposit.setDataverseIdAuthority(config.getString("dataverse.id-authority", ""));
        deposit.setDataverseId(config.getString("dataverse.id-identifier", ""));
        deposit.setDataverseBagId(config.getString("dataverse.bag-id", ""));
        deposit.setDataverseNbn(config.getString("dataverse.nbn", ""));
        deposit.setDataverseOtherId(config.getString("dataverse.other-id", ""));
        deposit.setDataverseOtherIdVersion(config.getString("dataverse.other-id-version", ""));
        deposit.setDataverseSwordToken(config.getString("dataverse.sword-token", ""));

        var isVersionOf = bag.getMetadata().get("Is-Version-Of");

        if (isVersionOf != null) {
            isVersionOf.stream()
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .ifPresent(item -> {
                    deposit.setUpdate(true);
                    deposit.setIsVersionOf(item);
                });
        }

        var created = getUniqueValue(bag, "Created");
        deposit.setBagCreated(OffsetDateTime.parse(created).toInstant());

        return deposit;
    }

    String getUniqueValue(Bag bag, String key) {
        var metadata = bag.getMetadata();
        var value = metadata.get(key);

        if (value == null) {

            throw new IllegalArgumentException(String.format("No '%s' value found in bag", key));
        }
        if (value.size() != 1) {
            throw new IllegalArgumentException(String.format("Value '%s' should contain exactly 1 value in bag; %s found", key, value.size()));
        }

        if (StringUtils.isBlank(value.get(0))) {
            throw new IllegalArgumentException(String.format("Value '%s' is empty", key));
        }

        return value.get(0);
    }

    void mapToConfig(Configuration config, Deposit deposit) {
        // TODO do we need to clear it if we use setProperty?
        config.clearProperty("state.label");
        config.clearProperty("state.description");
        config.setProperty("state.label", deposit.getState().toString());
        config.setProperty("state.description", deposit.getStateDescription());
        config.setProperty("identifier.doi", deposit.getDoi());
        config.setProperty("identifier.urn", deposit.getUrn());
    }
}
