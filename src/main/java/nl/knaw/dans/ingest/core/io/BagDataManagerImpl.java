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
package nl.knaw.dans.ingest.core.io;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Metadata;
import gov.loc.repository.bagit.exceptions.InvalidBagitFileFormatException;
import gov.loc.repository.bagit.exceptions.MaliciousPathException;
import gov.loc.repository.bagit.exceptions.UnparsableVersionException;
import gov.loc.repository.bagit.exceptions.UnsupportedAlgorithmException;
import gov.loc.repository.bagit.reader.BagReader;
import gov.loc.repository.bagit.reader.BagitTextFileReader;
import gov.loc.repository.bagit.reader.KeyValueReader;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class BagDataManagerImpl implements BagDataManager {
    private final String DEPOSIT_PROPERTIES_FILENAME = "deposit.properties";
    private final BagReader bagReader;

    public BagDataManagerImpl(BagReader bagReader) {
        this.bagReader = bagReader;
    }

    @Override
    public Metadata readBagMetadata(Path bagDir) throws UnparsableVersionException, InvalidBagitFileFormatException, IOException {
        var bagitInfo = BagitTextFileReader.readBagitTextFile(bagDir.resolve("bagit.txt"));
        var encoding = bagitInfo.getValue();

        var values = KeyValueReader.readKeyValuesFromFile(bagDir.resolve("bag-info.txt"), ":", encoding);
        var metadata = new Metadata();
        metadata.addAll(values);

        return metadata;
    }

    @Override
    public Bag readBag(Path bagDir) throws UnparsableVersionException, InvalidBagitFileFormatException, IOException, MaliciousPathException, UnsupportedAlgorithmException {
        return bagReader.read(bagDir);
    }

    @Override
    public Configuration readDepositProperties(Path path) throws ConfigurationException {
        var propertiesFile = path.resolve(DEPOSIT_PROPERTIES_FILENAME);
        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(propertiesFile.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>
            (PropertiesConfiguration.class, null, true)
            .configure(paramConfig);

        return builder.getConfiguration();
    }

    @Override
    public void saveDepositProperties(Path path, Map<String, Object> configuration) throws ConfigurationException {
        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(path.resolve(DEPOSIT_PROPERTIES_FILENAME).toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(
            PropertiesConfiguration.class, null, true
        ).configure(paramConfig);

        var config = builder.getConfiguration();

        for (var entry : configuration.entrySet()) {
            // setProperty overwrites existing values, so no clearing is needed
            config.setProperty(entry.getKey(), entry.getValue());
        }

        builder.save();
    }
}