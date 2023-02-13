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

import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DepositWriterImpl implements DepositWriter {
    @Override
    public void saveDeposit(Deposit deposit) throws InvalidDepositException {
        var path = deposit.getDir();
        var propertiesFile = path.resolve("deposit.properties");

        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(propertiesFile.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class, null, true).configure(
            paramConfig);

        try {
            var config = builder.getConfiguration();
            config.setProperty("state.label", deposit.getState().toString());
            config.setProperty("state.description", deposit.getStateDescription());
            config.setProperty("identifier.doi", deposit.getDoi());
            config.setProperty("identifier.urn", deposit.getUrn());
            builder.save();
        }
        catch (ConfigurationException cex) {
            throw new InvalidDepositException("Unable to save deposit properties", cex);
        }
    }

    @Override
    public void moveDeposit(Deposit deposit, Path target) throws IOException {
        moveDeposit(deposit.getDir(), target);
    }

    @Override
    public void moveDeposit(Path source, Path target) throws IOException {
        var destination = target.resolve(source.getFileName());
        Files.move(source, destination);
    }
}
