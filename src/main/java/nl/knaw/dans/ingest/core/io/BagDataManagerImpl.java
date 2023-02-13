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
    public Configuration readConfiguration(Path path) throws ConfigurationException {
        var propertiesFile = path.resolve("deposit.properties");
        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(propertiesFile.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>
            (PropertiesConfiguration.class, null, true)
            .configure(paramConfig);

        return builder.getConfiguration();
    }

    @Override
    public void saveConfiguration(Path path, Map<String, Object> configuration) throws ConfigurationException {
        var params = new Parameters();
        var paramConfig = params.properties()
            .setFileName(path.toString());

        var builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(
            PropertiesConfiguration.class, null, true
        ).configure(paramConfig);

        var config = builder.getConfiguration();

        for (var entry : configuration.entrySet()) {
            config.setProperty(entry.getKey(), entry.getValue());
        }

        builder.save();
    }
}