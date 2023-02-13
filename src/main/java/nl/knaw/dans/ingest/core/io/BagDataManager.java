package nl.knaw.dans.ingest.core.io;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Metadata;
import gov.loc.repository.bagit.exceptions.InvalidBagitFileFormatException;
import gov.loc.repository.bagit.exceptions.MaliciousPathException;
import gov.loc.repository.bagit.exceptions.UnparsableVersionException;
import gov.loc.repository.bagit.exceptions.UnsupportedAlgorithmException;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public interface BagDataManager {

    Metadata readBagMetadata(Path bagDir) throws UnparsableVersionException, InvalidBagitFileFormatException, IOException;
    Bag readBag(Path bagDir) throws UnparsableVersionException, InvalidBagitFileFormatException, IOException, MaliciousPathException, UnsupportedAlgorithmException;

    Configuration readConfiguration(Path path) throws ConfigurationException;

    void saveConfiguration(Path path, Map<String, Object> configuration) throws ConfigurationException;
}
