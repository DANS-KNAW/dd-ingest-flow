package nl.knaw.dans.ingest.core.service;

import nl.knaw.dans.ingest.core.domain.Deposit;

import java.nio.file.Path;

public interface OriginalFilePathResolver {

    Path resolveToPathOnDisk(Deposit deposit);
}
