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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.FileInfo;
import nl.knaw.dans.ingest.core.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.service.mapper.mapping.AccessRights;
import nl.knaw.dans.ingest.core.service.mapper.mapping.FileElement;
import nl.knaw.dans.ingest.core.service.mapper.mapping.License;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.Version;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.file.DataFile;
import nl.knaw.dans.lib.dataverse.model.file.FileMeta;
import org.apache.commons.codec.digest.DigestUtils;
import org.w3c.dom.Node;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public abstract class DatasetEditor {
    protected static final List<String> embargoExclusions = Arrays.asList("easy-migration.zip", "original-metadata.zip");

    protected final DataverseClient dataverseClient;
    protected final boolean isMigration;
    protected final Dataset dataset;
    protected final Deposit deposit;
    protected final Map<String, String> variantToLicense;
    protected final List<URI> supportedLicenses;

    protected final Pattern fileExclusionPattern;
    protected final ZipFileHandler zipFileHandler;

    protected final ObjectMapper objectMapper;
    protected final DatasetService datasetService;

    protected DatasetEditor(boolean isMigration,
        Dataset dataset,
        Deposit deposit,
        Map<String, String> variantToLicense,
        List<URI> supportedLicenses,
        Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler,
        ObjectMapper objectMapper,
        DatasetService datasetService) {
        this.dataverseClient = datasetService._getClient();
        this.isMigration = isMigration;
        this.dataset = dataset;
        this.deposit = deposit;
        this.variantToLicense = variantToLicense;
        this.supportedLicenses = supportedLicenses;
        this.fileExclusionPattern = fileExclusionPattern;
        this.zipFileHandler = zipFileHandler;
        this.objectMapper = objectMapper;
        this.datasetService = datasetService;
    }

    static Instant parseDate(String value) {
        try {
            log.debug("Trying to parse {} as LocalDate", value);
            return LocalDate.parse(value).atStartOfDay(ZoneId.systemDefault()).toInstant();
        }
        catch (DateTimeParseException e) {
            try {
                log.debug("Trying to parse {} as ZonedDateTime", value);
                return ZonedDateTime.parse(value).toInstant();
            }
            catch (DateTimeParseException ee) {
                log.debug("Trying to parse {} as LocalDateTime", value);
                var id = ZoneId.systemDefault().getRules().getOffset(Instant.now());
                return LocalDateTime.parse(value).toInstant(id);
            }
        }
    }

    public abstract String performEdit() throws IOException, DataverseException, InterruptedException;

    Map<Integer, FileInfo> addFiles(String persistentId, Collection<FileInfo> fileInfos) throws IOException, DataverseException {
        var result = new HashMap<Integer, FileInfo>(fileInfos.size());

        for (var fileInfo : fileInfos) {
            log.debug("Adding file, directoryLabel = {}, label = {}",
                fileInfo.getMetadata().getDirectoryLabel(), fileInfo.getMetadata().getLabel());

            var id = addFile(persistentId, fileInfo);
            result.put(id, fileInfo);
        }
        if (!isMigration) {
            var path = zipFileHandler.zipOriginalMetadata(deposit.getDdmPath(), deposit.getFilesXmlPath());
            var checksum = DigestUtils.sha1Hex(new FileInputStream(path.toFile()));
            var fileMeta = new FileMeta();
            fileMeta.setLabel("original-metadata.zip");
            var fileInfo = new FileInfo(path, checksum, fileMeta);
            var id = addFile(persistentId, fileInfo);
            result.put(id, fileInfo);
            try {
                Files.deleteIfExists(path);
            }
            catch (IOException e) {
                log.warn("Unable to delete zipfile {}", path, e);
            }
        }

        return result;
    }

    private Integer addFile(String persistentId, FileInfo fileInfo) throws IOException, DataverseException {
        var dataset = dataverseClient.dataset(persistentId);
        var wrappedZip = zipFileHandler.wrapIfZipFile(fileInfo.getPath());

        var file = wrappedZip.orElse(fileInfo.getPath());
        var metadata = objectMapper.writeValueAsString(fileInfo.getMetadata());
        log.info("Adding file {} with metadata {}", file, metadata);
        var result = dataset.addFileItem(Optional.of(file.toFile()), Optional.of(metadata));

        if (wrappedZip.isPresent()) {
            try {
                Files.deleteIfExists(wrappedZip.get());
            }
            catch (IOException e) {
                log.error("Unable to delete zipfile {}", wrappedZip.get(), e);
            }
        }

        log.debug("Result: {}", result);
        return result.getData().getFiles().get(0).getDataFile().getId();
    }

    protected String getLicense(Node node) {
        return XPathEvaluator.nodes(node, "/ddm:DDM/ddm:dcmiMetadata/dcterms:license")
            .filter(License::isLicenseUri)
            .findFirst()
            .map(n -> License.getLicenseUri(supportedLicenses, variantToLicense, n))
            .map(URI::toASCIIString)
            .orElseThrow(() -> new RejectedDepositException(deposit, "no license specified"));
    }

    protected String toJson(Map<String, String> input) throws JsonProcessingException {
        return objectMapper.writeValueAsString(input);
    }

    Map<Path, FileInfo> getFileInfo() {

        var files = FileElement.pathToFileInfo(deposit);

        return files.entrySet().stream()
            .map(entry -> {
                // relativize the path
                // TODO make this all tested
                var bagPath = entry.getKey();
                var fileInfo = entry.getValue();
                var newKey = Path.of("data").relativize(bagPath);

                return Map.entry(newKey, fileInfo);
            })
            .filter(entry -> {
                // remove entries that match the file exclusion pattern
                var path = entry.getKey().toString();

                if (fileExclusionPattern != null) {
                    return !fileExclusionPattern.matcher(path).matches();
                }

                return true;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // FIL008, FIL009
    void embargoFiles(String persistentId, Instant dateAvailable) throws IOException, DataverseException {
        var now = Instant.now();

        if (!dateAvailable.isAfter(now)) {
            log.debug("Date available in the past, no embargo: {}", dateAvailable);
        }
        else {
            var api = dataverseClient.dataset(persistentId);
            var files = api.getFiles(Version.LATEST.toString()).getData();

            var items = files.stream()
                .filter(f -> !embargoExclusions.contains(f.getLabel()))
                .map(FileMeta::getDataFile)
                .map(DataFile::getId)
                .collect(Collectors.toList());

            embargoFiles(persistentId, dateAvailable, items);
        }
    }

    void embargoFiles(String persistentId, Instant dateAvailable, Collection<Integer> fileIds) throws IOException, DataverseException {
        var now = Instant.now();

        if (!dateAvailable.isAfter(now)) {
            log.debug("Date available in the past, no embargo: {}", dateAvailable);
        }
        else if (fileIds.size() == 0) {
            log.debug("No files to embargo");
        }
        else {
            datasetService.setEmbargo(persistentId, dateAvailable, fileIds);
        }
    }

    Instant getDateAvailable(Deposit deposit) {
        return XPathEvaluator.strings(deposit.getDdm(), "/ddm:DDM/ddm:profile/ddm:available")
            .map(DatasetEditor::parseDate)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Deposit without a ddm:available element"));
    }

    void configureEnableAccessRequests(String persistentId, boolean canEnable) throws IOException, DataverseException {
        var api = dataverseClient.accessRequests(persistentId);

        var ddm = deposit.getDdm();
        var files = deposit.getFilesXml();

        var accessRights = XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/ddm:accessRights")
            .findFirst()
            .orElseThrow();

        var enable = AccessRights.isEnableRequests(accessRights, files);

        log.trace("AccessRequests enable {} can {}", enable, canEnable);

        if (!enable) {
            api.disable();
        }
        else if (canEnable) {
            api.enable();
        }
    }
}
