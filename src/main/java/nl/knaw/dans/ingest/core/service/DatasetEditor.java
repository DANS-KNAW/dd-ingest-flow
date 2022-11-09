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
import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.RejectedDepositException;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.Version;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.Embargo;
import org.apache.commons.lang.ArrayUtils;
import scala.collection.JavaConverters;
import scala.xml.Node;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public abstract class DatasetEditor {

    protected final DataverseClient dataverseClient;
    protected final boolean isMigration;
    protected final Dataset dataset;
    protected final Deposit deposit;
    protected final Map<String, String> variantToLicense;
    protected final List<URI> supportedLicenses;

    protected final int publishAwaitUnlockMillisecondsBetweenRetries;
    protected final int publishAwaitUnlockMaxNumberOfRetries;

    protected final Pattern fileExclusionPattern;
    protected final ZipFileHandler zipFileHandler;

    protected final ObjectMapper objectMapper;
    private final DateFormat dateAvailableFormat = new SimpleDateFormat("yyyy-MM-dd");

    protected DatasetEditor(DataverseClient dataverseClient,
        boolean isMigration,
        Dataset dataset,
        Deposit deposit,
        Map<String, String> variantToLicense,
        List<URI> supportedLicenses,
        int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries, Pattern fileExclusionPattern, ZipFileHandler zipFileHandler, ObjectMapper objectMapper) {
        this.dataverseClient = dataverseClient;
        this.isMigration = isMigration;
        this.dataset = dataset;
        this.deposit = deposit;
        this.variantToLicense = variantToLicense;
        this.supportedLicenses = supportedLicenses;
        this.publishAwaitUnlockMillisecondsBetweenRetries = publishAwaitUnlockMillisecondsBetweenRetries;
        this.publishAwaitUnlockMaxNumberOfRetries = publishAwaitUnlockMaxNumberOfRetries;
        this.fileExclusionPattern = fileExclusionPattern;
        this.zipFileHandler = zipFileHandler;
        this.objectMapper = objectMapper;
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

        return result;
    }

    private Integer addFile(String persistentId, FileInfo fileInfo) throws IOException, DataverseException {
        var dataset = dataverseClient.dataset(persistentId);
        var wrappedZip = zipFileHandler.wrapIfZipFile(fileInfo.getPath());

        var file = wrappedZip.orElse(fileInfo.getPath());
        var metadata = objectMapper.writeValueAsString(fileInfo.getMetadata());
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
        // TODO get license from XPath expression
        var licenseNode = node.$bslash("dcmiMetadata").$bslash("license")
            .find(License::isLicenseUri);

        if (licenseNode.isEmpty()) {
            throw new RejectedDepositException(deposit, "no license specified", null);
        }

        return License.getLicenseUri(supportedLicenses, variantToLicense, licenseNode).toASCIIString();
    }

    protected String toJson(Map<String, String> input) throws JsonProcessingException {
        return objectMapper.writeValueAsString(input);
    }

    Map<Path, FileInfo> getFileInfo() {
        return JavaConverters.mapAsJavaMap(deposit.getPathToFileInfo().get())
            .entrySet().stream()
            .map(entry -> {
                // conver to new FileInfo class
                var f = entry.getValue();
                var fileInfo = new FileInfo(f.file().toJava().toPath(), f.checksum(), f.metadata());

                return Map.entry(entry.getKey(), fileInfo);
            })
            .map(entry -> {
                // relativize the path
                var bagPath = entry.getKey();
                var fileInfo = entry.getValue();
                var newKey = Paths.get("data").relativize(bagPath);

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

    void embargoFiles(String persistentId, Date dateAvailable) throws IOException, DataverseException {
        if (!dateAvailable.after(new Date())) {
            log.debug("Date available in the past, no embargo: {}", dateAvailable);
        }
        else {
            var api = dataverseClient.dataset(persistentId);
            var files = api.getFiles(Version.LATEST.toString()).getData();

            var items = files.stream()
                .filter(f -> !"easy-migration".equals(f.getDirectoryLabel()))
                .map(f -> f.getDataFile().getId())
                .collect(Collectors.toList());

            embargoFiles(persistentId, dateAvailable, items);
        }
    }

    void embargoFiles(String persistentId, Date dateAvailable, Collection<Integer> fileIds) throws IOException, DataverseException {
        if (!dateAvailable.after(new Date())) {
            log.debug("Date available in the past, no embargo: {}", dateAvailable);
        }
        else {
            var api = dataverseClient.dataset(persistentId);
            var embargo = new Embargo(dateAvailableFormat.format(dateAvailable), "",
                ArrayUtils.toPrimitive(fileIds.toArray(Integer[]::new)));

            api.setEmbargo(embargo);
            api.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);
        }
    }
}
