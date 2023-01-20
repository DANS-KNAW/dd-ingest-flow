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

import gov.loc.repository.bagit.creator.CreatePayloadManifestsVistor;
import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.hash.Hasher;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import gov.loc.repository.bagit.util.PathUtils;
import gov.loc.repository.bagit.writer.ManifestWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static gov.loc.repository.bagit.hash.StandardSupportedAlgorithms.SHA1;

public class ManifestHelper {

    static public void addSha1File(Bag bag) throws NoSuchAlgorithmException, IOException {
        var manifests = bag.getPayLoadManifests();
        var algorithms = manifests.stream().map(Manifest::getAlgorithm);
        if (algorithms.anyMatch(SHA1::equals))
            return;

        var payloadFilesMap = Hasher.createManifestToMessageDigestMap(List.of(SHA1));
        var payloadVisitor = new CreatePayloadManifestsVistor(payloadFilesMap, true);
        Files.walkFileTree(PathUtils.getDataDir(bag), payloadVisitor);
        manifests.addAll(payloadFilesMap.keySet());
        ManifestWriter.writePayloadManifests(manifests, PathUtils.getBagitDir(bag), bag.getRootDir(), bag.getFileEncoding());
    }

    static public Map<Path, String> getFilePathToSha1(Deposit deposit) {
        var result = new HashMap<Path, String>();
        var bag = deposit.getBag();
        var manifest = bag.getPayLoadManifests().stream()
            .filter(item -> item.getAlgorithm().equals(StandardSupportedAlgorithms.SHA1))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Deposit bag does not have SHA-1 payload manifest"));

        for (var entry : manifest.getFileToChecksumMap().entrySet()) {
            result.put(deposit.getBagDir().relativize(entry.getKey()), entry.getValue());
        }

        return result;
    }
}
