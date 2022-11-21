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
package nl.knaw.dans.ingest.core.service.mapping;

import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.Deposit;
import nl.knaw.dans.ingest.core.service.FileInfo;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import nl.knaw.dans.lib.dataverse.model.file.FileMeta;
import org.w3c.dom.Node;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class FileElement extends Base {
    private final static String[] forbiddenCharactersInFileName = ":*?\"<>|;#".split("");
    //    private final static String[] allowedCharactersInDirectoryLabel = "_-.\\/ 0123456789" + Character..
    private final static Map<String, Boolean> accessibilityToRestrict = Map.of(
        "KNOWN", true,
        "NONE", true,
        "RESTRICTED_REQUEST", true,
        "ANONYMOUS", false
    );
    //    private val allowedCharactersInDirectoryLabel = List('_', '-', '.', '\\', '/', ' ') ++ ('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')

    public static FileMeta toFileMeta(Node node, boolean defaultRestrict) {
        // TODO implement this
        return null;

    }

    public static Map<Path, FileInfo> pathToFileInfo(Deposit deposit) {
        var defaultRestrict = XPathEvaluator.nodes(deposit.getDdm(), "//ddm:profile/ddm:accessRights")
            .map(AccessRights::toDefaultRestrict)
            .findFirst()
            .orElse(true);

        var filePathToSha1 = new HashMap<Path, String>();

        var bag = deposit.getBag();
        var manifest = bag.getPayLoadManifests().stream()
            .filter(item -> item.getAlgorithm().equals(StandardSupportedAlgorithms.SHA1))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Deposit bag does not have SHA-1 payload manifest"));

        for (var entry : manifest.getFileToChecksumMap().entrySet()) {
            filePathToSha1.put(deposit.getBagDir().relativize(entry.getKey()), entry.getValue());
        }

        var result = new HashMap<Path, FileInfo>();

        XPathEvaluator.nodes(deposit.getFilesXml(), "//file").forEach(node -> {
            var path = getAttribute(node, "filepath")
                .map(Node::getTextContent)
                .map(Path::of)
                .orElseThrow();

            var sha1 = filePathToSha1.get(path);

            result.put(path, new FileInfo(path, sha1, toFileMeta(node, defaultRestrict)));
        });

        return result;
    }
}
