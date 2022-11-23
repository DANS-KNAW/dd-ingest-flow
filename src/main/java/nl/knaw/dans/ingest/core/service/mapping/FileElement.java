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
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class FileElement extends Base {
    private final static Pattern filenameForbidden = Pattern.compile("[:*?\"<>|;#]");
    private final static Pattern directoryLabelForbidden = Pattern.compile("[^_\\-.\\\\/ 0-9a-zA-Z]+");
    private final static Map<String, Boolean> accessibilityToRestrict = Map.of(
        "KNOWN", true,
        "NONE", true,
        "RESTRICTED_REQUEST", true,
        "ANONYMOUS", false
    );

    public static FileMeta toFileMeta(Node node, boolean defaultRestrict) {
        var filepathAttribute = getAttribute(node, "filepath")
            .map(Node::getTextContent)
            .orElseThrow(() -> new RuntimeException("File node without a filepath attribute"));

        if (!filepathAttribute.startsWith("data/")) {
            throw new RuntimeException(String.format("file outside data folder: %s", filepathAttribute));
        }

        var pathInDataset = Path.of(filepathAttribute.substring("data/".length()));
        var filename = pathInDataset.getFileName().toString();
        var sanitizedFilename = replaceForbiddenCharactersInFilename(filename);
        var dirPath = Optional.ofNullable(pathInDataset.getParent()).map(Path::toString).orElse(null);
        var sanitizedDirLabel = replaceForbiddenCharactersInPath(dirPath);

        var restricted = getChildNode(node, "dcterms:accessibleToRights")
            .map(Node::getTextContent)
            .map(accessibilityToRestrict::get)
            .orElse(defaultRestrict);

        var originalFilePath = !StringUtils.equals(filename, sanitizedFilename) || !StringUtils.equals(dirPath, sanitizedDirLabel)
            ? pathInDataset.toString()
            : null;

        var kv = getKeyValuePairs(node, filename, originalFilePath);

        var description = getDescription(kv);

        var fm = new FileMeta();
        fm.setLabel(sanitizedFilename);
        fm.setDirectoryLabel(sanitizedDirLabel);
        fm.setDescription(description);
        fm.setRestricted(restricted);

        return fm;
    }

    static String getDescription(Map<String, List<String>> kv) {
        if (!kv.isEmpty()) {
            if (kv.keySet().size() == 1 && kv.containsKey("description")) {
                return kv.get("description").stream().findFirst().orElse(null);
            }
            else {
                return formatKeyValuePairs(kv);
            }
        }

        return null;
    }

    static String formatKeyValuePairs(Map<String, List<String>> kv) {
        return kv.entrySet().stream().map(entry -> {
                var values = StringUtils.join(entry.getValue(), ",");
                return String.format("%s: \"%s\"", entry.getKey(), values);
            })
            .collect(Collectors.joining("; "));
    }

    static Map<String, List<String>> getKeyValuePairs(Node node, String filename, String originalFilePath) {
        var fixedKeys = List.of(
            "hardware",
            "original_OS",
            "software",
            "notes",
            "case_quantity",
            "file_category",
            "description",
            "othmat_codebook",
            "data_collector",
            "collection_date",
            "time_period",
            "geog_cover",
            "geog_unit",
            "local_georef",
            "mapprojection",
            "analytic_units");

        var result = new HashMap<String, List<String>>();

        for (var key : fixedKeys) {
            var child = getChildNodes(node, String.format("*[local-name() = '%s']", key))
                .map(Node::getTextContent)
                .collect(Collectors.toList());

            log.trace("matches for key '{}': {}", key, result);

            if (child.size() > 0) {
                result.put(key, child);
            }
        }

        getChildNodes(node, "keyvaluepair")
            .forEach(n -> {
                var key = getChildNode(n, "key")
                    .map(Node::getTextContent)
                    .orElse(null);

                var value = getChildNode(n, "value")
                    .map(Node::getTextContent)
                    .orElse(null);

                if (key != null && value != null) {
                    result.computeIfAbsent(key, k -> new ArrayList<>())
                        .add(value);
                }
            });

        getChildNodes(node, "title")
            .map(Node::getTextContent)
            .filter(n -> StringUtils.equalsIgnoreCase(filename, n))
            .forEach(n -> result.computeIfAbsent("title", k -> new ArrayList<>())
                .add(n));

        if (originalFilePath != null) {
            result.computeIfAbsent("original_filepath", k -> new ArrayList<>()).add(originalFilePath);
        }

        return result;
    }

    static String replaceForbiddenCharactersInPath(String dirPath) {
        if (dirPath == null) {
            return null;
        }
        return directoryLabelForbidden.matcher(dirPath).replaceAll("_");
    }

    static String replaceForbiddenCharactersInFilename(String filename) {
        if (filename == null) {
            return null;
        }
        return filenameForbidden.matcher(filename).replaceAll("_");
    }

    public static Map<Path, FileInfo> pathToFileInfo(Deposit deposit) {
        var defaultRestrict = XPathEvaluator.nodes(deposit.getDdm(), "//ddm:profile/ddm:accessRights")
            .map(AccessRights::toDefaultRestrict)
            .findFirst()
            .orElse(true);

        var filePathToSha1 = getFilePathToSha1(deposit);
        var result = new HashMap<Path, FileInfo>();

        XPathEvaluator.nodes(deposit.getFilesXml(), "//files:file").forEach(node -> {
            var path = getAttribute(node, "filepath")
                .map(Node::getTextContent)
                .map(Path::of)
                .orElseThrow();

            var sha1 = filePathToSha1.get(path);
            var absolutePath = deposit.getBagDir().resolve(path);

            result.put(path, new FileInfo(absolutePath, sha1, toFileMeta(node, defaultRestrict)));
        });

        return result;
    }

    static Map<Path, String> getFilePathToSha1(Deposit deposit) {
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
