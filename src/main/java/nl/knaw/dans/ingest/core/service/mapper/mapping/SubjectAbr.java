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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Node;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Optional;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_BASE_URL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_ARTIFACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_COMPLEX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_OLD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_ARTIFACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_COMPLEX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_OLD;

@Slf4j
public class SubjectAbr extends Base {
    private static boolean hasAttributes(Node node, String a1, String v1, String a2, String v2) {
        for (var i = 0; i < node.getAttributes().getLength(); ++i) {
            var n = node.getAttributes().item(i);
        }
        var r1 = getAttribute(node, a1)
            .map(item -> v1.equals(item.getTextContent()))
            .orElse(false);

        var r2 = getAttribute(node, a2)
            .map(item -> v2.equals(item.getTextContent()))
            .orElse(false);

        return r1 && r2;
    }

    public static boolean isAbrComplex(Node node) {
        return hasAttributes(node,
            "subjectScheme", SCHEME_ABR_COMPLEX,
            "schemeURI", SCHEME_URI_ABR_COMPLEX
        );
    }

    public static boolean isOldAbr(Node node) {
        return hasAttributes(node,
            "subjectScheme", SCHEME_ABR_OLD,
            "schemeURI", SCHEME_URI_ABR_OLD
        );
    }

    public static boolean isAbrArtifact(Node node) {
        return hasAttributes(node,
            "subjectScheme", SCHEME_ABR_ARTIFACT,
            "schemeURI", SCHEME_URI_ABR_ARTIFACT
        );
    }

    private static String attributeToText(Node node, String attribute) {

        return getAttribute(node, attribute)
            .map(Node::getTextContent)
            .orElseGet(() -> {
                log.error("Missing {} attribute on {} node", attribute, node.getNodeName());
                return null;
            });
    }

    public static String toAbrComplex(Node node) {
        return attributeToText(node, "valueURI");
    }

    public static String toAbrArtifact(Node node) {
        return attributeToText(node, "valueURI");
    }

    public static String fromAbrOldToAbrArtifact(Node node) {
        if (!isOldAbr(node)) {
            return null;
        }
        return Optional.ofNullable(attributeToText(node, "valueURI"))
            .map(value -> {
                try {
                    var uuid = Paths.get(new URI(value).getPath()).getFileName().toString();
                    return String.format("%s/%s", ABR_BASE_URL, uuid);
                }
                catch (URISyntaxException e) {
                    log.error("Invalid URI: {}", value);
                }

                return null;
            })
            .orElse(null);
    }
}
