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
import nl.knaw.dans.ingest.core.service.XmlReader;
import org.w3c.dom.Node;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

@Slf4j
public class License extends Base {

    public static boolean isLicenseUri(Node node) {
        if (!"license".equals(node.getLocalName())) {
            return false;
        }

        if (!XmlReader.NAMESPACE_DCTERMS.equals(node.getNamespaceURI())) {
            return false;
        }

        if (!hasXsiType(node, "URI")) {
            return false;
        }

        // validate it is a valid URI
        try {
            new URI(node.getTextContent().trim());
            return true;
        }
        catch (URISyntaxException e) {
            log.error("Invalid URI: " + node.getTextContent(), e);
            return false;
        }
    }

    public static URI getLicenseUri(List<URI> supportedLicenses, Node licenseNode) {
        // TRM001
        var licenseText = Optional.ofNullable(licenseNode)
            .map(Node::getTextContent)
            .map(String::trim)
            .orElseThrow(() -> new IllegalArgumentException("License node is null"));

        try {
            if (!isLicenseUri(licenseNode)) {
                throw new IllegalArgumentException("Not a valid license node");
            }

            var licenseUri = new URI(licenseText);

            if (!supportedLicenses.contains(licenseUri)) {
                throw new IllegalArgumentException(String.format("Unsupported license: %s", licenseUri));
            }

            return licenseUri;
        }
        catch (URISyntaxException e) {
            log.error("Invalid license URI: {}", licenseText, e);
            throw new IllegalArgumentException("Not a valid license URI", e);
        }
    }
}
