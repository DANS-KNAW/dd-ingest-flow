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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.XmlReader;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public static URI getLicenseUri(List<URI> supportedLicenses, Map<String, String> variantToLicense, Node licenseNode) {
        var licenseText = Optional.ofNullable(licenseNode)
            .map(Node::getTextContent)
            .map(String::trim)
            .map(License::removeTrailingSlash)
            .map(s -> variantToLicense.getOrDefault(s, s))
            .orElseThrow(() -> new IllegalArgumentException("License node is null"));

        try {
            if (!isLicenseUri(licenseNode)) {
                throw new IllegalArgumentException("Not a valid license node");
            }

            var licenseUri = new URI(licenseText);
            final var licenseUriFinal = licenseUri;

            licenseUri = normalizeScheme(supportedLicenses, licenseUri)
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                    "Unsupported license: %s", licenseUriFinal)));

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

    private static String removeTrailingSlash(String s) {
        if (s.endsWith("/")) {
            return s.substring(0, s.length() - 1);
        }

        return s;
    }

    private static Optional<URI> normalizeScheme(List<URI> supportedLicenses, URI uri) {
        var schemes = Set.of("https", "http");

        for (var license : supportedLicenses) {
            if (StringUtils.equals(license.getHost(), uri.getHost())
                && StringUtils.equals(license.getPath(), uri.getPath())
                && license.getPort() == uri.getPort()
                && StringUtils.equals(license.getQuery(), uri.getQuery())
                && schemes.contains(uri.getScheme())
            ) {
                return Optional.of(license);
            }
        }

        return Optional.empty();
    }
}
