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

import lombok.extern.slf4j.Slf4j;
import scala.Option;
import scala.collection.JavaConverters;
import scala.xml.Node;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

@Slf4j
public class License {

    final static String DCTERMS_NAMESPACE_URI = "http://purl.org/dc/terms/";
    final static String XML_SCHEMA_INSTANCE_URI = "http://www.w3.org/2001/XMLSchema-instance";

    public static boolean isLicenseUri(Node node) {
        if (!"license".equals(node.label())) {
            return false;
        }

        if (!DCTERMS_NAMESPACE_URI.equals(node.namespace())) {
            return false;
        }

        var xsiType = "URI";
        var result = JavaConverters.asJavaCollection(node.attribute(XML_SCHEMA_INSTANCE_URI, "type").get())
            .stream()
            .map(Node::text)
            .filter(n -> n.endsWith(String.format(":%s", xsiType)) || xsiType.equals(n))
            .findFirst();

        if (result.isEmpty()) {
            return false;
        }

        // validate it is a valid URI
        try {
            new URI(node.text());
            return true;
        }
        catch (URISyntaxException e) {
            return false;
        }
    }

    public static URI getLicenseUri(List<URI> supportedLicenses, Map<String, String> variantToLicense, Option<Node> licenseNode) {
        // TODO add the URI normalization
        var licenseText = licenseNode.get().text();

        try {
            if (!isLicenseUri(licenseNode.get())) {
                throw new IllegalArgumentException("Not a valid license node");
            }

            var licenseUri = new URI(licenseText);

            if (!supportedLicenses.contains(licenseUri)) {
                throw new IllegalArgumentException(String.format("Unsupported license: %s", licenseUri));
            }

            return licenseUri;
        } catch (URISyntaxException e) {
            log.error("Invalid license URI: {}", licenseText, e);
            throw new IllegalArgumentException("Not a valid license URI", e);
        }
    }
}
