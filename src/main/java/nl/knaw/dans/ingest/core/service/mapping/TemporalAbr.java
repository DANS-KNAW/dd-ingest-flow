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
import org.w3c.dom.Node;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_PERIOD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_PLUS;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_PERIOD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_PLUS;

@Slf4j
public class TemporalAbr extends Base {
    private static boolean hasAttributes(Node node, String a1, String v1, String a2, String v2) {
        var r1 = getAttribute(node, XmlReader.NAMESPACE_DDM, a1)
            .map(item -> v1.equals(item.getTextContent()))
            .orElse(false);

        var r2 = getAttribute(node, XmlReader.NAMESPACE_DDM, a2)
            .map(item -> v2.equals(item.getTextContent()))
            .orElse(false);

        return r1 && r2;
    }

    public static boolean isNotAbrPeriod(Node node) {
        return !isAbrPeriod(node);
    }

    public static boolean isAbrPeriod(Node node) {
        var a1 = hasAttributes(node,
            "subjectScheme", SCHEME_ABR_PERIOD,
            "schemeURI", SCHEME_URI_ABR_PERIOD
        );

        var a2 = hasAttributes(node,
            "subjectScheme", SCHEME_ABR_PLUS,
            "schemeURI", SCHEME_URI_ABR_PLUS
        );

        return a1 || a2;
    }

    private static String attributeToText(Node node, String namespace, String attribute) {
        return getAttribute(node, namespace, attribute)
            .map(Node::getTextContent)
            .orElseGet(() -> {
                log.error("Missing {} attribute on {} node", attribute, node.getNodeName());
                return null;
            });
    }

    public static String toAbrPeriod(Node node) {
        return attributeToText(node, XmlReader.NAMESPACE_DDM, "valueURI");
    }
}