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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_RAPPORT_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_RAPPORT_TYPE;

@Slf4j
public class AbrReportType extends Base {

    public static boolean isAbrReportType(Node node) {
        return "reportNumber".equals(node.getLocalName())
            && hasAttributeValue(node, "subjectScheme", SCHEME_ABR_RAPPORT_TYPE)
            && hasAttributeValue(node, "schemeURI", SCHEME_URI_ABR_RAPPORT_TYPE);

    }

    public static String toAbrRapportType(Node node) {
        return getAttribute(node, "valueURI")
            .map(Node::getTextContent)
            .orElseGet(() -> {
                log.error("Missing valueURI attribute on ddm:reportNumber node");
                return null;
            });
    }
}
