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

@Slf4j
public class AbrReportType extends Base {
    private static final String ABR_BASE_URL = "https://data.cultureelerfgoed.nl/term/id/abr";

    private static final String SCHEME_ABR_OLD = "Archeologisch Basis Register";
    private static final String SCHEME_URI_ABR_OLD = "https://data.cultureelerfgoed.nl/term/id/rn/a4a7933c-e096-4bcf-a921-4f70a78749fe";

    private static final String SCHEME_ABR_PLUS = "Archeologisch Basis Register";
    private static final String SCHEME_URI_ABR_PLUS = "https://data.cultureelerfgoed.nl/term/id/abr/b6df7840-67bf-48bd-aa56-7ee39435d2ed";

    private static final String SCHEME_ABR_COMPLEX = "ABR Complextypen";
    private static final String SCHEME_URI_ABR_COMPLEX = "https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0";

    private static final String SCHEME_ABR_ARTIFACT = "ABR Artefacten";
    private static final String SCHEME_URI_ABR_ARTIFACT = "https://data.cultureelerfgoed.nl/term/id/abr/22cbb070-6542-48f0-8afe-7d98d398cc0b";

    private static final String SCHEME_ABR_PERIOD = "ABR Periodes";
    private static final String SCHEME_URI_ABR_PERIOD = "https://data.cultureelerfgoed.nl/term/id/abr/9b688754-1315-484b-9c89-8817e87c1e84";

    private static final String SCHEME_ABR_RAPPORT_TYPE = "ABR Rapporten";
    private static final String SCHEME_URI_ABR_RAPPORT_TYPE = "https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e";

    private static final String SCHEME_ABR_VERWERVINGSWIJZE = "ABR verwervingswijzen";
    private static final String SCHEME_URI_ABR_VERWERVINGSWIJZE = "https://data.cultureelerfgoed.nl/term/id/abr/554ca1ec-3ed8-42d3-ae4b-47bcb848b238";

    public static boolean isAbrReportType(Node node) {
        return "reportNumber".equals(node.getLocalName())
            && hasAttributeValue(node, XmlReader.NAMESPACE_DDM, "subjectScheme", SCHEME_ABR_RAPPORT_TYPE)
            && hasAttributeValue(node, XmlReader.NAMESPACE_DDM, "schemeURI", SCHEME_URI_ABR_RAPPORT_TYPE);

    }

    public static String toAbrRapportType(Node node) {
        return getAttribute(node, XmlReader.NAMESPACE_DDM, "valueURI")
            .map(Node::getTextContent)
            .orElseGet(() -> {
                log.error("Missing valueURI attribute on ddm:reportNumber node");
                return null;
            });
    }
}
