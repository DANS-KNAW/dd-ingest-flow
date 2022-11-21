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
import nl.knaw.dans.ingest.core.DatasetOrganization;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;
import java.util.Set;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_VALUE;
import static nl.knaw.dans.ingest.core.service.mapping.Contributor.contributorRoleToContributorType;

@Slf4j
public final class DcxDaiOrganization {

    public static CompoundFieldGenerator<Node> toAuthorValueObject = (builder, node) -> {
        try {
            var organization = parseOrganization(node);

            if (StringUtils.isNotBlank(organization.getName())) {
                builder.addSubfield(AUTHOR_NAME, organization.getName());
            }

            if (organization.getIsni() != null) {
                builder.addControlledSubfield(AUTHOR_IDENTIFIER_SCHEME, "ISNI");
                builder.addSubfield(AUTHOR_IDENTIFIER, organization.getIsni());
            }
            else if (organization.getViaf() != null) {
                builder.addControlledSubfield(AUTHOR_IDENTIFIER_SCHEME, "VIAF");
                builder.addSubfield(AUTHOR_IDENTIFIER, organization.getViaf());
            }
        }
        catch (XPathExpressionException e) {
            log.error("Xpath exception", e);
        }
    };

    public static CompoundFieldGenerator<Node> toContributorValueObject = (builder, node) -> {
        try {
            var organization = parseOrganization(node);

            if (StringUtils.isNotBlank(organization.getName())) {
                builder.addSubfield(CONTRIBUTOR_NAME, organization.getName());
            }
            if (StringUtils.isNotBlank(organization.getRole())) {
                var value = contributorRoleToContributorType.getOrDefault(organization.getRole(), "Other");
                builder.addSubfield(CONTRIBUTOR_TYPE, value);
            }
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException("Unable to parse author", e);
        }
    };

    public static CompoundFieldGenerator<Node> toGrantNumberValueObject = (builder, node) -> {
        try {
            var org = parseOrganization(node);
            builder.addSubfield(GRANT_NUMBER_AGENCY, org.getName().trim());
            builder.addSubfield(GRANT_NUMBER_VALUE, "");
        }
        catch (XPathExpressionException e) {
            log.error("Error parsing organization", e);
            throw new RuntimeException("Parse error", e);
        }
    };

    private static String getFirstValue(Node node, String expression) throws XPathExpressionException {
        return XPathEvaluator.strings(node, expression).map(String::trim).findFirst().orElse(null);
    }

    private static DatasetOrganization parseOrganization(Node node) throws XPathExpressionException {
        return DatasetOrganization.builder()
            .name(getFirstValue(node, "dcx-dai:name"))
            .role(getFirstValue(node, "dcx-dai:role"))
            .isni(getFirstValue(node, "dcx-dai:ISNI"))
            .viaf(getFirstValue(node, "dcx-dai:VIAF"))
            .build();
    }

    public static boolean isRightsHolderOrFunder(Node node) {
        try {
            var organization = parseOrganization(node);
            return Set.of("RightsHolder", "Funder").contains(organization.getRole());
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isRightsHolder(Node node) {
        try {
            var organization = parseOrganization(node);
            return StringUtils.contains(organization.getRole(), "RightsHolder");
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toRightsHolder(Node node) {
        try {
            var organization = parseOrganization(node);
            return organization.getName();
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isFunder(Node node) {
        try {
            var organization = parseOrganization(node);
            return StringUtils.contains(organization.getRole(), "Funder");
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }
}
