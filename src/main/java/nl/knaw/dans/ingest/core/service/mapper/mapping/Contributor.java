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
import nl.knaw.dans.ingest.core.service.mapper.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public final class Contributor extends Base {
    public static Map<String, String> contributorRoleToContributorType = new HashMap<>();
    public static CompoundFieldGenerator<Node> toContributorValueObject = (builder, node) -> {
        getChildNode(node, "dcx-dai:author")
            .filter(n -> !DcxDaiAuthor.isRightsHolder(n))
            .ifPresent(n -> DcxDaiAuthor.toContributorValueObject.build(builder, n));

        getChildNode(node, "dcx-dai:organization")
            .filter(n -> !DcxDaiOrganization.isRightsHolderOrFunder(n))
            .ifPresent(n -> DcxDaiOrganization.toContributorValueObject.build(builder, n));

    };

    static {
        contributorRoleToContributorType.put("DataCurator", "Data Curator");
        contributorRoleToContributorType.put("DataManager", "Data Manager");
        contributorRoleToContributorType.put("Editor", "Editor");
        contributorRoleToContributorType.put("Funder", "Funder");
        contributorRoleToContributorType.put("HostingInstitution", "Hosting Institution");
        contributorRoleToContributorType.put("ProjectLeader", "Project Leader");
        contributorRoleToContributorType.put("ProjectManager", "Project Manager");
        contributorRoleToContributorType.put("Related Person", "Related Person");
        contributorRoleToContributorType.put("Researcher", "Researcher");
        contributorRoleToContributorType.put("ResearchGroup", "Research Group");
        contributorRoleToContributorType.put("RightsHolder", "Rights Holder");
        contributorRoleToContributorType.put("Sponsor", "Sponsor");
        contributorRoleToContributorType.put("Supervisor", "Supervisor");
        contributorRoleToContributorType.put("WorkPackageLeader", "Work Package Leader");
        contributorRoleToContributorType.put("Other", "Other");
        contributorRoleToContributorType.put("Producer", "Other");
        contributorRoleToContributorType.put("RegistrationAuthority", "Other");
        contributorRoleToContributorType.put("RegistrationAgency", "Other");
        contributorRoleToContributorType.put("Distributor", "Other");
        contributorRoleToContributorType.put("DataCollector", "Other");
        contributorRoleToContributorType.put("ContactPerson", "Other");
    }

    public static boolean isValidContributor(Node node) {

        var isValidAuthor = getChildNode(node, "dcx-dai:author")
            .filter(n -> !DcxDaiAuthor.isRightsHolder(n))
            .map(DcxDaiAuthor::isValidContributor)
            .orElse(false);

        var isValidOrg = getChildNode(node, "dcx-dai:organization")
            .filter(n -> !DcxDaiOrganization.isRightsHolderOrFunder(n))
            .map(DcxDaiOrganization::isValidContributor)
            .orElse(false);

        return isValidAuthor || isValidOrg;
    }

}
