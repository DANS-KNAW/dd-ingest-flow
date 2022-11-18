package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public final class Contributor extends Base {
    public static Map<String, String> contributorRoleToContributorType = new HashMap<>();

    public static CompoundFieldGenerator<Node> toAuthorValueObject = (builder, node) -> {
        getChildNode(node, "dcx-dai:author")
            .filter(n -> !DcxDaiAuthor.isRightsHolder(n))
            .ifPresent(n -> DcxDaiAuthor.toContributorValueObject.build(builder, node));

        getChildNode(node, "dcx-dai:organization")
            .filter(n -> !DcxDaiOrganization.isRightsHolderOrFunder(n))
            .ifPresent(n -> DcxDaiOrganization.toContributorValueObject.build(builder, node));

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

}
