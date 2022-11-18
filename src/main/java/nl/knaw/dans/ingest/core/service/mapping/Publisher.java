package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import java.util.Set;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_ABBREVIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_LOGO_URL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_URL;

@Slf4j
public class Publisher extends Base {

    public static CompoundFieldGenerator<Node> toDistributorValueObject = (builder, node) -> {
        builder.addSubfield(DISTRIBUTOR_NAME, node.getTextContent());
        builder.addSubfield(DISTRIBUTOR_URL, "");
        builder.addSubfield(DISTRIBUTOR_LOGO_URL, "");
        builder.addSubfield(DISTRIBUTOR_ABBREVIATION, "");
        builder.addSubfield(DISTRIBUTOR_AFFILIATION, "");
    };

    private static Set<String> dansNames = Set.of("DANS", "DANS-KNAW", "DANS/KNAW");

    public static boolean isDans(Node node) {
        return dansNames.contains(node.getTextContent());
    }

    public static boolean isNotDans(Node node) {
        return !isDans(node);
    }
}
