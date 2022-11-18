package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import java.util.regex.Pattern;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_END;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_START;

@Slf4j
public class DatesOfCollection extends Base {

    private static final Pattern DATES_OF_COLLECTION_PATTERN = Pattern.compile("^(.*)/(.*)$");

    public static CompoundFieldGenerator<Node> toDistributorValueObject = (builder, node) -> {
        var matches = DATES_OF_COLLECTION_PATTERN.matcher(node.getTextContent().trim());

        if (matches.matches()) {
            builder.addSubfield(DATE_OF_COLLECTION_START, matches.group(1));
            builder.addSubfield(DATE_OF_COLLECTION_END, matches.group(2));
        }
    };

    public static boolean isValidDistributorDate(Node node) {
        var text = node.getTextContent().trim();
        var matcher = DATES_OF_COLLECTION_PATTERN.matcher(text.trim());

        return matcher.matches();
    }
}
