package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;

public class Description extends Base {

    public static CompoundFieldGenerator<Node> toDescription = (builder, value) -> {
        var text = newlineToHtml(value.getTextContent());
        builder.addSubfield(DESCRIPTION_VALUE, text);
    };
    private static Map<String, String> labelToPrefix = Map.of(
        "date", "Date",
        "valid", "Valid",
        "issued", "Issued",
        "modified", "Modified",
        "dateAccepted", "Date Accepted",
        "dateCopyrighted", "Date Copyrighted",
        "dateSubmitted", "Date Submitted",
        "coverage", "Coverage"
    );
    public static CompoundFieldGenerator<Node> toPrefixedDescription = (builder, value) -> {
        var name = value.getLocalName();
        var prefix = labelToPrefix.getOrDefault(name, name);

        builder.addSubfield(DESCRIPTION_VALUE, String.format("%s: %s", prefix, value.getTextContent()));
    };

    private static String newlineToHtml(String value) {
        var newline = "\r\n|\n|\r";
        var paragraph = "(\r\n){2,}|\n{2,}|\r{2,}";

        return Arrays.stream(value.trim().split(paragraph))
            .map(String::trim)
            .map(p -> String.format("<p>%s</p>", p))
            .map(p -> Arrays.stream(p.split(newline))
                .map(String::trim)
                .collect(Collectors.joining("<br>")))
            .collect(Collectors.joining(""));
    }

    public static boolean isNonTechnicalInfo(Node node) {
        return !isTechnicalInfo(node);
    }

    public static boolean isTechnicalInfo(Node node) {
        return hasAttribute(node, "descriptionType", "TechnicalInfo");
    }
}
