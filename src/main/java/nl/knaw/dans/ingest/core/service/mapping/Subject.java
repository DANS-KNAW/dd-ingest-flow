package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import java.util.regex.Pattern;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VOCABULARY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VOCABULARY_URI;

@Slf4j
public class Subject extends Base {
    private final static String SCHEME_PAN = "PAN thesaurus ideaaltypes";
    private final static String SCHEME_URI_PAN = "https://data.cultureelerfgoed.nl/term/id/pan/PAN";

    private final static String SCHEME_AAT = "Art and Architecture Thesaurus";
    private final static String SCHEME_URI_AAT = "http://vocab.getty.edu/aat/";

    private final static Pattern matchPrefix = Pattern.compile("^\\s*[a-zA-Z]+\\s+Match:\\s*");
    public static CompoundFieldGenerator<Node> toKeywordValue = (builder, value) -> {
        builder.addSubfield(KEYWORD_VALUE, value.getTextContent());
        builder.addSubfield(KEYWORD_VOCABULARY, "");
        builder.addSubfield(KEYWORD_VOCABULARY_URI, "");
    };
    public static CompoundFieldGenerator<Node> toPanKeywordValue = (builder, value) -> {
        builder.addSubfield(KEYWORD_VALUE, removeMatchPrefix(value.getTextContent()));
        builder.addSubfield(KEYWORD_VOCABULARY, SCHEME_PAN);
        builder.addSubfield(KEYWORD_VOCABULARY_URI, SCHEME_URI_PAN);
    };
    public static CompoundFieldGenerator<Node> toAatKeywordValue = (builder, value) -> {
        builder.addSubfield(KEYWORD_VALUE, removeMatchPrefix(value.getTextContent()));
        builder.addSubfield(KEYWORD_VOCABULARY, SCHEME_PAN);
        builder.addSubfield(KEYWORD_VOCABULARY_URI, SCHEME_URI_PAN);
    };

    private static final String removeMatchPrefix(String input) {
        return matchPrefix.matcher(input).replaceAll("");
    }

    public static boolean hasNoCvAttributes(Node node) {
        var ss = getAttribute(node, "subjectScheme")
            .map(Node::getTextContent)
            .orElse("")
            .isEmpty();

        var su = getAttribute(node, "schemeURI")
            .map(Node::getTextContent)
            .orElse("")
            .isEmpty();

        return ss && su;
    }

    public static boolean isPanTerm(Node node) {
        return node.getLocalName().equals("subject")
            && hasAttribute(node, "subjectScheme", SCHEME_PAN)
            && hasAttribute(node, "schemeURI", SCHEME_URI_PAN);
    }

    public static boolean isAatTerm(Node node) {
        return node.getLocalName().equals("subject")
            && hasAttribute(node, "subjectScheme", SCHEME_AAT)
            && hasAttribute(node, "schemeURI", SCHEME_URI_AAT);
    }
}
