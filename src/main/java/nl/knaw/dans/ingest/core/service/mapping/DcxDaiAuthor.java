package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.DatasetAuthor;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;
import java.util.List;
import java.util.Optional;

public final class DcxDaiAuthor extends Base {
    private static String getFirstValue(Node node, String expression) throws XPathExpressionException {
        return XPathEvaluator.strings(node, expression).map(String::trim).findFirst().orElse(null);
    }

    private static DatasetAuthor parseAuthor(Node node) throws XPathExpressionException {
        return DatasetAuthor.builder()
            .titles(getFirstValue(node, "dcx-dai:titles"))
            .initials(getFirstValue(node, "dcx-dai:initials"))
            .insertions(getFirstValue(node, "dcx-dai:insertions"))
            .surname(getFirstValue(node, "dcx-dai:surname"))
            .dai(getFirstValue(node, "dcx-dai:DAI"))
            .isni(getFirstValue(node, "dcx-dai:ISNI"))
            .orcid(getFirstValue(node, "dcx-dai:ORCID"))
            .role(getFirstValue(node, "dcx-dai:role"))
            .organization(getFirstValue(node, "dcx-dai:organization/dcx-dai:name"))
            .build();
    }

    public static boolean isRightsHolder(Node node) {
        try {
            var author = parseAuthor(node);
            return StringUtils.contains(author.getRole(), "RightsHolder");
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toRightsHolder(Node node) {
        try {
            var author = parseAuthor(node);
            return formatRightsHolder(author);
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    private static String formatRightsHolder(DatasetAuthor author) {
        if (author.getSurname() == null || author.getSurname().isBlank()) {
            return Optional.ofNullable(author.getOrganization()).orElse("");
        }

        return String.join(" ", List.of(
                Optional.ofNullable(author.getTitles()).orElse(""),
                Optional.ofNullable(author.getInitials()).orElse(""),
                Optional.ofNullable(author.getInsertions()).orElse(""),
                Optional.ofNullable(author.getSurname()).orElse(""),
                Optional.ofNullable(author.getOrganization()).orElse("")
            ))
            .trim().replaceAll("\\s+", " ");
    }

    //    private static String getFirstValue(Node node, String expression) throws XPathExpressionException {
    //        var nodes =node.getChildNodes();
    //        return xmlReader.xpathToStreamOfStrings(node, expression).map(String::trim).findFirst().orElse(null);
    //    }
    //
    //    private static DatasetAuthor parseAuthor(Node node) throws XPathExpressionException {
    //        return DatasetAuthor.builder()
    //            .titles(getFirstValue(node, "dcx-dai:titles"))
    //            .initials(getFirstValue(node, "dcx-dai:initials"))
    //            .insertions(getFirstValue(node, "dcx-dai:insertions"))
    //            .surname(getFirstValue(node, "dcx-dai:surname"))
    //            .dai(getFirstValue(node, "dcx-dai:DAI"))
    //            .isni(getFirstValue(node, "dcx-dai:ISNI"))
    //            .orcid(getFirstValue(node, "dcx-dai:ORCID"))
    //            .role(getFirstValue(node, "dcx-dai:role"))
    //            .organization(getFirstValue(node, "dcx-dai:organization/dcx-dai:name"))
    //            .build();
    //    }
}
