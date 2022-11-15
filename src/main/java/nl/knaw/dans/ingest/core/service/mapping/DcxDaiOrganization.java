package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.DatasetOrganization;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;

public final class DcxDaiOrganization {
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
