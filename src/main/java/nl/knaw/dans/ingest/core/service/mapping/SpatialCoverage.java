package nl.knaw.dans.ingest.core.service.mapping;

import org.w3c.dom.Node;

import java.util.Set;

public class SpatialCoverage extends Base {

    private static Set<String> controlledValues = Set.of(
        "Netherlands",
        "United Kingdom",
        "Belgium",
        "Germany"
    );

    public static boolean hasNoChildElement(Node node) {
        return !hasChildElement(node);
    }

    public static boolean hasChildElement(Node node) {
        var children = node.getChildNodes();

        for (var i = 0; i < children.getLength(); ++i) {
            var e = children.item(i);

            // this means it has a child element that is an element
            if (e.getNodeType() == Node.ELEMENT_NODE) {
                return true;
            }
        }

        return false;
    }

    public static String toControlledSpatialValue(Node node) {
        var text = node.getTextContent().trim();
        return controlledValues.contains(text) ? text : null;
    }

    public static String toUncontrolledSpatialValue(Node node) {
        var text = node.getTextContent().trim();
        return controlledValues.contains(text) ? null : text;
    }
}
