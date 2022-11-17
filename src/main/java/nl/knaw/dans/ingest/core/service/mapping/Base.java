package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;
import java.util.Optional;

import static nl.knaw.dans.ingest.core.service.XmlReader.NAMESPACE_XSI;

@Slf4j
public class Base {
    static boolean hasXsiType(Node node, String xsiType) {
        var attributes = node.getAttributes();

        if (attributes == null) {
            return false;
        }

        return Optional.ofNullable(attributes.getNamedItemNS(NAMESPACE_XSI, "type"))
            .map(item -> {
                var text = item.getTextContent();
                return xsiType.equals(text) || text.endsWith(":" + xsiType);
            })
            .orElse(false);
    }

    static Optional<Node> getAttribute(Node node, String name) {
        return Optional.ofNullable(node.getAttributes())
            .map(a -> Optional.ofNullable(a.getNamedItem(name)))
            .flatMap(i -> i);
    }

    static Optional<Node> getAttribute(Node node, String namespaceURI, String name) {
        return Optional.ofNullable(node.getAttributes())
            .map(a -> Optional.ofNullable(a.getNamedItemNS(namespaceURI, name)))
            .flatMap(i -> i);
    }

    static boolean hasAttribute(Node node, String name, String value) {
        return getAttribute(node, name)
            .map(n -> StringUtils.equals(value, n.getTextContent()))
            .orElse(false);

    }

    static boolean hasAttribute(Node node, String namespaceURI, String name, String value) {
        return getAttribute(node, namespaceURI, name)
            .map(n -> StringUtils.equals(value, n.getTextContent()))
            .orElse(false);

    }

    public static String asText(Node node) {
        return node.getTextContent();
    }

    public static boolean hasChildNode(Node node, String xpath) {
        return getChildNode(node, xpath).isPresent();
    }

    public static Optional<Node> getChildNode(Node node, String xpath) {
        try {
            return XPathEvaluator.nodes(node, xpath).findAny();
        }
        catch (XPathExpressionException e) {
            log.error("Error evaluation XPath expression {}", xpath, e);
            return Optional.empty();
        }
    }
}
