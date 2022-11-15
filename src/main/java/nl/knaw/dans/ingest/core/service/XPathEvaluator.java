package nl.knaw.dans.ingest.core.service;

import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;
import java.util.stream.Stream;

// TODO make singleton or something
public final class XPathEvaluator {

    private static XmlReader xmlReader;

    public static void init(XmlReader xmlReader) {
        XPathEvaluator.xmlReader = xmlReader;
    }

    public static synchronized Stream<Node> nodes(Node node, String... expressions) throws XPathExpressionException {
        return xmlReader.xpathsToStream(node, expressions);
    }

    public static synchronized Stream<String> strings(Node node, String... expressions) throws XPathExpressionException {
        return xmlReader.xpathsToStreamOfStrings(node, expressions);
    }

}
