/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.ingest.core.service;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

// TODO make singleton or something
public final class XPathEvaluator {

    public static final String NAMESPACE_XML = "http://www.w3.org/XML/1998/namespace";
    public static final String NAMESPACE_DC = "http://purl.org/dc/elements/1.1/";
    public static final String NAMESPACE_DCX_DAI = "http://easy.dans.knaw.nl/schemas/dcx/dai/";
    public static final String NAMESPACE_DDM = "http://easy.dans.knaw.nl/schemas/md/ddm/";
    public static final String NAMESPACE_DCTERMS = "http://purl.org/dc/terms/";
    public static final String NAMESPACE_XSI = "http://www.w3.org/2001/XMLSchema-instance";
    public static final String NAMESPACE_ID_TYPE = "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/";
    public static final String NAMESPACE_DCX_GML = "http://easy.dans.knaw.nl/schemas/dcx/gml/";
    public static final String NAMESPACE_FILES_XML = "http://easy.dans.knaw.nl/schemas/bag/metadata/files/";
    public static final String NAMESPACE_OPEN_GIS = "http://www.opengis.net/gml";

    private static XPath xpath;

    private static XPath getXpath() {
        if (xpath == null) {
            xpath = XPathFactory
                .newInstance()
                .newXPath();

            final var namespaceMap = Map.of(
                "xml", NAMESPACE_XML,
                "dc", NAMESPACE_DC,
                "dcx-dai", NAMESPACE_DCX_DAI,
                "ddm", NAMESPACE_DDM,
                "dcterms", NAMESPACE_DCTERMS,
                "xsi", NAMESPACE_XSI,
                "id-type", NAMESPACE_ID_TYPE,
                "dcx-gml", NAMESPACE_DCX_GML,
                "files", NAMESPACE_FILES_XML,
                "gml", NAMESPACE_OPEN_GIS
            );

            xpath.setNamespaceContext(new NamespaceContext() {

                @Override
                public String getNamespaceURI(String s) {
                    return namespaceMap.get(s);
                }

                @Override
                public String getPrefix(String s) {
                    return null;
                }

                @Override
                public Iterator<String> getPrefixes(String s) {
                    return null;
                }
            });
        }

        return xpath;
    }

    public static Stream<Node> nodes(Node node, String... expressions) {
        try {
            return xpathsToStream(node, expressions);
        }
        catch (XPathExpressionException e) {
            // TODO add actual expression to error message
            throw new RuntimeException("Error evaluating xpath", e);
        }
    }

    public static Stream<String> strings(Node node, String... expressions) {
        try {
            return xpathsToStreamOfStrings(node, expressions);
        }
        catch (XPathExpressionException e) {
            // TODO add actual expression to error message
            throw new RuntimeException("Error evaluating xpath", e);
        }
    }

    private static synchronized Object evaluateXpath(Node node, String expr, QName type) throws XPathExpressionException {
        return getXpath().compile(expr).evaluate(node, type);
    }

    private static Stream<Node> xpathToStream(Node node, String expression) throws XPathExpressionException {
        var nodes = (NodeList) evaluateXpath(node, expression, XPathConstants.NODESET);

        return IntStream.range(0, nodes.getLength())
            .mapToObj(nodes::item);
    }

    private static Stream<Node> xpathsToStream(Node node, String... expressions) throws XPathExpressionException {
        var items = new ArrayList<Stream<Node>>();

        for (var expr : expressions) {
            var item = xpathToStream(node, expr);
            items.add(item);
        }

        return items.stream().flatMap(i -> i);
    }

    private static Stream<String> xpathsToStreamOfStrings(Node node, String... expressions) throws XPathExpressionException {
        return xpathsToStream(node, expressions).map(Node::getTextContent);
    }
}
