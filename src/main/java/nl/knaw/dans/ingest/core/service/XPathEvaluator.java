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
