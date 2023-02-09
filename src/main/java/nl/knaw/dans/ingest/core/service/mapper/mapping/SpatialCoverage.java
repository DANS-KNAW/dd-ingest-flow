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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import org.w3c.dom.Node;

import java.util.Set;

public class SpatialCoverage extends Base {

    private static final Set<String> controlledValues = Set.of(
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
