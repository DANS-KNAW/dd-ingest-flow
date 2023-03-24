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

import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Node;

import java.util.Optional;

@Slf4j
public final class InCollection extends Base {
    private static final String valueURI = "valueURI";

    public static String toCollection(Node node) {
        return Optional.ofNullable(node.getAttributes())
            .map(n -> Optional.ofNullable(n.getNamedItem(valueURI)))
            .flatMap(i -> i)
            .map(Node::getTextContent)
            .orElseThrow(() -> new IllegalArgumentException(String.format("Missing attribute %s on ddm:inCollection node", valueURI)));
    }
}
