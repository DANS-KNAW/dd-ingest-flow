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
import nl.knaw.dans.ingest.core.service.mapper.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_X;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_Y;

@Slf4j
public class SpatialPoint extends Spatial {

    public static CompoundFieldGenerator<Node> toEasyTsmSpatialPointValueObject = (builder, node) -> {
        var isRd = isRd(node);
        var point = getChildNode(node, "//Point")
            .map(n -> getPoint(n, isRd))
            .orElseThrow(() -> new RuntimeException(String.format("No point node found in node %s", node.getNodeName())));

        builder.addControlledSubfield(SPATIAL_POINT_SCHEME, isRd ? RD_SCHEME : LONLAT_SCHEME);
        builder.addSubfield(SPATIAL_POINT_X, point.getX());
        builder.addSubfield(SPATIAL_POINT_Y, point.getY());
    };
}
