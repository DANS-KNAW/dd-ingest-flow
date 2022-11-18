package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_X;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_Y;

@Slf4j
public class SpatialPoint extends Spatial {

    // TODO test this with the right XML
    public static CompoundFieldGenerator<Node> toEasyTsmSpatialPointValueObject = (builder, node) -> {
        var isRd = SpatialPoint.isRd(node);
        var point = getChildNode(node, "/Point")
            .map(n -> getPoint(n, isRd))
            .orElseThrow(() -> new RuntimeException(String.format("No point node found in node %s", node.getNodeName())));

        builder.addControlledSubfield(SPATIAL_POINT_SCHEME, isRd ? RD_SCHEME : LONLAT_SCHEME);
        builder.addSubfield(SPATIAL_POINT_X, point.getX());
        builder.addSubfield(SPATIAL_POINT_Y, point.getY());
    };

}
