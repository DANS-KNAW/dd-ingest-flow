package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_X;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_Y;

@Slf4j
public class SpatialPoint extends Spatial {

    // TODO test this with the right XML
    public static Map<String, MetadataField> toEasyTsmSpatialPointValueObject(Node node) {
        var result = new HashMap<String, MetadataField>();
        var isRd = SpatialPoint.isRd(node);
        var point = getChildNode(node, "/Point")
            .map(n -> getPoint(n, isRd))
            .orElseThrow(() -> new RuntimeException(String.format("No point node found in node %s", node.getNodeName())));

        result.put(SPATIAL_POINT_SCHEME, new ControlledSingleValueField(SPATIAL_POINT_SCHEME, isRd ? RD_SCHEME : LONLAT_SCHEME));
        result.put(SPATIAL_POINT_X, new PrimitiveSingleValueField(SPATIAL_POINT_X, point.getX()));
        result.put(SPATIAL_POINT_Y, new PrimitiveSingleValueField(SPATIAL_POINT_Y, point.getY()));

        return result;
    }

}
