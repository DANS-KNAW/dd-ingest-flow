package nl.knaw.dans.ingest.core.service.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_EAST;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_NORTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SOUTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_WEST;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT_SCHEME;

@Slf4j
public class SpatialBox extends Spatial {

    public static Map<String, MetadataField> toEasyTsmSpatialBoxValueObject(Node node) {
        var result = new HashMap<String, MetadataField>();
        var envelope = getChildNode(node, "gml:Envelope")
            // TODO is it a requirement for the envelope to be there? what should happen if it is missing?
            // does the validate-dans-bag thing validate the existence of this?
            .orElseThrow();

        var isRd = isRd(envelope);

        var lowerCorner = getChildNode(envelope, "gml:lowerCorner")
            .map(n -> getPoint(n, isRd))
            // TODO check if this should be caught
            .orElseThrow();

        var upperCorner = getChildNode(envelope, "gml:upperCorner")
            .map(n -> getPoint(n, isRd))
            // TODO check if this should be caught
            .orElseThrow();

        result.put(SPATIAL_BOX_SCHEME, new ControlledSingleValueField(SPATIAL_POINT_SCHEME, isRd ? RD_SCHEME : LONLAT_SCHEME));
        result.put(SPATIAL_BOX_NORTH, new PrimitiveSingleValueField(SPATIAL_BOX_NORTH, upperCorner.getY()));
        result.put(SPATIAL_BOX_EAST, new PrimitiveSingleValueField(SPATIAL_BOX_EAST, upperCorner.getX()));
        result.put(SPATIAL_BOX_SOUTH, new PrimitiveSingleValueField(SPATIAL_BOX_SOUTH, lowerCorner.getY()));
        result.put(SPATIAL_BOX_WEST, new PrimitiveSingleValueField(SPATIAL_BOX_WEST, lowerCorner.getX()));

        return result;
    }

}
