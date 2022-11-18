package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_EAST;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_NORTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_SOUTH;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX_WEST;

@Slf4j
public class SpatialBox extends Spatial {
    public static CompoundFieldGenerator<Node> toEasyTsmSpatialBoxValueObject = (builder, node) -> {
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

        builder.addControlledSubfield(SPATIAL_BOX_SCHEME, isRd ? RD_SCHEME : LONLAT_SCHEME);
        builder.addSubfield(SPATIAL_BOX_NORTH, upperCorner.getY());
        builder.addSubfield(SPATIAL_BOX_EAST, upperCorner.getX());
        builder.addSubfield(SPATIAL_BOX_SOUTH, lowerCorner.getY());
        builder.addSubfield(SPATIAL_BOX_WEST, lowerCorner.getX());
    };

}
