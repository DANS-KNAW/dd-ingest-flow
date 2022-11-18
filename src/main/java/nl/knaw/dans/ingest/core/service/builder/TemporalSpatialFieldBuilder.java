package nl.knaw.dans.ingest.core.service.builder;

import org.w3c.dom.Node;

import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_BOX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_COVERAGE_CONTROLLED;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_COVERAGE_UNCONTROLLED;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SPATIAL_POINT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TEMPORAL_COVERAGE;

public class TemporalSpatialFieldBuilder extends FieldBuilder {

    public void addTemporalCoverage(Stream<String> nodes) {
        addMultipleStrings(TEMPORAL_COVERAGE, nodes);
    }

    public void addSpatialPoint(Stream<Node> stream, CompoundFieldGenerator<Node> generator) {
        addMultiple(SPATIAL_POINT, stream, generator);
    }

    public void addSpatialBox(Stream<Node> stream, CompoundFieldGenerator<Node> generator) {
        addMultiple(SPATIAL_BOX, stream, generator);
    }

    public void addSpatialCoverageControlled(Stream<String> stream) {
        addMultipleControlledFields(SPATIAL_COVERAGE_CONTROLLED, stream);
    }

    public void addSpatialCoverageUncontrolled(Stream<String> nodes) {
        addMultipleStrings(SPATIAL_COVERAGE_UNCONTROLLED, nodes);
    }

}
