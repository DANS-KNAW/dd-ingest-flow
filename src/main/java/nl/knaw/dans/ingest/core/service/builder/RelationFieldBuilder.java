package nl.knaw.dans.ingest.core.service.builder;

import org.w3c.dom.Node;

import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUDIENCE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.COLLECTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION;

public class RelationFieldBuilder extends FieldBuilder {

    public void addAudiences(Stream<String> nodes) {
        addMultipleStrings(AUDIENCE, nodes);
    }

    public void addCollections(Stream<String> values) {
        addMultipleStrings(COLLECTION, values);
    }

    public void addRelations(Stream<Node> stream, CompoundFieldGenerator<Node> generator) {
        addMultiple(RELATION, stream, generator);
    }
}
