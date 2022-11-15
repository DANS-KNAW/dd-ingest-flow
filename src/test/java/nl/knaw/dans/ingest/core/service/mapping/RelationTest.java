package nl.knaw.dans.ingest.core.service.mapping;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_URI;
import static org.assertj.core.api.Assertions.assertThat;

class RelationTest extends BaseTest {

    @Test
    void testToRelationObject() throws Exception {
        var doc = readDocument("dataset.xml");
        var items = xmlReader.xpathToStream(doc, "//ddm:dcmiMetadata//*")
            .filter(Relation::isRelation)
            .map(Relation::toRelationObject)
            .collect(Collectors.toList());

        assertThat(items)
            .extracting(RELATION)
            .extracting("value")
            .containsOnly("relation", "conforms to");

        assertThat(items)
            .extracting(RELATION_URI)
            .extracting("value")
            .containsOnly("", "https://knaw.nl/");

        assertThat(items)
            .extracting(RELATION_TEXT)
            .extracting("value")
            .containsOnly("Relation", "Conforms To");
    }
}