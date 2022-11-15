package nl.knaw.dans.ingest.core.service.mapping;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class IdentifierTest extends BaseTest {

    @Test
    void testToArchisNumberValue() throws Exception {
        var ddm = readDocument("dataset.xml");
        var ids = xmlReader.xpathToStream(ddm, "//ddm:dcmiMetadata/dcterms:identifier");

        var results = ids
            .filter(Identifier::isArchisZaakId)
            .map(Identifier::toArchisNumberValue)
            .collect(Collectors.toList());

        assertThat(results)
            .extracting(ARCHIS_NUMBER_TYPE)
            .extracting("value")
            .containsOnly("monument");

    }
}