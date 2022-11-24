package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_END;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_START;
import static org.assertj.core.api.Assertions.assertThat;

class DatesOfCollectionTest extends BaseTest {

    @Test
    void testSplitCorrectlyFormattedDateRangeInStartAndEndSubFields() throws Exception {
        var doc = readDocumentFromString("<ddm:datesOfCollection xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    2022-01-01/2022-02-01\n"
            + "</ddm:datesOfCollection>\n");

        var builder = new CompoundFieldBuilder("", true);
        DatesOfCollection.toDistributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_START)
            .extracting("value")
            .containsOnly("2022-01-01");

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_END)
            .extracting("value")
            .containsOnly("2022-02-01");
    }

    @Test
    void testHandlesRangesWithoutStart() throws Exception {
        var doc = readDocumentFromString("<ddm:datesOfCollection xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    /2022-02-01\n"
            + "</ddm:datesOfCollection>\n");

        var builder = new CompoundFieldBuilder("", true);
        DatesOfCollection.toDistributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_START)
            .extracting("value")
            .containsOnly("");

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_END)
            .extracting("value")
            .containsOnly("2022-02-01");
    }

    @Test
    void testHandlesRangesWithoutEnd() throws Exception {
        var doc = readDocumentFromString("<ddm:datesOfCollection xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    2022-01-01/   \n"
            + "</ddm:datesOfCollection>\n");

        var builder = new CompoundFieldBuilder("", true);
        DatesOfCollection.toDistributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_START)
            .extracting("value")
            .containsOnly("2022-01-01");

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_END)
            .extracting("value")
            .containsOnly("");
    }
}