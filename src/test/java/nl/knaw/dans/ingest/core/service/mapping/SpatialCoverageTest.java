package nl.knaw.dans.ingest.core.service.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpatialCoverageTest extends BaseTest {

    @Test
    void hasChildElement() throws Exception {
        var doc = readDocumentFromString("<node><child>text</child> text</node>");
        var root = doc.getDocumentElement();
        assertTrue(SpatialCoverage.hasChildElement(root));
    }

    @Test
    void hasNoChildElement() throws Exception {
        var doc = readDocumentFromString("<node><child>text</child> text</node>");
        var root = doc.getDocumentElement().getFirstChild();
        assertFalse(SpatialCoverage.hasChildElement(root));
    }

    @Test
    void tesFieldBuilder() throws Exception {
        var builder = new CompoundFieldBuilder("TEST", true);
        builder.addSubfield("field1", "value1")
            .addSubfield("field2", "value2")
            .addControlledSubfield("field3", "x");

        builder.nextValue();
        builder.addSubfield("field1", "value3")
            .addSubfield("field2", "value4")
            .addControlledSubfield("field3", "x");

        builder.nextValue();
        var result = builder.build();
        System.out.println("RESULT: " + result);
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
        System.out.println("RESULT: " + str);
    }
}