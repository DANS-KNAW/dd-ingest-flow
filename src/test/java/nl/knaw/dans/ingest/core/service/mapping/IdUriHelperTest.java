package nl.knaw.dans.ingest.core.service.mapping;

import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.mapping.IdUriHelper.reduceUriToOrcidId;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IdUriHelperTest {
    @Test
    void reduceUriToOrcidId_should_fix_input() {
        assertEquals("0000-0000-1234-567X", reduceUriToOrcidId("http://bla/0012-34567X"));
    }
    @Test
    void reduceUriToOrcidId_should_not_touch_garbage() {
        assertEquals("http://bla/001x2-34567X", reduceUriToOrcidId("http://bla/001x2-34567X"));
    }
}
