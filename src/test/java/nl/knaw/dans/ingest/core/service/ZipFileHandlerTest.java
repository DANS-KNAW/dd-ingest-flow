package nl.knaw.dans.ingest.core.service;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZipFileHandlerTest {

    @Test
    void wrapIfZipFile() throws IOException {
        var path = Path.of(Objects.requireNonNull(getClass().getResource("/zip/test.zip")).getPath());
        var handler = new ZipFileHandler(Path.of("/tmp"));

        Optional<Path> result = Optional.empty();

        try {
            result = handler.wrapIfZipFile(path);
            assertTrue(result.isPresent());
        } finally {
            // cleanup
            if (result.isPresent()) {
                Files.deleteIfExists(result.get());
            }
        }

    }

    @Test
    void needsToBeWrappedEndsWithZip() throws Exception {
        var handler = new ZipFileHandler(Path.of("/tmp"));
        var spied = Mockito.spy(handler);

        Mockito.doReturn("unrelated")
            .when(spied).getMimeType(Mockito.any());

        assertTrue(spied.needsToBeWrapped(Path.of("test.zip")));
    }

    @Test
    void needsToBeWrappedEndsWithNonZip() throws Exception {
        var handler = new ZipFileHandler(Path.of("/tmp"));
        var spied = Mockito.spy(handler);
        Mockito.doReturn("unrelated")
            .when(spied).getMimeType(Mockito.any());

        assertFalse(spied.needsToBeWrapped(Path.of("test.txt")));
    }

    @Test
    void needsToBeWrappedEndsWithNonZipButHasCorrectMimetype() throws Exception {
        var handler = new ZipFileHandler(Path.of("/tmp"));
        var spied = Mockito.spy(handler);
        Mockito.doReturn("application/zip")
            .when(spied).getMimeType(Mockito.any());

        assertTrue(spied.needsToBeWrapped(Path.of("test.txt")));
    }
}