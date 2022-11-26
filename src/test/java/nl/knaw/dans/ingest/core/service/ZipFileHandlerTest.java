/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    void wrap_if_zip_file() throws IOException {
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
    void needs_to_be_wrapped_ends_with_zip() throws Exception {
        var handler = new ZipFileHandler(Path.of("/tmp"));
        var spied = Mockito.spy(handler);

        Mockito.doReturn("unrelated")
            .when(spied).getMimeType(Mockito.any());

        assertTrue(spied.needsToBeWrapped(Path.of("test.zip")));
    }

    @Test
    void needs_to_be_wrapped_ends_with_non_zip() throws Exception {
        var handler = new ZipFileHandler(Path.of("/tmp"));
        var spied = Mockito.spy(handler);
        Mockito.doReturn("unrelated")
            .when(spied).getMimeType(Mockito.any());

        assertFalse(spied.needsToBeWrapped(Path.of("test.txt")));
    }

    @Test
    void needs_to_be_wrapped_ends_with_non_zip_but_has_correct_mimetype() throws Exception {
        var handler = new ZipFileHandler(Path.of("/tmp"));
        var spied = Mockito.spy(handler);
        Mockito.doReturn("application/zip")
            .when(spied).getMimeType(Mockito.any());

        assertTrue(spied.needsToBeWrapped(Path.of("test.txt")));
    }
}