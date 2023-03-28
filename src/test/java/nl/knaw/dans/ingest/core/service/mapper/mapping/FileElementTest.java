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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.DepositFile;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileElementTest extends BaseTest {

    @Test
    void toFileMetadata_should_include_metadata_from_child_elements() throws Exception {
        var doc = readDocumentFromString(
            "<file filepath=\"data/leeg.txt\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    <dcterms:format>text/plain</dcterms:format>\n"
                + "    <dcterms:hardware>Hardware</dcterms:hardware>\n"
                + "    <dcterms:description>Empty file</dcterms:description>\n"
                + "    <dcterms:time_period>Classical</dcterms:time_period>\n"
                + "</file>");

        var result = FileElement.toFileMeta(doc.getDocumentElement(), true);

        assertEquals("leeg.txt", result.getLabel());
        assertEquals(" ", result.getDirectoryLabel());
        assertEquals("description: \"Empty file\"; time_period: \"Classical\"; hardware: \"Hardware\"", result.getDescription());
        assertEquals(true, result.getRestricted());
    }

    @Test
    void toFileMetadata_should_strip_data_prefix_from_path_to_get_directoryLabel() throws Exception {
        var doc = readDocumentFromString(
            "    <file filepath=\"data/this/is/the/directory/label/leeg.txt\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    </file>");

        var result = FileElement.toFileMeta(doc.getDocumentElement(), true);
        assertEquals("leeg.txt", result.getLabel());
        assertEquals("this/is/the/directory/label", result.getDirectoryLabel());
        assertTrue(result.getRestricted());
    }

    @Test
    void toFileMetadata_should_represent_keyvalue_pairs_in_the_description() throws Exception {
        var doc = readDocumentFromString(
            "    <file filepath=\"data/this/is/the/directory/label/leeg.txt\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:afm=\"http://easy.dans.knaw.nl/schemas/bag/metadata/afm/\">\n"
                + "       <afm:othmat_codebook>FOTOBEST.csv; FOTOLST.csv</afm:othmat_codebook>"
                + "       <afm:keyvaluepair>\n"
                + "             <afm:key>FOTONR</afm:key>\n"
                + "             <afm:value>3</afm:value>\n"
                + "       </afm:keyvaluepair>"
                + "    </file>");

        var result = FileElement.toFileMeta(doc.getDocumentElement(), true);
        assertEquals("leeg.txt", result.getLabel());
        assertEquals("this/is/the/directory/label", result.getDirectoryLabel());
        assertTrue(result.getRestricted());
        assertEquals("othmat_codebook: \"FOTOBEST.csv; FOTOLST.csv\"; FOTONR: \"3\"", result.getDescription()); // FIL002A/B (migration only)
    }

    @Test
    void toFileMetadata_should_include_original_filepath_if_directoryLabel_or_label_change_during_sanitation() throws Exception {
        var doc = readDocumentFromString(
            "    <file filepath=\"data/directory/path/with/&lt;for'bidden&gt;/(chars)/strange?filename*.txt\" "
                + "         xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" "
                + "         xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    </file>");

        var result = FileElement.toFileMeta(doc.getDocumentElement(), true);
        assertEquals("strange_filename_.txt", result.getLabel());
        assertEquals("directory/path/with/_for_bidden_/_chars_", result.getDirectoryLabel());
        assertTrue(result.getRestricted());
        assertEquals("original_filepath: \"directory/path/with/<for'bidden>/(chars)/strange?filename*.txt\"", result.getDescription());
    }

    @Test
    void toFileMetadata_should_NOT_include_original_filepath_if_directoryLabel_or_label_stay_unchanged_during_sanitation() throws Exception {
        var doc = readDocumentFromString(
            "    <file filepath=\"data/directory/path/with/all/legal/chars/normal_filename.txt\""
                + "         xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" "
                + "         xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    </file>");
        var result = FileElement.toFileMeta(doc.getDocumentElement(), true);
        assertEquals("normal_filename.txt", result.getLabel());
        assertEquals("directory/path/with/all/legal/chars", result.getDirectoryLabel());
        assertTrue(result.getRestricted());
        assertNull(result.getDescription());
    }

    @Test
    void toFileMetadata_should_only_replace_nonASCII_chars_in_directory_names_during_sanitization() throws Exception {
        var originalFilePath = "data/directory/path/with/all/leg\u00e5l/chars/n\u00f8rmal_filename.txt";
        var doc = readDocumentFromString(String.format(
            "    <file filepath=\"%s\""
                + "         xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" "
                + "         xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    </file>", originalFilePath));

        var result = FileElement.toFileMeta(doc.getDocumentElement(), true);
        assertEquals("n\u00f8rmal_filename.txt", result.getLabel());
        assertEquals("directory/path/with/all/leg_l/chars", result.getDirectoryLabel());
        assertTrue(result.getRestricted());
        assertEquals("original_filepath: \"directory/path/with/all/leg\u00e5l/chars/n\u00f8rmal_filename.txt\"", result.getDescription());
    }

    @Test
    void replaceForbiddenCharactersInFilename_should_replace_each_char_with_underscore() {
        /*
        Replace forbidden chars with underscore. (Forbidden chars are:
        : (colon)
        * (asterisk)
        ? (question mark)
        "" (double quote)
        < (lower than)
        > (greater than)
        | (pipe)
        ; (semicolon)
        # (hash)
        */
        // note that there are 7 invalid characters between 'test' and '.txt'
        var filename = "test**::?>>.txt";
        var result = FileElement.replaceForbiddenCharactersInFilename(filename);

        assertEquals("test_______.txt", result);
    }

    @Test
    void replaceForbiddenCharactersInPath_should_replace_each_char_with_underscore() {
        /*
        Replace forbidden chars with underscore. Only the following characters are allowed:
        alphanumeric chars (only ASCII)
        / slash
        \ backslash
        . dot
        - hyphen
         space (but not tab)
        */
        var filename = "dir()\t\t ^^^/xyz/\\a.b-c";
        var result = FileElement.replaceForbiddenCharactersInPath(filename);

        assertEquals("dir____ ___/xyz/\\a.b-c", result);
    }

    @Test
    void pathToFileInfo_should_return_same_path_for_physical_and_normal() throws Exception {
        var doc = readDocumentFromString(
            "<file filepath=\"data/path/to/file1.txt\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    <dcterms:format>text/plain</dcterms:format>\n"
                + "    <dcterms:hardware>Hardware</dcterms:hardware>\n"
                + "    <dcterms:description>Empty file</dcterms:description>\n"
                + "    <dcterms:time_period>Classical</dcterms:time_period>\n"
                + "</file>");

        var filePath = Path.of("data/path/to/file1.txt");
        var deposit = new Deposit();
        deposit.setBagDir(Path.of("bagdir"));
        deposit.setFiles(List.of(
                new DepositFile(filePath, null, "check1", doc.getDocumentElement())
            )
        );

        deposit.setDdm(readDocumentFromString("<root></root>"));

        var result = FileElement.pathToFileInfo(deposit);
        assertEquals(result.get(filePath).getPath(), result.get(filePath).getPhysicalPath());
    }

    @Test
    void pathToFileInfo_should_store_physical_path_if_available() throws Exception {
        var doc = readDocumentFromString(
            "<file filepath=\"data/path/to/file1.txt\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
                + "    <dcterms:format>text/plain</dcterms:format>\n"
                + "    <dcterms:hardware>Hardware</dcterms:hardware>\n"
                + "    <dcterms:description>Empty file</dcterms:description>\n"
                + "    <dcterms:time_period>Classical</dcterms:time_period>\n"
                + "</file>");

        var filePath = Path.of("data/path/to/file1.txt");
        var deposit = new Deposit();
        deposit.setBagDir(Path.of("bagdir"));
        deposit.setFiles(List.of(
                new DepositFile(filePath, Path.of("data/new-file-name"), "check1", doc.getDocumentElement())
            )
        );

        deposit.setDdm(readDocumentFromString("<root></root>"));

        var result = FileElement.pathToFileInfo(deposit);
        assertEquals(Path.of("bagdir/data/new-file-name"), result.get(filePath).getPhysicalPath());
    }
}