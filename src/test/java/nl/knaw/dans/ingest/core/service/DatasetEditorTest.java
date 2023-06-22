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

import gov.loc.repository.bagit.reader.BagReader;
import nl.knaw.dans.ingest.core.dataverse.DataverseServiceImpl;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.DepositFile;
import nl.knaw.dans.ingest.core.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.service.mapper.mapping.BaseTest;
import nl.knaw.dans.ingest.core.service.mapper.mapping.FileElement;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DatasetEditorTest extends BaseTest {
    private final Path testDir = new File("target/test/" + getClass().getSimpleName()).toPath();
    String filesNS = String.format("xmlns='%s' xmlns:afm='%s' xmlns:dcterms='%s'",
        XmlNamespaces.NAMESPACE_FILES_XML, XmlNamespaces.NAMESPACE_AFM_XML, XmlNamespaces.NAMESPACE_DCTERMS);

    @BeforeEach
    void clear() {
        FileUtils.deleteQuietly(testDir.toFile());
    }

    private DatasetEditor createDatasetEditor(Deposit deposit, final boolean isMigration, final Pattern fileExclusionPattern, final List<URI> supportedLicenses) {
        var dataverseService = new DataverseServiceImpl(Mockito.mock(DataverseClient.class), 1, 1);
        var zipFileHandler = new ZipFileHandler(testDir.resolve("tmp"));
        return new DatasetEditor(isMigration, null, deposit, supportedLicenses, fileExclusionPattern, zipFileHandler, null, dataverseService) {

            @Override
            public String performEdit() {
                return null;
            }
        };
    }

    private Deposit createDeposit(String manifest) throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'/>"
            + "    <file filepath='data/subdir/file2.txt'/>"
            + "    <file filepath='data/subdir2/file3.txt'/>"
            + "</files>";

        return createDeposit(manifest, filesXml);
    }

    private Deposit createDeposit(String manifest, String filesXml) throws Exception {
        var bagDir = testDir.resolve("bag");
        var filesXmlElement = readDocumentFromString(filesXml);
        FileUtils.write(bagDir.resolve("bagit.txt").toFile(), (""
            + "BagIt-Version: 0.97\n"
            + "Tag-File-Character-Encoding: UTF-8\n"), StandardCharsets.UTF_8);
        FileUtils.write(bagDir.resolve("manifest-sha1.txt").toFile(), manifest, StandardCharsets.UTF_8);
        var deposit = new Deposit();
        deposit.setBagDir(bagDir);
        deposit.setBag(new BagReader().read(bagDir));
        deposit.setFilesXml(filesXmlElement);

        var manifestChecksums = Arrays.stream(manifest.split("\n"))
            .filter(StringUtils::isNotBlank)
            .map(row -> row.trim().split(" {2}", 2))
            .collect(Collectors.toMap(row -> Path.of(row[1]), row -> row[0]));

        var fileList = XPathEvaluator.nodes(filesXmlElement.getDocumentElement(), "/files:files/files:file")
            .map(item -> {
                var filepath = Path.of(item.getAttributes().getNamedItem("filepath").getTextContent());
                var checksum = manifestChecksums.getOrDefault(filepath, "default-checksum");
                return new DepositFile(filepath, filepath, checksum, item);
            })
            .collect(Collectors.toList());

        deposit.setFiles(fileList);
        return deposit;
    }

    @Test
    void getFileInfo_trims_and_filters_paths() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'/>"
            + "    <file filepath='data/subdir/file2.txt'/>"
            + "    <file filepath='data/subdir2/file3.txt'/>"
            + "</files>";

        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));

        var fileInfoMap = FileElement.pathToFileInfo(deposit, true);
        var filteredFileInfoMap = createDatasetEditor(deposit, true, Pattern.compile(".*file2.*"), null)
            .getFileInfo();

        assertThat(fileInfoMap.keySet()).containsExactlyInAnyOrder(
            Paths.get("data/file1.txt"),
            Paths.get("data/subdir/file2.txt"),
            Paths.get("data/subdir2/file3.txt"));
        assertThat(filteredFileInfoMap.keySet()).containsExactlyInAnyOrder(
            Paths.get("file1.txt"),
            Paths.get("subdir2/file3.txt"));
    }

    @Test
    void FIL001_FIL002_FIL003() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/' xmlns:dcterms='http://purl.org/dc/terms/'>"
            + "    <file filepath='data/subdir_υποφάκελο/c:a*q?d&quot;l&lt;g&gt;p|s;h#.txt'/>"
            + "</files>";
        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        var fileMeta = createDatasetEditor(deposit, true, null, null)
            .getFileInfo().values().iterator().next().getMetadata();
        assertThat(fileMeta)
            // FIL001
            .hasFieldOrPropertyWithValue("label", "c_a_q_d_l_g_p_s_h_.txt")
            // FIL002
            .hasFieldOrPropertyWithValue("directoryLabel", "subdir__________")
            // FIL003
            .hasFieldOrPropertyWithValue("description", "original_filepath: \"subdir_υποφάκελο/c:a*q?d\"l<g>p|s;h#.txt\"");
    }

    @Test
    void FIL002A_FIL003() throws Exception {
        var filesXml = ""
            + "<files " + filesNS + ">"
            + "    <file filepath='data/subdir/#.txt'>"
            + "        <afm:keyvaluepair>"
            + "            <afm:key>FOTONR</afm:key>"
            + "            <afm:value>3</afm:value>"
            + "        </afm:keyvaluepair>"
            + "        <afm:keyvaluepair>"
            + "            <afm:key>some</afm:key>"
            + "            <afm:value>thing</afm:value>"
            + "        </afm:keyvaluepair>"
            + "    </file>"
            + "</files>";
        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        var descriptions = createDatasetEditor(deposit, true, null, null)
            .getFileInfo().values().iterator().next().getMetadata().getDescription().split("; ");
        assertThat(descriptions).containsExactlyInAnyOrder(
            "some: \"thing\"",
            "original_filepath: \"subdir/#.txt\"",
            "FOTONR: \"3\"");
    }

    @Test
    void FIL002B_FIL003() throws Exception {
        var filesXml = ""
            + "<files " + filesNS + ">"
            + "    <file filepath='data/subdir/#.txt'>"
            + "        <dcterms:othmat_codebook>FOTOBEST.csv; FOTOLST.csv</dcterms:othmat_codebook>"
            + "    </file>"
            + "</files>";
        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        var description = createDatasetEditor(deposit, true, null, null)
            .getFileInfo().values().iterator().next().getMetadata().getDescription();
        assertThat(description)
            .contains("othmat_codebook: \"FOTOBEST.csv; FOTOLST.csv\"");
        assertThat(description.split("; "))
            .contains("original_filepath: \"subdir/#.txt\"");
    }

    @Test
    void FIL004_FIL003() throws Exception {
        var filesXml = ""
            + "<files " + filesNS + ">"
            + "    <file filepath='data/subdir/#.txt'>"
            + "        <dcterms:description>A file with a problematic name</dcterms:description>"
            + "    </file>"
            + "</files>";
        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        var fileMeta = createDatasetEditor(deposit, true, null, null)
            .getFileInfo().values().iterator().next().getMetadata();
        assertThat(fileMeta)
            .hasFieldOrPropertyWithValue("label", "_.txt")
            .hasFieldOrPropertyWithValue("description", "original_filepath: \"subdir/#.txt\"; description: \"A file with a problematic name\"");
    }

    @Test
    void FIL005_anonymous() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'>"
            + "        <accessibleToRights>ANONYMOUS</accessibleToRights>"
            + "    </file>"
            + "</files>";
        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        var fileInfo = createDatasetEditor(deposit, true, null, null)
            .getFileInfo().values().iterator().next();
        assertThat(fileInfo.getMetadata().getRestricted()).isEqualTo(false);
    }

    @Test
    void FIL005_not_anonymous() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'>"
            + "        <accessibleToRights>blabla</accessibleToRights>"
            + "    </file>"
            + "</files>";
        var deposit = createDeposit(""
            + "a5c5c4051724b655863c517a15c56e45753c3e5a  data/file1.txt\n"
            + "0d57a5bc9f5af7e8edcc90d64fd3c24dfc23e727  data/original-metadata.zip\n", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        // NOTE this test fails when replaced with FileElement.pathToFileInfo(deposit);
        var pathFileInfoMap = createDatasetEditor(deposit, true, null, null)
            .getFileInfo();
        assertThat(pathFileInfoMap.get(Paths.get("file1.txt")).getMetadata().getRestricted())
            .isEqualTo(true);
    }

    @Test
    void FIL006_open_access() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'/>"
            + "</files>";
        var deposit = createDeposit(""
            + "a5c5c4051724b655863c517a15c56e45753c3e5a  data/file1.txt\n"
            + "0d57a5bc9f5af7e8edcc90d64fd3c24dfc23e727  data/original-metadata.zip\n", filesXml);
        deposit.setDdm(readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "    <ddm:profile>"
            + "        <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>"
            + "    </ddm:profile>"
            + "</ddm:DDM>"));
        var fileInfo = createDatasetEditor(deposit, true, null, null)
            .getFileInfo().values().iterator().next();
        assertThat(fileInfo.getMetadata().getRestricted()).isEqualTo(false);
    }

    @Test
    void FIL006_not_open_access() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'/>"
            + "</files>";
        var deposit = createDeposit(""
            + "a5c5c4051724b655863c517a15c56e45753c3e5a  data/file1.txt\n"
            + "0d57a5bc9f5af7e8edcc90d64fd3c24dfc23e727  data/original-metadata.zip\n", filesXml);
        deposit.setDdm(readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "    <ddm:profile>"
            + "        <ddm:accessRights>blabla</ddm:accessRights>"
            + "    </ddm:profile>"
            + "</ddm:DDM>"));
        // NOTE this test fails when replaced with FileElement.pathToFileInfo(deposit);
        var pathFileInfoMap = createDatasetEditor(deposit, true, null, null)
            .getFileInfo();
        assertThat(pathFileInfoMap.get(Paths.get("file1.txt")).getMetadata().getRestricted())
            .isEqualTo(true);
    }

    @Test
    void FIL008_FIL009() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/'>"
            + "    <file filepath='data/file1.txt'/>"
            + "    <file filepath='data/easy-migration.zip'/>"
            + "</files>";
        var deposit = createDeposit("", filesXml);
        deposit.setDdm(readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "    <ddm:profile>"
            + "        <ddm:available>" + (new DateTime().plusWeeks(7)) + "</ddm:available>"
            + "    </ddm:profile>"
            + "</ddm:DDM>"));

        var datasetEditor = createDatasetEditor(deposit, true, null, null);
        // Calling embargoFiles to test these rules makes only sense once
        // the dataset is added to a real dataverse instance, turning it into an integration test.
        // The method is kept for documentational reasons.
        // Note that the file original-metadata.zip is added as not restricted.
    }

    @Test
    void fileInfoFields() throws Exception {
        var filesXml = ""
            + "<files xmlns='http://easy.dans.knaw.nl/schemas/bag/metadata/files/' xmlns:dcterms='http://purl.org/dc/terms/'>"
            + "    <file filepath='data/file1.txt'/>"
            + "    <file filepath='data/subdir/file2.txt'/>"
            + "    <file filepath='data/subdir_υποφάκελο/c:a*q?d&quot;l&lt;g&gt;p|s;h#.txt'/>"
            + "</files>";
        var deposit = createDeposit(""
            + "a5c5c4051724b655863c517a15c56e45753c3e5a  data/file1.txt\n"
            + "0d57a5bc9f5af7e8edcc90d64fd3c24dfc23e727  data/subdir/file2.txt\n"
            + "fa4cdb6b45c8a393aaca564ded8a52d62ee7a944  data/subdir_υποφάκελο/c:a*q?d\"l<g>p|s;h#.txt\n", filesXml);
        deposit.setDdm(readDocumentFromString("<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'/>"));
        var fileInfoMap = createDatasetEditor(deposit, true, null, null)
            .getFileInfo();

        var path2 = Paths.get("subdir/file2.txt");
        var path3 = Paths.get("subdir_υποφάκελο/c:a*q?d\"l<g>p|s;h#.txt");
        assertThat(fileInfoMap.get(path2))
            .hasFieldOrPropertyWithValue("checksum", "0d57a5bc9f5af7e8edcc90d64fd3c24dfc23e727")
            .hasFieldOrPropertyWithValue("path", Paths.get("target/test/DatasetEditorTest/bag/data/subdir/file2.txt"))
            .hasFieldOrPropertyWithValue("metadata.label", "file2.txt") // FIL001
            .hasFieldOrPropertyWithValue("metadata.directoryLabel", "subdir"); // FIL002
        assertThat(fileInfoMap.get(path3))
            .hasFieldOrPropertyWithValue("checksum", "fa4cdb6b45c8a393aaca564ded8a52d62ee7a944")
            .hasFieldOrPropertyWithValue("path", Paths.get("target/test/DatasetEditorTest/bag/data/" + path3))
            .hasFieldOrPropertyWithValue("metadata.label", "c_a_q_d_l_g_p_s_h_.txt"); // FIL001
        // more metadata fields in rule-specific tests
    }

    @Test
    void getDateAvailable() throws Exception {
        var deposit = new Deposit();
        deposit.setDdm(readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "    <ddm:profile>"
            + "        <ddm:available>2018-04-09</ddm:available>"
            + "    </ddm:profile>"
            + "</ddm:DDM>"));
        var editor = createDatasetEditor(deposit, true, null, null);
        Instant actual = editor.getDateAvailable(deposit);
        LocalDate actualLocalDate = LocalDate.ofInstant(actual, ZoneId.systemDefault());
        assertThat(actualLocalDate).isEqualTo(LocalDate.of(2018, 4, 9));
    }

    @Test
    void getDateAvailable_at_noon() throws Exception {
        var deposit = new Deposit();
        deposit.setDdm(readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "    <ddm:profile>"
            + "        <ddm:available>2018-04-09T12:00</ddm:available>"
            + "    </ddm:profile>"
            + "</ddm:DDM>"));
        var editor = createDatasetEditor(deposit, true, null, null);
        String actual = editor.getDateAvailable(deposit).toString()
            .replaceAll("T.*", "");
        assertThat(actual).isEqualTo("2018-04-09");
    }

    @Test
    void getDateAvailable_not_found() throws Exception {
        var deposit = new Deposit();
        deposit.setDdm(readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "</ddm:DDM>"));
        var editor = createDatasetEditor(deposit, true, null, null);
        assertThatThrownBy(() -> editor.getDateAvailable(deposit))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Deposit without a ddm:available element");
    }

    @Test
    void getLicense() throws Exception {
        String license = "http://opensource.org/licenses/MIT";
        var datasetXml = readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'"
            + "         xmlns:dcterms='http://purl.org/dc/terms/'"
            + "         xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + ">"
            + "    <ddm:dcmiMetadata>"
            + "        <dcterms:license xsi:type='dcterms:URI'>" + license + "</dcterms:license>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        List<URI> supportedLicenses = List.of(new URI(license));
        var editor = createDatasetEditor(new Deposit(), true, null, supportedLicenses);
        assertThat(editor.getLicense(datasetXml)).isEqualTo(license);
    }

    @Test
    void getLicense_not_supported() throws Exception {
        var datasetXml = readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'"
            + "         xmlns:dcterms='http://purl.org/dc/terms/'"
            + "         xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'"
            + ">"
            + "    <ddm:dcmiMetadata>"
            + "        <dcterms:license xsi:type='dcterms:URI'>http://opensource.org/licenses/MIT</dcterms:license>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        var editor = createDatasetEditor(new Deposit(), true, null, List.of());
        assertThatThrownBy(() -> editor.getLicense(datasetXml))
            .isInstanceOf(IllegalArgumentException.class) // TODO shouldn't this be RejectedDepositException?
            .hasMessage("Unsupported license: http://opensource.org/licenses/MIT");
    }

    @Test
    void getLicense_not_found() throws Exception {
        var datasetXml = readDocumentFromString(""
            + "<ddm:DDM xmlns:ddm='http://schemas.dans.knaw.nl/dataset/ddm-v2/'>"
            + "</ddm:DDM>");
        var editor = createDatasetEditor(new Deposit(), true, null, null);
        assertThatThrownBy(() -> editor.getLicense(datasetXml))
            .isInstanceOf(RejectedDepositException.class)
            .hasMessage("Rejected null: no license specified");
    }
}
