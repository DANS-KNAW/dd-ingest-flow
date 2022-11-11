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
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DepositToDvDatasetMetadataMapperTest {

    private final Set<String> activeMetadataBlocks = Set.of("citation");
    private final XmlReader xmlReader = new XmlReaderImpl();

    Document readDocument(String name) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlFile(Path.of(
            Objects.requireNonNull(getClass().getResource(String.format("/xml/%s", name))).getPath()
        ));
    }

    Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlString(xml);
    }

    @Test
    void toDataverseDataset() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        mapper.toDataverseDataset(doc, null);
    }

    @Test
    void getTitles() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getTitles(doc);

        assertEquals("Title of the dataset", result.get(0));
    }

    @Test
    void getTitlesWithEmpty() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title xml:lang=\"en\"></dc:title>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>";

        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocumentFromString(xml);

        assertThrows(MissingRequiredFieldException.class, () -> mapper.getTitles(doc));
    }

    @Test
    void getAlternativeTitles() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getAlternativeTitles(doc);

        assertThat(result)
            .containsOnly("Alt title 1", "Alt title 2");

    }

    @Test
    void getOtherIdFromDataset() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getOtherIdFromDataset(doc);

        assertEquals(2, result.size());

        assertThat(result)
            .extracting(x -> x.get(OTHER_ID_VALUE))
            .extracting("value")
            .containsOnly("easy-dataset:18335", "easy-dataset:18337");
    }

    @Test
    void getAuthorCreators() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getAuthorCreators(doc);

        assertEquals(1, result.size());

        assertThat(result)
            .extracting(x -> x.get(AUTHOR_NAME))
            .extracting("value")
            .containsOnly("D.N. van den Aarden");

        assertThat(result)
            .extracting(x -> x.get(AUTHOR_IDENTIFIER))
            .extracting("value")
            .containsOnly("123456789");

        assertThat(result)
            .extracting(x -> x.get(AUTHOR_IDENTIFIER_SCHEME))
            .extracting("value")
            .containsOnly("DAI");
    }

    @Test
    void getOrganizationCreators() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getOrganizationCreators(doc);

        assertEquals(1, result.size());

        assertThat(result)
            .extracting(x -> x.get(AUTHOR_NAME))
            .extracting("value")
            .containsOnly("Anti-Vampire League");

        assertThat(result)
            .extracting(x -> x.get(AUTHOR_IDENTIFIER))
            .extracting("value")
            .containsOnly("isni");

        assertThat(result)
            .extracting(x -> x.get(AUTHOR_IDENTIFIER_SCHEME))
            .extracting("value")
            .containsOnly("ISNI");
    }

    @Test
    void getCreators() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getCreators(doc);
        assertEquals(1, result.size());

        assertThat(result)
            .extracting(AUTHOR_NAME)
            .extracting("value")
            .containsOnly("Bergman, W.A.");
    }

    @Test
    void getDescription() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getDescription(doc, DESCRIPTION);
        assertEquals(1, result.size());

        assertThat(result)
            .extracting(DESCRIPTION)
            .extracting("value")
            .containsOnly("<p>Lorem ipsum dolor sit amet,</p><p>consectetur adipiscing elit.<br>Lorem ipsum.</p>");
    }

    @Test
    void getNonTechnicalDescription() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getNonTechnicalDescription(doc, DESCRIPTION);
        assertEquals(1, result.size());

        assertThat(result)
            .extracting(DESCRIPTION)
            .extracting("value")
            .containsOnly("<p>Metadata description</p>");
    }

    @Test
    void getTechnicalDescription() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getTechnicalDescription(doc, DESCRIPTION);
        assertEquals(1, result.size());

        assertThat(result)
            .extracting(DESCRIPTION)
            .extracting("value")
            .containsOnly("<p>Technical metadata description</p>");
    }

    @Test
    void getOtherDescriptions() throws Exception {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        var doc = readDocument("dataset.xml");

        var result = mapper.getOtherDescriptions(doc, DESCRIPTION);
        assertEquals(7, result.size());

        assertThat(result)
            .extracting(DESCRIPTION)
            .extracting("value")
            .containsOnly(
                "Date: 2015-09-07",
                "Date Accepted: 2015-09-06",
                "Date Copyrighted: 2015-09-05",
                "Modified: 2015-09-08",
                "Issued: 2015-09-04",
                "Valid: yes",
                "Coverage: no"
            );
    }

    @Test
    void mapNarcisClassification() {
        var tests = new HashMap<String, String>();
        tests.put("D11000", "Mathematical Sciences");
        tests.put("D12300", "Physics");
        tests.put("D13200", "Chemistry");
        tests.put("D14320", "Engineering");
        tests.put("D16000", "Computer and Information Science");
        tests.put("D17000", "Astronomy and Astrophysics");
        tests.put("D18220", "Agricultural Sciences");
        tests.put("D22200", "Medicine, Health and Life Sciences");
        tests.put("D36000", "Arts and Humanities");
        tests.put("D41100", "Law");
        tests.put("D65000", "Social Sciences");
        tests.put("D42100", "Social Sciences");
        tests.put("D70100", "Business and Management");
        tests.put("D15300", "Earth and Environmental Sciences");

        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);

        for (var test : tests.entrySet()) {
            var result = mapper.mapNarcisClassification(test.getKey());
            assertEquals(test.getValue(), result);
        }
    }
    @Test
    void mapNarcisClassificationToOther() {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        assertEquals("Other", mapper.mapNarcisClassification("D99999"));
    }

    @Test
    void mapNarcisClassificationInvalid() {
        var mapper = new DepositToDvDatasetMetadataMapper(xmlReader, activeMetadataBlocks);
        assertThrows(RuntimeException.class, () -> mapper.mapNarcisClassification("INVALID"));
    }
}