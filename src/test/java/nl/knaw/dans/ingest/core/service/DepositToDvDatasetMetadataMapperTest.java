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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DepositToDvDatasetMetadataMapperTest {

    private final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpation", "dansDataVaultMetadata");
    private final XmlReader xmlReader = new XmlReaderImpl();

    private final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();

    Document readDocument(String name) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlFile(Path.of(
            Objects.requireNonNull(getClass().getResource(String.format("/xml/%s", name))).getPath()
        ));
    }

    Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlString(xml);
    }

    DepositToDvDatasetMetadataMapper getMapper() {
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage
        );
    }

    @BeforeEach
    void setUp() {
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
    }

    @Test
    void toDataverseDataset() throws Exception {
        var mapper = getMapper();
        var doc = readDocument("dataset.xml");

        var vaultMetadata = new VaultMetadata("pid", "bagId", "nbn", "otherId:something", "otherIdVersion", "swordToken");

        var result = mapper.toDataverseDataset(doc, null, null, null, null, vaultMetadata);
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);

        System.out.println("STR: " + str);
    }
    //
    //    @Test
    //    void getTitles() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getTitles(doc)
    //            .collect(Collectors.toList());
    //
    //        assertEquals("Title of the dataset", result.get(0));
    //    }
    //
    //    @Test
    //    void getTitlesWithEmpty() throws Exception {
    //        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
    //            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n"
    //            + "    <ddm:profile>\n"
    //            + "        <dc:title xml:lang=\"en\"></dc:title>\n"
    //            + "    </ddm:profile>\n"
    //            + "</ddm:DDM>";
    //
    //        var mapper = getMapper();
    //        var doc = readDocumentFromString(xml);
    //
    //        assertThrows(MissingRequiredFieldException.class, () -> mapper.getTitles(doc));
    //    }
    //
    //    @Test
    //    void getAlternativeTitles() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getAlternativeTitles(doc);
    //
    //        assertThat(result)
    //            .containsOnly("Alt title 1", "Alt title 2");
    //
    //    }
    //
    //    @Test
    //    void getOtherIdFromDataset() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getOtherIdFromDataset(doc);
    //
    //        assertEquals(2, result.size());
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(OTHER_ID_VALUE))
    //            .extracting("value")
    //            .containsOnly("easy-dataset:18335", "easy-dataset:18337");
    //    }
    //
    //    @Test
    //    void getAuthorCreators() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getAuthorCreators(doc);
    //
    //        assertEquals(1, result.size());
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(AUTHOR_NAME))
    //            .extracting("value")
    //            .containsOnly("D.N. van den Aarden");
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(AUTHOR_IDENTIFIER))
    //            .extracting("value")
    //            .containsOnly("123456789");
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(AUTHOR_IDENTIFIER_SCHEME))
    //            .extracting("value")
    //            .containsOnly("DAI");
    //    }
    //
    //    @Test
    //    void getOrganizationCreators() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getOrganizationCreators(doc);
    //
    //        assertEquals(1, result.size());
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(AUTHOR_NAME))
    //            .extracting("value")
    //            .containsOnly("Anti-Vampire League");
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(AUTHOR_IDENTIFIER))
    //            .extracting("value")
    //            .containsOnly("isni");
    //
    //        assertThat(result)
    //            .extracting(x -> x.get(AUTHOR_IDENTIFIER_SCHEME))
    //            .extracting("value")
    //            .containsOnly("ISNI");
    //    }
    //
    //    @Test
    //    void getCreators() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getCreators(doc);
    //        assertEquals(1, result.size());
    //
    //        assertThat(result)
    //            .extracting(AUTHOR_NAME)
    //            .extracting("value")
    //            .containsOnly("Bergman, W.A.");
    //    }
    //
    //    @Test
    //    void getDescription() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getDescription(doc, DESCRIPTION);
    //        assertEquals(1, result.size());
    //
    //        assertThat(result)
    //            .extracting(DESCRIPTION)
    //            .extracting("value")
    //            .containsOnly("<p>Lorem ipsum dolor sit amet,</p><p>consectetur adipiscing elit.<br>Lorem ipsum.</p>");
    //    }
    //
    //    @Test
    //    void getNonTechnicalDescription() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getNonTechnicalDescription(doc, DESCRIPTION);
    //        assertEquals(1, result.size());
    //
    //        assertThat(result)
    //            .extracting(DESCRIPTION)
    //            .extracting("value")
    //            .containsOnly("<p>Metadata description</p>");
    //    }
    //
    //    @Test
    //    void getTechnicalDescription() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getTechnicalDescription(doc, DESCRIPTION);
    //        assertEquals(1, result.size());
    //
    //        assertThat(result)
    //            .extracting(DESCRIPTION)
    //            .extracting("value")
    //            .containsOnly("<p>Technical metadata description</p>");
    //    }
    //
    //    @Test
    //    void getOtherDescriptions() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    ////        var result = mapper.getOtherDescriptions(doc, DESCRIPTION);
    ////        assertEquals(7, result.size());
    //
    ////        assertThat(result)
    ////            .extracting(DESCRIPTION)
    ////            .extracting("value")
    ////            .containsOnly(
    ////                "Date: 2015-09-07",
    ////                "Date Accepted: 2015-09-06",
    ////                "Date Copyrighted: 2015-09-05",
    ////                "Modified: 2015-09-08",
    ////                "Issued: 2015-09-04",
    ////                "Valid: yes",
    ////                "Coverage: no"
    ////            );
    //    }
    //
    //    @Test
    //    void getKeywords() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var items = mapper.getKeywords(doc);
    //
    //        assertThat(items)
    //            .extracting(KEYWORD_VALUE)
    //            .extracting("value")
    //            .containsOnly("no tags subject", "knoop", "button", "nld");
    //    }
    //
    //    @Test
    //    void testGetRelatedPublication() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getRelatedPublication(doc);
    //
    //        assertThat(result)
    //            .extracting(PUBLICATION_ID_NUMBER)
    //            .extracting("value")
    //            .containsOnly(
    //                "isbn identifier 1",
    //                "isbn identifier 2",
    //                "issn identifier 1",
    //                "issn identifier 2"
    //            );
    //
    //        assertThat(result)
    //            .extracting(PUBLICATION_ID_TYPE)
    //            .extracting("value")
    //            .containsAll(
    //                List.of("ISBN", "ISSN")
    //            );
    //    }
    //
    //    @Test
    //    void testGetCitationBlockLanguage() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getCitationBlockLanguage(doc, "");
    //
    //        assertThat(result)
    //            .containsOnly("Dutch", "German");
    //
    //    }
    //
    //    @Test
    //    void testGetProductionDate() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getProductionDate(doc);
    //
    //        assertThat(result)
    //            .containsOnly("2012-12-01");
    //
    //    }
    //
    //    @Test
    //    void testGetContributorDetails() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getContributorDetails(doc);
    //
    //        assertThat(result)
    //            .extracting(CONTRIBUTOR_NAME)
    //            .extracting("value")
    //            .containsOnly("J Doe", "X Men", "Important");
    //
    //        assertThat(result)
    //            .extracting(CONTRIBUTOR_TYPE)
    //            .extracting("value")
    //            .containsOnly("Project Manager", "Project Leader", "Other");
    //    }
    //
    //    @Test
    //    void testGetFunderDetails() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getFunderDetails(doc);
    //
    //        assertThat(result)
    //            .extracting(GRANT_NUMBER_AGENCY)
    //            .extracting("value")
    //            .containsOnly("Anti-Vampire League");
    //    }
    //
    //    @Test
    //    void testGetNwoGrantNumber() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getNwoGrantNumbers(doc);
    //
    //        assertThat(result)
    //            .extracting(GRANT_NUMBER_VALUE)
    //            .extracting("value")
    //            .containsOnly("12345", "6789");
    //    }
    //
    //    @Test
    //    void testGetPublishers() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getPublishers(doc);
    //
    //        assertThat(result)
    //            .extracting(DISTRIBUTOR_NAME)
    //            .extracting("value")
    //            .containsOnly("Synthegra");
    //    }
    //
    //    @Test
    //    void testGetDistributionDate() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getDistributionDate(doc);
    //
    //        assertThat(result)
    //            .containsOnly("2013-05-01");
    //    }
    //
    //    @Test
    //    void testGetDatesOfCollection() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getDatesOfCollection(doc);
    //
    //        assertThat(result)
    //            .extracting(DATE_OF_COLLECTION_START)
    //            .extracting("value")
    //            .containsOnly("2022-01-01", "2021-01-01", "2020-01-01", "");
    //
    //        assertThat(result)
    //            .extracting(DATE_OF_COLLECTION_END)
    //            .extracting("value")
    //            .containsOnly("2022-02-01", "2021-02-01", "", "2019-02-01");
    //    }
    //
    //    @Test
    //    void testGetDatasources() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getDataSources(doc);
    //        assertThat(result)
    //            .containsOnly("FTP", "HTTP");
    //    }
    //
    //    @Test
    //    void testGetRightsHolders() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getRightsHolders(doc);
    //        assertThat(result)
    //            .containsOnly("Mr. Rights");
    //    }
    //
    //    @Test
    //    void testGetLanguageAttributes() throws Exception {
    //        var mapper = getMapper();
    //        var doc = readDocument("dataset.xml");
    //
    //        var result = mapper.getLanguageAttributes(doc);
    //        assertThat(result)
    //            .containsOnly("en", "la", "nl", "de");
    //    }
    //
        @Test
        void testGetAcquisitionMethods()  throws Exception{
            var mapper = getMapper();
            var doc = readDocument("abrs.xml");

            var result = mapper.getAcquisitionMethods(doc);

            assertThat(result)
                .map(Node::getTextContent)
                .map(String::trim)
                .containsOnly("Method 1");
        }
}