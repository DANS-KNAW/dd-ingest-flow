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
package nl.knaw.dans.ingest.core.service.mapper;

import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_URI;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static org.assertj.core.api.Assertions.assertThat;

public class DansRelationMetadataTest {

    @Test
    void REL001_audience() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + "  <ddm:profile>"
            + "      <dc:title>Title of the dataset</dc:title>"
            + "      <ddm:audience>D24000</ddm:audience>"
            + "  </ddm:profile>"
            + dcmi("")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        // more values in CIT013 in CitationMetadataFromProfileTest
        assertThat(getPrimitiveMultiValueField("dansRelationMetadata", "dansAudience", result))
            .containsExactlyInAnyOrder("https://www.narcis.nl/classification/D24000");
    }

    @Test
    void REL002_collection() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "  <ddm:inCollection "
            + "      subjectScheme='DANS Collection'"
            + "      valueURI='https://vocabularies.dans.knaw.nl/collections/ssh/eeea099b-8c82-4f16-9c50-e67f3a9f24c2'"
            + "      schemeURI='https://vocabularies.dans.knaw.nl/collections'"
            + "  >COOL</ddm:inCollection>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        assertThat(getPrimitiveMultiValueField("dansRelationMetadata", "dansCollection", result))
            .containsExactlyInAnyOrder("https://vocabularies.dans.knaw.nl/collections/ssh/eeea099b-8c82-4f16-9c50-e67f3a9f24c2");
    }

    @Test
    void REL003_text_only() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<ddm:hasFormat>barbapapa</ddm:hasFormat>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("dansRelationMetadata", "dansRelation", result);
        assertThat(field).extracting(RELATION_TYPE).extracting("value").containsOnly("has format");
        assertThat(field).extracting(RELATION_URI).extracting("value").containsOnly("");
        assertThat(field).extracting(RELATION_TEXT).extracting("value").containsOnly("barbapapa");
    }

    @Test
    void REL003_with_href() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<ddm:relation href='https://example.com/relation'>rabarbara</ddm:relation>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("dansRelationMetadata", "dansRelation", result);
        assertThat(field).extracting(RELATION_TYPE).extracting("value").containsOnly("relation");
        assertThat(field).extracting(RELATION_URI).extracting("value").containsOnly("https://example.com/relation");
        assertThat(field).extracting(RELATION_TEXT).extracting("value").containsOnly("rabarbara");
    }

    @Test
    void REL003_with_scheme() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<ddm:relation scheme='DOI'>http://doi.org/10.1111/sode.12120</ddm:relation>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("dansRelationMetadata", "dansRelation", result);
        assertThat(field).extracting(RELATION_TYPE).extracting("value").containsOnly("relation");
        assertThat(field).extracting(RELATION_URI).extracting("value").containsOnly("");
        assertThat(field).extracting(RELATION_TEXT).extracting("value").containsOnly("http://doi.org/10.1111/sode.12120");
    }

    @Test
    void REL003() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "  <ddm:relation>x</ddm:relation>"
            + "  <ddm:conformsTo>x</ddm:conformsTo>"
            + "  <ddm:hasFormat>x</ddm:hasFormat>"
            + "  <ddm:hasPart>x</ddm:hasPart>"
            + "  <ddm:references>x</ddm:references>"
            + "  <ddm:replaces>x</ddm:replaces>"
            + "  <ddm:requires>x</ddm:requires>"
            + "  <ddm:hasVersion>x</ddm:hasVersion>"
            + "  <ddm:isFormatOf>x</ddm:isFormatOf>"
            + "  <ddm:isPartOf>x</ddm:isPartOf>"
            + "  <ddm:isReferencedBy>x</ddm:isReferencedBy>"
            + "  <ddm:isRequiredBy>x</ddm:isRequiredBy>"
            + "  <ddm:isVersionOf>x</ddm:isVersionOf>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        assertThat(getCompoundMultiValueField("dansRelationMetadata", "dansRelation", result))
            .extracting(RELATION_TYPE).extracting("value")
            .containsExactlyInAnyOrder("relation",
                "conforms to",
                "has format",
                "has part",
                "references",
                "replaces",
                "requires",
                "has version",
                "is format of",
                "is part of",
                "is referenced by",
                "is required by",
                "is version of");
    }
}
