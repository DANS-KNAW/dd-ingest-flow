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

import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundSingleValueField;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ALTERNATIVE_TITLE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATASET_CONTACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATASET_CONTACT_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATASET_CONTACT_EMAIL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATASET_CONTACT_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATA_SOURCES;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_END;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_START;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VOCABULARY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VOCABULARY_URI;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.LANGUAGE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.NOTES_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_CITATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_URL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SERIES;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SERIES_INFORMATION;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.config;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.createMapper;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getControlledMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mockedContact;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mockedVaultMetadata;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.toPrettyJsonString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CitationMetadataFromDcmiTest {

    @Test
    void CIT002_CIT010_dct_titles_before_dct_alternative_maps_first_to_alternative_rest_to_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <dct:alternative>alt title 1</dct:alternative>"
            + "        <dct:title>title 1</dct:title>"
            + "        <dct:title>title 2</dct:title>"
            + "        <dct:alternative>alt title 2</dct:alternative>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);

        // CIT002 first of dcmi title/alternative
        assertThat(getPrimitiveMultiValueField("citation", ALTERNATIVE_TITLE, result))
                .containsExactly("title 1");
        
        
        // CIT010 rest of dcmi title/alternative
        assertThat(getCompoundMultiValueField("citation", DESCRIPTION, result))
            .extracting(DESCRIPTION_VALUE).extracting("value")
            .containsExactlyInAnyOrder("<p>alt title 1</p>", "<p>title 2</p>", "<p>alt title 2</p>");
    }

    @Test
    void CIT002A_vault_metadata_other_id_is_ignored_when_not_migration() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("")
            + "</ddm:DDM>");
        var result = createMapper(false).toDataverseDataset(doc, null, null, null, mockedVaultMetadata, null, true, null, null);
        var field = getCompoundMultiValueField("citation", OTHER_ID, result);

        assertThat(field).isNull();
    }

    @Test
    void CIT00X_vault_metadata_other_id_maps_to_other_id() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("")
            + "</ddm:DDM>");
        var result = createMapper(true).toDataverseDataset(doc, "otherId:something", null, null, new VaultMetadata(), null, true, null, null);
        var field = getCompoundMultiValueField("citation", OTHER_ID, result);

        assertThat(field).hasSize(1);
        assertThat(field).extracting(OTHER_ID_AGENCY).extracting("value")
            .contains("otherId");
        assertThat(field).extracting(OTHER_ID_VALUE).extracting("value")
            .contains("something");
    }

    @Test
    void CIT002B_dct_identifier_type_easy2_maps_to_other_id() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "<dct:identifier xsi:type='id-type:EASY2'>easy-dataset:123</dct:identifier>")
            + "</ddm:DDM>");
        var result = createMapper(true).toDataverseDataset(doc, null, null, null, new VaultMetadata(), null,true, null, null);
        var field = getCompoundMultiValueField("citation", OTHER_ID, result);

        assertThat(field).hasSize(1);
        assertThat(field).extracting(OTHER_ID_AGENCY).extracting("value")
            .contains("DANS-KNAW");
        assertThat(field).extracting(OTHER_ID_VALUE).extracting("value")
            .contains("easy-dataset:123");
    }

    @Test
    void CIT002B_dct_identifier_type_easy2_is_ignored_when_not_migration() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "<dct:identifier xsi:type='id-type:EASY2'>easy-dataset:123</dct:identifier>")
            + "</ddm:DDM>");
        var result = createMapper(false).toDataverseDataset(doc, null, null, null, new VaultMetadata(), null,true, null, null);
        var field = getCompoundMultiValueField("citation", OTHER_ID, result);

        assertThat(field).isNull();
    }

    @Test
    void CIT004_typeless_dct_identifier_maps_to_other_id() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "<dct:identifier>typeless:123</dct:identifier>")
            + "</ddm:DDM>");
        var result = createMapper(true).toDataverseDataset(doc, null, null, null, new VaultMetadata(), null,true, null, null);
        var field = getCompoundMultiValueField("citation", OTHER_ID, result);

        assertThat(field).hasSize(1);
        assertThat(field).extracting(OTHER_ID_AGENCY).extracting("value")
            .contains("");
        assertThat(field).extracting(OTHER_ID_VALUE).extracting("value")
            .contains("typeless:123");
    }

    @Test
    void CIT008_contact_name_email_affiliation_of_amd_maps_to_contact() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DATASET_CONTACT, result);
        assertThat(field).extracting(DATASET_CONTACT_AFFILIATION).extracting("value")
            .contains("DANS");
        assertThat(field).extracting(DATASET_CONTACT_NAME).extracting("value")
            .contains("D. O'Seven");
        assertThat(field).extracting(DATASET_CONTACT_EMAIL).extracting("value")
            .contains("J.Bond@does.not.exist.dans.knaw.nl");
    }

    @Test
    void CIT011_dct_date_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:date>2015-09-07</dct:date>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Date: 2015-09-07");
    }

    @Test
    void CIT011_dct_dateAccepted_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:dateAccepted>2015-09-06</dct:dateAccepted>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Date Accepted: 2015-09-06");
    }

    @Test
    void CIT011_dct_dateCopyrighted_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:dateCopyrighted>2015-09-05</dct:dateCopyrighted>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Date Copyrighted: 2015-09-05");
    }

    @Test
    void CIT011_dct_dateSubmitted_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:dateSubmitted>2015-09-05</dct:dateSubmitted>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Date Submitted: 2015-09-05");
    }

    @Test
    void CIT011_dct_modified_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:modified>2015-09-08</dct:modified>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Modified: 2015-09-08");
    }

    @Test
    void CIT011_dct_issued_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:issued>2015-09-04</dct:issued>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Issued: 2015-09-04");
    }

    @Test
    void CIT011_dct_valid_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:valid>2015-09-04</dct:valid>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Valid: 2015-09-04");
    }

    @Test
    void CIT011_dct_coverage_maps_to_ds_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("<dct:coverage>2015-09-04</dct:coverage>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("Coverage: 2015-09-04");
    }

    @Test
    void CIT012_dct_description_maps_to_dsDescription() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <dct:description>blabla rabarbera</dct:description>"
            + "        <dct:description>pietje puck</dct:description>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsExactlyInAnyOrder("<p>blabla rabarbera</p>", "<p>pietje puck</p>");
    }

    @Test
    void CIT012A_dct_accessRights_maps_to_dsDescription() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + "    <ddm:profile>"
            + "        <dc:title>Title of the dataset</dc:title>"
            + "        <dc:description>Lorem ipsum.</dc:description>"
            + "        <ddm:audience>D24000</ddm:audience>"
            + "    </ddm:profile>"
            + dcmi("<dct:accessRights>Some story</dct:accessRights>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);

        assertThat(str).containsOnlyOnce("<p>Some story</p>");

        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("<p>Some story</p>", "<p>Lorem ipsum.</p>");
        assertThat(result.getDatasetVersion().getTermsOfAccess()).isEqualTo("");
    }

    @Test
    void CIT012A_dct_accessRights_is_ignored_when_not_migration() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + "    <ddm:profile>"
            + "        <dc:title>Title of the dataset</dc:title>"
            + "        <dc:description>Lorem ipsum.</dc:description>"
            + "        <ddm:audience>D24000</ddm:audience>"
            + "    </ddm:profile>"
            + dcmi("<dct:accessRights>Some story</dct:accessRights>")
            + "</ddm:DDM>");

        var result = createMapper(false).toDataverseDataset(doc, null, "2023-02-27", mockedContact, mockedVaultMetadata, null,false, null, null);
        var str = toPrettyJsonString(result);

        assertThat(str).doesNotContain("<p>Some story</p>");


        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly( "<p>Lorem ipsum.</p>");
        assertThat(result.getDatasetVersion().getTermsOfAccess()).isEqualTo("");
    }

    @Test
    void CIT014_subject_maps_to_keyword() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "  <dc:subject xml:lang='en'>metal</dc:subject>"
            + "  <dct:subject>childcare; voor- en vroegschoolse educatie; schoolloopbanen; school career; sociaal-emotionele ontwikkeling; social-emotional development; primary school; basisschool</dct:subject>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", KEYWORD, result);
        assertThat(field).extracting(KEYWORD_VALUE).extracting("value")
            .containsOnly("metal",
                "childcare; voor- en vroegschoolse educatie; schoolloopbanen; school career; sociaal-emotionele ontwikkeling; social-emotional development; primary school; basisschool");
        assertThat(result.getDatasetVersion().getTermsOfAccess()).isEqualTo("");
    }

    @Test
    void CIT015_ddm_subject_pan_maps_to_keyword() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<ddm:subject schemeURI='https://data.cultureelerfgoed.nl/term/id/pan/PAN'"
            + "                     subjectScheme='PAN thesaurus ideaaltypes'"
            + "                     valueURI='https://data.cultureelerfgoed.nl/term/id/pan/08-01-08'"
            + "                     xml:lang='en'>non-military uniform button"
            + "        </ddm:subject>"
            + "        <ddm:subject schemeURI='https://data.cultureelerfgoed.nl/term/id/pan/PAN'"
            + "                     subjectScheme='whoops'"
            + "                     valueURI='https://data.cultureelerfgoed.nl/term/id/pan/08-01-08'"
            + "                     xml:lang='en'>non-military uniform button"
            + "        </ddm:subject>"
            + "        <ddm:subject schemeURI='http://vocab.getty.edu/aat/'"
            + "                     subjectScheme='Art and Architecture Thesaurus'"
            + "                     valueURI='http://vocab.getty.edu/aat/300239261'"
            + "                     xml:lang='en'>Broader Match: buttons (fasteners)"
            + "        </ddm:subject>"
            + "        <ddm:subject schemeURI='http://vocab.getty.edu/whoops/'"
            + "                     subjectScheme='Art and Architecture Thesaurus'"
            + "                     valueURI='http://vocab.getty.edu/aat/300239261'"
            + "                     xml:lang='en'>Broader Match: buttons (fasteners)"
            + "        </ddm:subject>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", KEYWORD, result);
        assertThat(field).extracting(KEYWORD_VALUE).extracting("value")
            .containsOnly("non-military uniform button", "buttons (fasteners)");
        assertThat(field).extracting(KEYWORD_VOCABULARY).extracting("value")
            .containsOnly("PAN thesaurus ideaaltypes", "Art and Architecture Thesaurus");
        assertThat(field).extracting(KEYWORD_VOCABULARY_URI).extracting("value")
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/pan/PAN",
                "http://vocab.getty.edu/aat/");
        // note that the whoops elements are ignored
        String jsonString = toPrettyJsonString(result);
        assertThat(jsonString).containsOnlyOnce("buttons");
        assertThat(jsonString).containsOnlyOnce("uniform");
    }

    @Test
    void CIT016_dct_language_maps_to_keyword() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<dct:language>gibberish</dct:language><dct:language>koeterwaals</dct:language>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", KEYWORD, result);
        assertThat(field).extracting(KEYWORD_VALUE).extracting("value")
            .containsOnly("gibberish", "koeterwaals");
        assertThat(field).extracting(KEYWORD_VOCABULARY).extracting("value")
            .containsOnly("", "");
        assertThat(field).extracting(KEYWORD_VOCABULARY_URI).extracting("value")
            .containsOnly("", "");
    }

    @Test
    void CIT017_dc_identifier_ISBN_ISSN_maps_to_publication() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi(""
            + "        <dc:identifier xsi:type='id-type:ISSN'>0925-6229</dc:identifier>"
            + "        <dc:identifier xsi:type='ISSN'>987-654</dc:identifier>"
            + "        <dc:identifier xsi:type='id-type:ISBN'>0-345-24223-8</dc:identifier>"
            + "        <dc:identifier xsi:type='ISBN'>978-3-16-148410-0</dc:identifier>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", PUBLICATION, result);
        assertThat(field).extracting(PUBLICATION_ID_TYPE).extracting("value")
            .containsOnly("issn", "issn", "isbn", "isbn");
        assertThat(field).extracting(PUBLICATION_ID_NUMBER).extracting("value")
            .containsOnly("0925-6229", "987-654", "0-345-24223-8", "978-3-16-148410-0");
        assertThat(field).extracting(PUBLICATION_CITATION).extracting("value")
            .containsOnly("", "");
        assertThat(field).extracting(PUBLICATION_URL).extracting("value")
            .containsOnly("", "");
    }

    @Test
    void mapping_skips_hidden() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi(""
            + "        <dc:identifier xsi:type='id-type:ISSN'>0925-6229</dc:identifier>"
            + "        <dc:identifier xsi:type='ISSN'>987-654</dc:identifier>"
            + "        <dc:identifier xsi:type='id-type:ISBN'>0-345-24223-8</dc:identifier>"
            + "        <dc:identifier xsi:type='ISBN'>978-3-16-148410-0</dc:identifier>")
            + "</ddm:DDM>");

        var skipFields = List.of("dateOfDeposit", "publication");
        var activeMetadataBlocks = Set.of("citation", "dansRights", "dansDataVaultMetadata");
        var result = new DepositToDvDatasetMetadataMapper(true, activeMetadataBlocks, Map.of(), Map.of(), List.of(), config.getDataSuppliers(), skipFields, true)
            .toDataverseDataset(doc, null, "2023-02-27", mockedContact, mockedVaultMetadata,null, false, null, null);
        var field = getCompoundMultiValueField("citation", PUBLICATION, result);
        assertThat(field).isNull();
    }

    @Test
    void CIT017A_dct_provenance_maps_to_notesText() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<dct:provenance>copied xml to csv</dct:provenance>")
            + "</ddm:DDM>");

        var result = createMapper(true).toDataverseDataset(doc, null, "2023-02-27", mockedContact, mockedVaultMetadata, null,false, null, null);
        var str = toPrettyJsonString(result);

        assertThat(str).containsOnlyOnce("copied xml to csv");
        assertThat(str).doesNotContain("<p>copied xml to csv</p>");

        assertThat(getPrimitiveSingleValueField("citation", NOTES_TEXT, result))
            .isEqualTo("copied xml to csv");
    }

    @Test
    void CIT017A_dct_provenance_maps_is_is_ignored_when_not_migration() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<dct:provenance>copied xml to csv</dct:provenance>")
            + "</ddm:DDM>");

        var result = createMapper(false).toDataverseDataset(doc, null, "2023-02-27", mockedContact, mockedVaultMetadata, null,false, null, null);
        var str = toPrettyJsonString(result);

        assertThat(str).doesNotContain("copied xml to csv");
        assertThat(str).doesNotContain("<p>copied xml to csv</p>");

        assertThat(getPrimitiveSingleValueField("citation", NOTES_TEXT, result))
            .isNull();
    }

    @Test
    void CIT018_ddm_language_code_maps_to_language() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi(""
            + "  <ddm:language encodingScheme='ISO639-1' code='fy'>West-Fries</ddm:language>"
            + "  <ddm:language encodingScheme='ISO639-2' code='kal'>Groenlands</ddm:language>"
            + "  <ddm:language encodingScheme='ISO639-2' code='baq'>Baskisch</ddm:language>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);

        assertThat(getControlledMultiValueField("citation", LANGUAGE, result))
            .containsExactlyInAnyOrder("Western Frisian", "Kalaallisut, Greenlandic", "Basque");
    }

    @Test
    void CIT018_not_iso_language_is_ignored() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi(""
            + "  <ddm:language encodingScheme='rabarber' code='baq'>Baskisch</ddm:language>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var s = toPrettyJsonString(result);
        assertThat(s).doesNotContain("Baskisch");
        assertThat(s).doesNotContain("rabarber");
        assertThat(s).doesNotContain("baq");
    }

    @Test
    void CIT020_contributor_author_role_maps_to_contributor_type() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + dcmi(""
            + "    <dcx-dai:contributorDetails>"
            + "      <dcx-dai:author>"
            + "        <dcx-dai:titles>dr.</dcx-dai:titles>"
            + "        <dcx-dai:initials>M. H.</dcx-dai:initials>"
            + "        <dcx-dai:surname>van Binsbergen</dcx-dai:surname>"
            + "        <dcx-dai:role>Distributor</dcx-dai:role>"
            + "        <dcx-dai:organization>"
            + "          <dcx-dai:name>Kohnstamm Instituut</dcx-dai:name>"
            + "        </dcx-dai:organization>"
            + "       </dcx-dai:author>"
            + "     </dcx-dai:contributorDetails>"
            + "     <dcx-dai:contributorDetails>"
            + "         <dcx-dai:author>"
            + "             <dcx-dai:initials>R</dcx-dai:initials>"
            + "             <dcx-dai:surname>Smith</dcx-dai:surname>"
            + "             <dcx-dai:role>RightsHolder</dcx-dai:role>"
            + "         </dcx-dai:author>"
            + "     </dcx-dai:contributorDetails>"
            + "     <dcx-dai:contributorDetails>"
            + "         <dcx-dai:author>"
            + "             <dcx-dai:initials>A</dcx-dai:initials>"
            + "             <dcx-dai:surname>Jones</dcx-dai:surname>"
            + "             <dcx-dai:role>Funder</dcx-dai:role>"
            + "         </dcx-dai:author>"
            + "     </dcx-dai:contributorDetails>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", CONTRIBUTOR, result);
        assertThat(field).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .doesNotContain("RightsHolder");
        assertThat(field).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .containsOnly("Other", "Funder"); // see CIT024 publisher becomes distributor
        assertThat(field).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly("M. H. van Binsbergen (Kohnstamm Instituut)", "A Jones");
    }

    @Test
    void CIT021_contributor_organization_role_maps_to_contributor_type() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile() + dcmi("" 
            + "        <dcx-dai:contributorDetails>"
            + "            <dcx-dai:organization>"
            + "                <dcx-dai:name xml:lang='en'>Contributing Org</dcx-dai:name>"
            + "                <dcx-dai:role>RightsHolder</dcx-dai:role>"
            + "            </dcx-dai:organization>"
            + "        </dcx-dai:contributorDetails>"
            + "        <dcx-dai:contributorDetails>"
            + "            <dcx-dai:organization>"
            + "                <dcx-dai:name xml:lang='en'>Anti-Vampire League</dcx-dai:name>"
            + "                <dcx-dai:role>"
            + "                    Funder"
            + "                </dcx-dai:role>"
            + "                <dcx-dai:ISNI>http://isni.org/isni/0000000121032683</dcx-dai:ISNI>"
            + "            </dcx-dai:organization>"
            + "        </dcx-dai:contributorDetails>"
            + "        <dcx-dai:contributorDetails>"
            + "            <dcx-dai:organization>"
            + "                <dcx-dai:name xml:lang='en'>Important</dcx-dai:name>"
            + "                <dcx-dai:role>Important guy</dcx-dai:role>"
            + "                <dcx-dai:ISNI>http://isni.org/isni/0000000121032684</dcx-dai:ISNI>"
            + "            </dcx-dai:organization>"
            + "        </dcx-dai:contributorDetails>")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", CONTRIBUTOR, result);
        assertThat(field).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .doesNotContain("RightsHolder", "Funder"); // see CIT024 publisher becomes distributor
        assertThat(field).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .containsOnly("Other");
        assertThat(field).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly("Important"); // TODO first token/word of the role is used as default for the missing name, ISNI is not mapped
    }

    @Test
    void CIT021A_description_type_other_maps_only_to_author_name() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<ddm:description descriptionType='Other'>Author from description other</ddm:description>")
            + "</ddm:DDM>");

        var result = createMapper(true).toDataverseDataset(doc, null, "2023-02-27", mockedContact, mockedVaultMetadata, null,false, null, null);
        var field = getCompoundMultiValueField("citation", CONTRIBUTOR, result);
        var expected = "Author from description other";
        assertThat(field).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly(expected);
        // not as description and author
        assertThat(toPrettyJsonString(result)).containsOnlyOnce(expected);
    }

    @Test
    void CIT021A_description_type_other_is_ignored_when_not_migration() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<ddm:description descriptionType='Other'>Author from description other</ddm:description>")
            + "</ddm:DDM>");

        var result = createMapper(false).toDataverseDataset(doc, null, "2023-02-27", mockedContact, mockedVaultMetadata, null, false, null, null);
        var field = getCompoundMultiValueField("citation", CONTRIBUTOR, result);
        var expected = "Author from description other";
        assertThat(field).isNull();
        // not as description and author
        assertThat(toPrettyJsonString(result)).doesNotContain(expected);
    }

    @Test
    void CIT022_ddm_funder_maps_to_grant_number() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi(""
            + "<ddm:funding>"
            + "    <ddm:funderName>Funder name</ddm:funderName>"
            + "    <ddm:fundingProgramme>Funding programme</ddm:fundingProgramme>"
            + "    <ddm:awardNumber>Award number</ddm:awardNumber>"
            + "    <ddm:awardTitle xml:lang='en'>Award title</ddm:awardTitle>"
            + "</ddm:funding>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", GRANT_NUMBER, result);
        assertThat(toPrettyJsonString(result)).doesNotContain("Award title"); // ignored input
        assertThat(field).extracting(GRANT_NUMBER_AGENCY).extracting("value")
            .containsExactlyInAnyOrder("Funder name");
        assertThat(field).extracting(GRANT_NUMBER_VALUE).extracting("value")
            .containsExactlyInAnyOrder("Funding programme Award number");
    }

    @Test
    void CIT023_dct_identifier_nwo_project_nr_maps_to_grant_number() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<dct:identifier xsi:type='id-type:NWO-PROJECTNR'>380-60-007</dct:identifier>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", GRANT_NUMBER, result);
        assertThat(field).extracting(GRANT_NUMBER_AGENCY).extracting("value")
            .containsExactlyInAnyOrder("NWO");
        assertThat(field).extracting(GRANT_NUMBER_VALUE).extracting("value")
            .containsExactlyInAnyOrder("380-60-007");
    }

    @Test
    void CIT024_dct_publisher_maps_to_distributor() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile()
            + dcmi("<dct:publisher>Synthegra</dct:publisher>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DISTRIBUTOR, result);
        assertThat(field).extracting(DISTRIBUTOR_NAME).extracting("value")
            .containsExactlyInAnyOrder("Synthegra");
    }

    @Test
    void CIT026_ddm_dates_of_collection_maps_to_date_of_collection() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <ddm:datesOfCollection>2022-01-01/2022-02-01</ddm:datesOfCollection>"
            + "        <ddm:datesOfCollection>"
            + "            2021-02-01/2021-03-01"
            + "        </ddm:datesOfCollection>"
            + "        <ddm:datesOfCollection>"
            + "            2020-04-01/"
            + "        </ddm:datesOfCollection>"
            + "        <ddm:datesOfCollection>"
            + "            /2019-05-01"
            + "        </ddm:datesOfCollection>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", DATE_OF_COLLECTION, result);
        assertThat(field).extracting(DATE_OF_COLLECTION_START).extracting("value")
            .containsExactlyInAnyOrder("2022-01-01", "2021-02-01", "2020-04-01", "");
        assertThat(field).extracting(DATE_OF_COLLECTION_END).extracting("value")
            .containsExactlyInAnyOrder("2022-02-01", "2021-03-01", "2019-05-01", "");
    }

    @Test
    void CIT027_no_ddm_description_with_type_series_information_series_informations_maps_to_empty_series() throws Exception { // TODO fix
        // see also CIT027_multiple_series_informations_to_single_compound_field in MappingIntegrationMap
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        assertThat(getCompoundMultiValueField("citation", SERIES, result))
            .isNull();
    }

    @Test
    void CIT027_multiple_ddm_description_with_type_series_information_series_informations_maps_to_single_series() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">"
                + minimalDdmProfile()
                + "    <ddm:dcmiMetadata>"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>"
                + "        <ddm:description descriptionType='SeriesInformation'>series\n123</ddm:description>"
                + "        <ddm:description descriptionType='SeriesInformation'>another\nseries\n456</ddm:description>"
                + "    </ddm:dcmiMetadata>"
                + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);

        var field = (CompoundMultiValueField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(SERIES)).findFirst().orElseThrow();
        assertThat(field.getValue().get(0))
            .extracting(SERIES_INFORMATION)
            .extracting("value")
            .isEqualTo("<p>series<br>123</p>");
        assertThat(field.getValue().get(1))
            .extracting(SERIES_INFORMATION)
            .extracting("value")
            .isEqualTo("<p>another<br>series<br>456</p>");
    }

    @Test
    void CIT028_dc_source_maps_to_data_sources() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">"
                + minimalDdmProfile()
                + dcmi(""
                + "<dc:source>LISS panel, CentERdata</dc:source>"
                + " <dc:source>General population sample survey, part of the International Social Survey Programme</dc:source>"
                + "<dct:source>HTTP</dct:source>")
                + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        var field = getPrimitiveMultiValueField("citation", DATA_SOURCES, result);
        assertThat(field).containsExactlyInAnyOrder(
            "LISS panel, CentERdata",
            "General population sample survey, part of the International Social Survey Programme",
            "HTTP");
    }
}
