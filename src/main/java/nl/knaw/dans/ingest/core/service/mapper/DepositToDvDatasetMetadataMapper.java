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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import nl.knaw.dans.ingest.core.exception.MissingRequiredFieldException;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import nl.knaw.dans.ingest.core.service.mapper.builder.ArchaeologyFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.CitationFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.DataVaultFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.FieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.RelationFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.RightsFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.TemporalSpatialFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.mapping.AbrAcquisitionMethod;
import nl.knaw.dans.ingest.core.service.mapper.mapping.AbrReport;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Audience;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Author;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Base;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Contact;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Contributor;
import nl.knaw.dans.ingest.core.service.mapper.mapping.DatesOfCollection;
import nl.knaw.dans.ingest.core.service.mapper.mapping.DcxDaiAuthor;
import nl.knaw.dans.ingest.core.service.mapper.mapping.DcxDaiOrganization;
import nl.knaw.dans.ingest.core.service.mapper.mapping.DepositPropertiesOtherDoi;
import nl.knaw.dans.ingest.core.service.mapper.mapping.DepositPropertiesVaultMetadata;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Description;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Funder;
import nl.knaw.dans.ingest.core.service.mapper.mapping.HasOrganizationalIdentifier;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Identifier;
import nl.knaw.dans.ingest.core.service.mapper.mapping.InCollection;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Language;
import nl.knaw.dans.ingest.core.service.mapper.mapping.PersonalData;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Publisher;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Relation;
import nl.knaw.dans.ingest.core.service.mapper.mapping.SpatialBox;
import nl.knaw.dans.ingest.core.service.mapper.mapping.SpatialCoverage;
import nl.knaw.dans.ingest.core.service.mapper.mapping.SpatialPoint;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Subject;
import nl.knaw.dans.ingest.core.service.mapper.mapping.SubjectAbr;
import nl.knaw.dans.ingest.core.service.mapper.mapping.TemporalAbr;
import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.user.AuthenticatedUser;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RIGHTS_HOLDER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TITLE;
import static nl.knaw.dans.ingest.core.service.XPathConstants.DDM_DCMI_METADATA;
import static nl.knaw.dans.ingest.core.service.XPathConstants.DDM_PROFILE;

@Slf4j
public class DepositToDvDatasetMetadataMapper {
    private final Set<String> activeMetadataBlocks;

    private final CitationFieldBuilder citationFields = new CitationFieldBuilder();
    private final RightsFieldBuilder rightsFields = new RightsFieldBuilder();
    private final RelationFieldBuilder relationFields = new RelationFieldBuilder();
    private final ArchaeologyFieldBuilder archaeologyFields = new ArchaeologyFieldBuilder();
    private final TemporalSpatialFieldBuilder temporalSpatialFields = new TemporalSpatialFieldBuilder();
    private final DataVaultFieldBuilder dataVaultFieldBuilder = new DataVaultFieldBuilder();

    private final Map<String, String> iso1ToDataverseLanguage;
    private final Map<String, String> iso2ToDataverseLanguage;
    private final List<String> spatialCoverageCountryTerms;
    private final Map<String, String> dataSuppliers;
    private final List<String> skipFields;
    private final boolean isMigration;
    private final boolean deduplicate;

    DepositToDvDatasetMetadataMapper(boolean deduplicate, Set<String> activeMetadataBlocks, Map<String, String> iso1ToDataverseLanguage,
        Map<String, String> iso2ToDataverseLanguage, List<String> spatialCoverageCountryTerms, Map<String, String> dataSuppliers, List<String> skipFields, boolean isMigration) {
        this.deduplicate = deduplicate;
        this.activeMetadataBlocks = activeMetadataBlocks;
        this.iso1ToDataverseLanguage = iso1ToDataverseLanguage;
        this.iso2ToDataverseLanguage = iso2ToDataverseLanguage;
        this.spatialCoverageCountryTerms = spatialCoverageCountryTerms;
        this.dataSuppliers = dataSuppliers;
        this.skipFields = skipFields;
        this.isMigration = isMigration;
    }

    public Dataset toDataverseDataset(
        @NonNull Document ddm,
        @Nullable String otherDoiId,
        @Nullable String dateOfDeposit,
        @Nullable AuthenticatedUser contactData,
        @NonNull VaultMetadata vaultMetadata,
        String depositorUserId,
        boolean restrictedFilesPresent,
        String hasOrganizationalIdentifier,
        String hasOrganizationalIdentifierVersion
    ) throws MissingRequiredFieldException {
        var termsOfAccess = "";

        if (activeMetadataBlocks.contains("citation")) {
            var otherTitlesAndAlternativeTitles = getOtherTitles(ddm).collect(Collectors.toList());
            citationFields.addTitle(getTitles(ddm)); // CIT001
            citationFields.addAlternativeTitle(otherTitlesAndAlternativeTitles.stream().map(Node::getTextContent)); // CIT002

            if (isMigration) {
                citationFields.addOtherIdsStrings(Stream.ofNullable(vaultMetadata.getOtherId()) // CIT002A
                    .filter(DepositPropertiesVaultMetadata::isValidOtherIdValue), DepositPropertiesVaultMetadata.toOtherIdValue);
                citationFields.addOtherIds(getIdentifiers(ddm).filter(Identifier::hasXsiTypeEasy2), Identifier.toOtherIdValue); // CIT002B
                citationFields.addOtherIdsStrings(Stream.ofNullable(otherDoiId), DepositPropertiesOtherDoi.toOtherIdValue); // PAN second version DOIs (migration)
            }
            citationFields.addOtherIds(getIdentifiers(ddm).filter(Identifier::hasNoXsiType), Identifier.toOtherIdValue); // CIT004
            citationFields.addOtherIdsStrings(Stream.ofNullable(hasOrganizationalIdentifier).filter(HasOrganizationalIdentifier::isValidOtherIdValue),
                HasOrganizationalIdentifier.toOtherIdValue); // CIT003
            citationFields.addAuthors(getCreators(ddm), Author.toAuthorValueObject); // CIT005, CIT006, CIT007
            citationFields.addDatasetContact(Stream.ofNullable(contactData), Contact.toContactValue); // CIT008
            citationFields.addDescription(getProfileDescriptions(ddm), Description.toDescription); // CIT009

            // CIT010
            if (otherTitlesAndAlternativeTitles.size() > 1) { // First element is put in alternativeTitle field. See CIT002
                citationFields.addDescription(otherTitlesAndAlternativeTitles.stream().skip(1), Description.toDescription);
            }

            citationFields.addDescription(getOtherDescriptions(ddm).filter(Description::isNotBlank), Description.toPrefixedDescription); // CIT011
            citationFields.addDescription(getDcmiDctermsDescriptions(ddm), Description.toDescription); // CIT012
            citationFields.addDescription(getDcmiDdmDescriptions(ddm).filter(Description::isNotMapped), Description.toDescription); // CIT012

            if (restrictedFilesPresent) {
                // TRM005
                termsOfAccess = getDctAccessRights(ddm).map(Node::getTextContent).findFirst().orElse("");
            }
            else if (isMigration) {
                // CIT012A
                citationFields.addDescription(getDctAccessRights(ddm), Description.toDescription);
            }

            citationFields.addSubject(getAudiences(ddm), Audience::toCitationBlockSubject);  // CIT013
            citationFields.addKeywords(getSubjects(ddm).filter(Subject::hasNoCvAttributes), Subject.toKeywordValue); // CIT014
            citationFields.addKeywords(getDdmSubjects(ddm).filter(Subject::isPanTerm), Subject.toPanKeywordValue); // CIT015
            citationFields.addKeywords(getDdmSubjects(ddm).filter(Subject::isAatTerm), Subject.toAatKeywordValue); // CIT015
            citationFields.addKeywords(getLanguages(ddm), Language.toKeywordValue); // CIT016
            citationFields.addPublications(getIdentifiers(ddm).filter(Identifier::isRelatedPublication), Identifier.toRelatedPublicationValue); // CIT017
            citationFields.addLanguages(getDdmLanguages(ddm), node -> Language.toCitationBlockLanguage(node, iso1ToDataverseLanguage, iso2ToDataverseLanguage)); // CIT018
            citationFields.addProductionDate(getCreated(ddm).map(Base::toYearMonthDayFormat)); // CIT019
            citationFields.addGrantNumbers(getIdentifiers(ddm).filter(Identifier::isNwoGrantNumber), Identifier.toNwoGrantNumber); // CIT023
            citationFields.addContributors(getContributorDetails(ddm).filter(Contributor::isValidContributor), Contributor.toContributorValueObject); // CIT020, CIT021
            citationFields.addGrantNumbers(getFunders(ddm), Funder.toGrantNumberValueObject); // CIT022
            citationFields.addDistributor(getPublishers(ddm).filter(Publisher::isNotDans), Publisher.toDistributorValueObject); // CIT024
            citationFields.addDistributionDate(getAvailable(ddm).map(Base::toYearMonthDayFormat)); // CIT025
            if (isMigration) {
                citationFields.addNotesText(getProvenance(ddm)); // CIT017A
                citationFields.addContributors(getDcmiDdmDescriptions(ddm).filter(Description::hasDescriptionTypeOther), Contributor.toContributorValueObject); // CIT021A
                citationFields.addDateOfDeposit(dateOfDeposit); // CIT025A
            }
            citationFields.addDatesOfCollection(getDatesOfCollection(ddm)
                .filter(DatesOfCollection::isValidDatesOfCollectionPattern), DatesOfCollection.toDateOfCollectionValue); // CIT026
            citationFields.addSeries(getDcmiDdmDescriptions(ddm).filter(Description::isSeriesInformation), Description.toSeries); // CIT027
            citationFields.addDataSources(getDataSources(ddm)); // CIT028
        }
        else {
            throw new IllegalStateException("Metadatablock citation should always be active");
        }

        if (activeMetadataBlocks.contains("dansRights")) {
            if (isMigration) {
                rightsFields.addRightsHolders(getContributorDetailsAuthors(ddm).filter(DcxDaiAuthor::isRightsHolder).map(DcxDaiAuthor::toRightsHolder)); // RIG000A
                rightsFields.addRightsHolders(getContributorDetailsOrganizations(ddm).filter(DcxDaiOrganization::isRightsHolder).map(DcxDaiOrganization::toRightsHolder)); // RIG000B
            }
            rightsFields.addRightsHolders(getRightsHolders(ddm)); // RIG001
            rightsFields.addPersonalDataPresent(getPersonalData(ddm).map(PersonalData::toPersonalDataPresent)); // RIG002
            rightsFields.addLanguageOfMetadata(getLanguageAttributes(ddm)
                .map(s -> Language.isoToDataverse(s, iso1ToDataverseLanguage, iso2ToDataverseLanguage))); // RIG003
        }

        if (activeMetadataBlocks.contains("dansRelationMetadata")) {
            relationFields.addAudiences(getAudiences(ddm).map(Audience::toNarcisTerm)); // REL001
            relationFields.addCollections(getInCollections(ddm).filter(InCollection::isCollection).map(InCollection::toCollection)); // REL002
            relationFields.addRelations(getRelations(ddm)
                .filter(Relation::isRelation), Relation.toRelationObject); // REL003
        }

        if (activeMetadataBlocks.contains("dansArchaeologyMetadata")) {
            archaeologyFields.addArchisZaakId(getIdentifiers(ddm).filter(Identifier::isArchisZaakId).map(Identifier::toArchisZaakId)); // AR001
            archaeologyFields.addArchisNumber(getIdentifiers(ddm).filter(Identifier::isArchisNumber), Identifier.toArchisNumberValue); // AR002
            archaeologyFields.addRapportType(getReportNumbers(ddm).filter(AbrReport::isAbrReportType).map(AbrReport::toAbrRapportType)); // AR003
            archaeologyFields.addRapportNummer(getReportNumbers(ddm).filter(AbrReport::isAbrReportType).map(AbrReport::toAbrRapportNumber)); // AR004
            archaeologyFields.addVerwervingswijze(getAcquisitionMethods(ddm).filter(AbrAcquisitionMethod::isVerwervingswijze).map(AbrAcquisitionMethod::toVerwervingswijze)); // AR005
            archaeologyFields.addComplex(getDdmSubjects(ddm).filter(SubjectAbr::isAbrComplex).map(SubjectAbr::toAbrComplex)); // AR006
            if (isMigration)
                archaeologyFields.addArtifact(getDdmSubjects(ddm).filter(SubjectAbr::isOldAbr).map(SubjectAbr::fromAbrOldToAbrArtifact)); // TODO: REMOVE AFTER MIGRATION
            archaeologyFields.addArtifact(getDdmSubjects(ddm).filter(SubjectAbr::isAbrArtifact).map(SubjectAbr::toAbrArtifact)); // AR007
            archaeologyFields.addPeriod(getDdmTemporal(ddm).filter(TemporalAbr::isAbrPeriod).map(TemporalAbr::toAbrPeriod)); // AR008
        }

        if (activeMetadataBlocks.contains("dansTemporalSpatial")) {
            temporalSpatialFields.addTemporalCoverage(getDctermsTemporal(ddm).map(TemporalAbr::asText)); // TS001
            temporalSpatialFields.addSpatialPoint(getDcxGmlSpatial(ddm).filter(SpatialPoint::isPoint),
                SpatialPoint.toEasyTsmSpatialPointValueObject); // TS002, TS003
            temporalSpatialFields.addSpatialBox(getBoundedBy(ddm).filter(SpatialBox::isBox), SpatialBox.toEasyTsmSpatialBoxValueObject); // TS004, TS005
            temporalSpatialFields.addSpatialCoverageControlled(getSpatial(ddm)
                .map(node -> SpatialCoverage.toControlledSpatialValue(node, spatialCoverageCountryTerms))); // TS006
            temporalSpatialFields.addSpatialCoverageUncontrolled(getSpatial(ddm)
                .map((Node node) -> SpatialCoverage.toUncontrolledSpatialValue(node, spatialCoverageCountryTerms))); // TS007
        }

        if (!activeMetadataBlocks.contains("dansDataVaultMetadata")) {
            throw new IllegalStateException("dansDataVaultMetadata must always be active");
        }

        dataVaultFieldBuilder.addBagId(vaultMetadata.getBagId()); // VLT003

        if (isMigration) {
            dataVaultFieldBuilder.addNbn(vaultMetadata.getNbn()); // VLT004A
        }
        dataVaultFieldBuilder.addDansOtherId(hasOrganizationalIdentifier); // VLT005
        dataVaultFieldBuilder.addDansOtherIdVersion(hasOrganizationalIdentifierVersion); // VLT006
        dataVaultFieldBuilder.addSwordToken(vaultMetadata.getSwordToken()); // VLT007

        // VLT008
        var dataSupplier = dataSuppliers.get(depositorUserId);
        if (StringUtils.isBlank(dataSupplier)) {
            log.warn("No mapping to Data Supplier found for user id '{}'. The field dansDataSupplier will be left empty", depositorUserId);
        }
        else {
            dataVaultFieldBuilder.addDataSupplier(dataSupplier);
        }

        return assembleDataverseDataset(termsOfAccess, skipFields);
    }

    private Stream<Node> getPersonalData(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_PROFILE + "/ddm:personalData");
    }

    void processMetadataBlock(boolean deduplicate, Map<String, MetadataBlock> fields, String title, String displayName, FieldBuilder builder, List<String> skipFields) {
        var compoundFields = builder.getCompoundFields().values()
            .stream()
            .map(CompoundFieldBuilder::build);

        var primitiveFields = builder.getPrimitiveFields()
            .values()
            .stream()
            .map(b -> b.build(deduplicate));

        if (deduplicate) {
            compoundFields = compoundFields.distinct();
            primitiveFields = primitiveFields.distinct();
        }

        List<MetadataField> result = Stream.of(compoundFields, primitiveFields)
            .flatMap(i -> i)
            .filter(Objects::nonNull)
            .filter(b -> !skipFields.contains(b.getTypeName()))
            .collect(Collectors.toList());

        var block = new MetadataBlock();
        block.setDisplayName(displayName);
        block.setFields(result);

        fields.put(title, block);
    }

    private Dataset assembleDataverseDataset(String termsOfAccess, List<String> skipFields) {
        var fields = new HashMap<String, MetadataBlock>();

        processMetadataBlock(deduplicate, fields, "citation", "Citation Metadata", citationFields, skipFields);
        processMetadataBlock(deduplicate, fields, "dansRights", "Rights Metadata", rightsFields, skipFields);
        processMetadataBlock(deduplicate, fields, "dansRelationMetadata", "Relation Metadata", relationFields, skipFields);
        processMetadataBlock(deduplicate, fields, "dansArchaeologyMetadata", "Archaeology-Specific Metadata", archaeologyFields, skipFields);
        processMetadataBlock(deduplicate, fields, "dansTemporalSpatial", "Temporal and Spatial Coverage", temporalSpatialFields, skipFields);
        processMetadataBlock(deduplicate, fields, "dansDataVaultMetadata", "Dans Vault Metadata", dataVaultFieldBuilder, skipFields);

        checkRequiredField(fields, "citation", TITLE);
        checkRequiredField(fields, "citation", SUBJECT);
        checkRequiredField(fields, "dansRights", RIGHTS_HOLDER);

        var version = new DatasetVersion();
        version.setTermsOfAccess(termsOfAccess);
        version.setFileAccessRequest(true);
        version.setMetadataBlocks(fields);
        version.setFiles(new ArrayList<>());

        var dataset = new Dataset();
        dataset.setDatasetVersion(version);

        if (log.isTraceEnabled()) {
            try {
                log.trace("fields: {}", new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(fields));
            }
            catch (JsonProcessingException e) {
                log.trace("error formatting fields as json", e);
            }
        }

        return dataset;
    }

    Stream<Node> getProfileDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_PROFILE + "/dcterms:description", DDM_PROFILE + "/dc:description");
    }

    Stream<Node> getDcmiDctermsDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:description", DDM_DCMI_METADATA + "/dc:description");
    }

    Stream<Node> getDcmiDdmDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/ddm:description");
    }

    Stream<Node> getProvenance(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:provenance");
    }

    Stream<Node> getDctermsTemporal(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:temporal");
    }

    Stream<Node> getDdmTemporal(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/ddm:temporal");
    }

    Stream<Node> getSpatial(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:spatial");
    }

    Stream<Node> getDcxGmlSpatial(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcx-gml:spatial");
    }

    Stream<Node> getBoundedBy(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcx-gml:spatial//gml:boundedBy");
    }

    Stream<Node> getSubjects(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:subject", DDM_DCMI_METADATA + "/dc:subject");
    }

    Stream<Node> getDdmSubjects(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/ddm:subject");
    }

    Stream<Node> getLanguages(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:language", DDM_DCMI_METADATA + "/dc:language");
    }

    Stream<Node> getDdmLanguages(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/ddm:language");
    }

    Stream<Node> getAcquisitionMethods(Document ddm) {
        var expr = DDM_DCMI_METADATA + "/ddm:acquisitionMethod";
        return XPathEvaluator.nodes(ddm, expr);
    }

    Stream<Node> getReportNumbers(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "/ddm:reportNumber");
    }

    Stream<Node> getRelations(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "//*");
    }

    Stream<Node> getInCollections(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "/ddm:inCollection");
    }

    Stream<String> getLanguageAttributes(Document ddm) {
        return XPathEvaluator.strings(ddm, DDM_PROFILE + "//@xml:lang", DDM_DCMI_METADATA + "//@xml:lang");
    }

    Stream<Node> getContributorDetailsOrganizations(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "/dcx-dai:contributorDetails/dcx-dai:organization");
    }

    Stream<Node> getContributorDetailsAuthors(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "/dcx-dai:contributorDetails/dcx-dai:author");
    }

    Stream<Node> getContributorDetails(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcx-dai:contributorDetails[dcx-dai:author]", DDM_DCMI_METADATA + "/dcx-dai:contributorDetails[dcx-dai:organization]");
    }

    Stream<Node> getCreated(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_PROFILE + "/ddm:created");
    }

    Stream<String> getAudiences(Document ddm) {
        return XPathEvaluator.strings(ddm, DDM_PROFILE + "/ddm:audience");
    }

    Stream<Node> getIdentifiers(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:identifier", DDM_DCMI_METADATA + "/dc:identifier");
    }

    Stream<String> getTitles(Document ddm) {
        return XPathEvaluator.strings(ddm, DDM_PROFILE + "/dc:title", DDM_PROFILE + "/dcterms:title");
    }

    Stream<Node> getOtherTitles(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "/dcterms:title", DDM_DCMI_METADATA + "/dc:title", DDM_DCMI_METADATA + "/dcterms:alternative");
    }

    Stream<Node> getCreators(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_PROFILE + "/dcx-dai:creatorDetails", DDM_PROFILE + "/dcx-dai:creator", DDM_PROFILE + "/dc:creator", DDM_PROFILE + "/dcterms:creator");
    }

    Stream<Node> getOtherDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            DDM_DCMI_METADATA + "/dcterms:date",
            DDM_DCMI_METADATA + "/dc:date",
            DDM_DCMI_METADATA + "/dcterms:dateAccepted",
            DDM_DCMI_METADATA + "/dcterms:dateCopyrighted",
            DDM_DCMI_METADATA + "/dcterms:dateSubmitted",
            DDM_DCMI_METADATA + "/dcterms:modified",
            DDM_DCMI_METADATA + "/dcterms:issued",
            DDM_DCMI_METADATA + "/dcterms:valid",
            DDM_DCMI_METADATA + "/dcterms:coverage",
            DDM_DCMI_METADATA + "/dc:coverage");
    }

    Stream<Node> getDdmAccessRights(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_PROFILE + "/ddm:accessRights");
    }

    Stream<Node> getDctAccessRights(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:accessRights");
    }

    Stream<Node> getPublishers(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/dcterms:publisher", DDM_DCMI_METADATA + "/dc:publisher");
    }

    Stream<Node> getAvailable(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_PROFILE + "/ddm:available");
    }

    Stream<Node> getDatesOfCollection(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/ddm:datesOfCollection");
    }

    Stream<String> getDataSources(Document ddm) {
        return XPathEvaluator.strings(ddm, DDM_DCMI_METADATA + "/dc:source", DDM_DCMI_METADATA + "/dcterms:source");
    }

    Stream<String> getRightsHolders(Document ddm) {
        return XPathEvaluator.strings(ddm, DDM_DCMI_METADATA + "/dcterms:rightsHolder");
    }

    Stream<Node> getFunders(Document ddm) {
        return XPathEvaluator.nodes(ddm, DDM_DCMI_METADATA + "/ddm:funding");
    }

    private void checkRequiredField(HashMap<String, MetadataBlock> blocks, String blockName, String fieldName) {
        if (blocks.get(blockName).getFields().stream().map(MetadataField::getTypeName).noneMatch(fieldName::equals)) {
            throw new MissingRequiredFieldException(fieldName);
        }
    }
}
