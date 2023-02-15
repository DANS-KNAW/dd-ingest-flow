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
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import nl.knaw.dans.ingest.core.exception.MissingRequiredFieldException;
import nl.knaw.dans.ingest.core.service.mapper.builder.ArchaeologyFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.CitationFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.DataVaultFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.FieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.RelationFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.RightsFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.builder.TemporalSpatialFieldBuilder;
import nl.knaw.dans.ingest.core.service.mapper.mapping.AbrAcquisitionMethod;
import nl.knaw.dans.ingest.core.service.mapper.mapping.AbrReportType;
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
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_VERWERVINGSWIJZE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_VERWERVINGSWIJZE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TITLE;

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
    private final boolean deduplicate;

    DepositToDvDatasetMetadataMapper(boolean deduplicate, Set<String> activeMetadataBlocks, Map<String, String> iso1ToDataverseLanguage,
        Map<String, String> iso2ToDataverseLanguage) {
        this.deduplicate = deduplicate;
        this.activeMetadataBlocks = activeMetadataBlocks;
        this.iso1ToDataverseLanguage = iso1ToDataverseLanguage;
        this.iso2ToDataverseLanguage = iso2ToDataverseLanguage;
    }

    public Dataset toDataverseDataset(
        @NonNull Document ddm,
        @Nullable String otherDoiId,
        @Nullable Document agreements,
        @Nullable String dateOfDeposit,
        @Nullable AuthenticatedUser contactData,
        @Nullable VaultMetadata vaultMetadata,
        boolean hasRestrictedOrNoneFiles) throws MissingRequiredFieldException {
        var termsOfAccess = "N/a";

        if (activeMetadataBlocks.contains("citation")) {
            checkRequiredField(TITLE, getTitles(ddm));
            checkRequiredField(SUBJECT, getAudiences(ddm));

            var alternativeTitles = getAlternativeTitles(ddm).collect(Collectors.toList());
            citationFields.addTitle(getTitles(ddm)); // CIT001
            citationFields.addAlternativeTitle(alternativeTitles.stream().map(Node::getTextContent)); // CIT002

            if (vaultMetadata != null) {
                citationFields.addOtherIdsStrings(Stream.ofNullable(vaultMetadata.getOtherId()) // CIT002A
                    .filter(DepositPropertiesVaultMetadata::isValidOtherIdValue), DepositPropertiesVaultMetadata.toOtherIdValue);
            }

            citationFields.addOtherIds(getIdentifiers(ddm).filter(Identifier::canBeMappedToOtherId), Identifier.toOtherIdValue); // CIT002B, CIT004
            citationFields.addOtherIdsStrings(Stream.ofNullable(otherDoiId), DepositPropertiesOtherDoi.toOtherIdValue); // PAN second version DOIs (migration)
            // TODO: CIT003
            citationFields.addAuthors(getCreators(ddm), Author.toAuthorValueObject); // CIT005, CIT006, CIT007
            citationFields.addDatasetContact(Stream.ofNullable(contactData), Contact.toContactValue); // CIT008
            citationFields.addDescription(getProfileDescriptions(ddm), Description.toDescription); // CIT009

            // CIT010
            if (alternativeTitles.size() > 1) {
                citationFields.addDescription(alternativeTitles.stream().skip(1), Description.toDescription);
            }

            citationFields.addDescription(getOtherDescriptions(ddm).filter(Description::isNotBlank), Description.toPrefixedDescription); // CIT011
            citationFields.addDescription(getDcmiDctermsDescriptions(ddm), Description.toDescription); // CIT012
            citationFields.addDescription(getDcmiDdmDescriptions(ddm).filter(Description::isNotMapped), Description.toDescription); // CIT012


            if (hasRestrictedOrNoneFiles) {
                // TODO: FIX: access request enabled -> always fill in, otherwise: only if accessRights found.
                // TRM004, TRM005
                // stick to initial value if nothing is found
                termsOfAccess = getDctAccessRights(ddm).map(Node::getTextContent).findFirst().orElse(termsOfAccess);
            }
            else {
                // CIT012A
                citationFields.addDescription(getDctAccessRights(ddm), Description.toDescription);
            }

            citationFields.addSubject(getAudiences(ddm), Audience::toCitationBlockSubject);  // CIT013
            citationFields.addKeywords(getSubjects(ddm).filter(Subject::hasNoCvAttributes), Subject.toKeywordValue); // CIT014
            citationFields.addKeywords(getSubjects(ddm).filter(Subject::isPanTerm), Subject.toPanKeywordValue); // CIT015
            citationFields.addKeywords(getSubjects(ddm).filter(Subject::isAatTerm), Subject.toAatKeywordValue); // CIT015
            citationFields.addKeywords(getLanguages(ddm).filter(Language::isNotIsoLanguage), Language.toKeywordValue); // CIT016
            citationFields.addPublications(getIdentifiers(ddm).filter(Identifier::isRelatedPublication), Identifier.toRelatedPublicationValue); // CIT017
            citationFields.addNotesText(getProvenance(ddm)); // CIT017A
            citationFields.addLanguages(getLanguages(ddm), node -> Language.toCitationBlockLanguage(node, iso1ToDataverseLanguage, iso2ToDataverseLanguage)); // CIT018
            citationFields.addProductionDate(getCreated(ddm).map(Base::toYearMonthDayFormat)); // CIT019
            citationFields.addContributors(getContributorDetails(ddm).filter(Contributor::isValidContributor), Contributor.toContributorValueObject); // CIT020, CIT021
            citationFields.addContributors(getDcmiDdmDescriptions(ddm).filter(Description::hasDescriptionTypeOther), Contributor.toContributorValueObject); // TODO: REMOVE AFTER MIGRATION
            citationFields.addGrantNumbers(getIdentifiers(ddm).filter(Identifier::isNwoGrantNumber), Identifier.toNwoGrantNumber); // CIT023
            // TODO: CIT022 ?? (role = funder)
            citationFields.addDistributor(getPublishers(ddm).filter(Publisher::isNotDans), Publisher.toDistributorValueObject); // CIT024
            citationFields.addDistributionDate(getAvailable(ddm).map(Base::toYearMonthDayFormat)); // CIT025
            citationFields.addDateOfDeposit(dateOfDeposit); // CIT025A
            citationFields.addDatesOfCollection(getDatesOfCollection(ddm)
                .filter(DatesOfCollection::isValidDatesOfCollectionPattern), DatesOfCollection.toDateOfCollectionValue); // CIT026
            citationFields.addSeries(getDcmiDdmDescriptions(ddm).filter(Description::isSeriesInformation), Description.toSeries); // CIT027
            citationFields.addDataSources(getDataSources(ddm)); // CIT028
        }
        else {
            throw new IllegalStateException("Metadatablock citation should always be active");
        }

        if (activeMetadataBlocks.contains("dansRights")) {
            checkRequiredField(RIGHTS_HOLDER, getRightsHolders(ddm));
            rightsFields.addRightsHolders(getContributorDetailsAuthors(ddm).filter(DcxDaiAuthor::isRightsHolder).map(DcxDaiAuthor::toRightsHolder)); // RIG000A
            rightsFields.addRightsHolders(getContributorDetailsOrganizations(ddm).filter(DcxDaiOrganization::isRightsHolder).map(DcxDaiOrganization::toRightsHolder)); // RIG000B
            rightsFields.addRightsHolders(getRightsHolders(ddm)); // RIG001
            rightsFields.addPersonalDataPresent(getPersonalData(ddm).map(PersonalData::toPersonalDataPresent)); // RIG002
            rightsFields.addLanguageOfMetadata(getLanguageAttributes(ddm)
                .map(s -> Language.isoToDataverse(s, iso1ToDataverseLanguage, iso2ToDataverseLanguage))); // RIG003
        }

        if (activeMetadataBlocks.contains("dansRelationMetadata")) {
            relationFields.addAudiences(getAudiences(ddm).map(Audience::toNarcisTerm)); // REL001
            relationFields.addCollections(getInCollections(ddm).map(InCollection::toCollection)); // REL002
            relationFields.addRelations(getRelations(ddm)
                .filter(Relation::isRelation), Relation.toRelationObject); // REL003
        }

        if (activeMetadataBlocks.contains("dansArchaeologyMetadata")) {
            archaeologyFields.addArchisZaakId(getIdentifiers(ddm).filter(Identifier::isArchisZaakId).map(Identifier::toArchisZaakId)); // AR001
            archaeologyFields.addArchisNumber(getIdentifiers(ddm).filter(Identifier::isArchisNumber), Identifier.toArchisNumberValue); // AR002
            archaeologyFields.addRapportType(getReportNumbers(ddm).filter(AbrReportType::isAbrReportType).map(AbrReportType::toAbrRapportType)); // AR003
            archaeologyFields.addRapportNummer(getReportNumbers(ddm).map(Base::asText)); // AR004
            archaeologyFields.addVerwervingswijze(getAcquisitionMethods(ddm).map(AbrAcquisitionMethod::toVerwervingswijze)); // AR005
            archaeologyFields.addComplex(getSubjects(ddm).filter(SubjectAbr::isAbrComplex).map(SubjectAbr::toAbrComplex)); // AR006
            archaeologyFields.addArtifact(getSubjects(ddm).filter(SubjectAbr::isOldAbr).map(SubjectAbr::fromAbrOldToAbrArtifact)); // TODO: REMOVE AFTER MIGRATION
            archaeologyFields.addArtifact(getSubjects(ddm).filter(SubjectAbr::isAbrArtifact).map(SubjectAbr::toAbrArtifact)); // AR007
            archaeologyFields.addPeriod(getSubjects(ddm).filter(TemporalAbr::isAbrPeriod).map(TemporalAbr::toAbrPeriod)); // TODO: FIX: USE ddm:temporal AR008
        }

        if (activeMetadataBlocks.contains("dansTemporalSpatial")) {
            temporalSpatialFields.addTemporalCoverage(getDctermsTemporal(ddm).map(TemporalAbr::asText)); // TS001
            temporalSpatialFields.addSpatialPoint(getDcxGmlSpatial(ddm)
                .filter(node -> SpatialPoint.hasChildNode(node, "/Point")), SpatialPoint.toEasyTsmSpatialPointValueObject); // TS002, TS003
            temporalSpatialFields.addSpatialBox(getBoundedBy(ddm), SpatialBox.toEasyTsmSpatialBoxValueObject); // TS004, TS005
            temporalSpatialFields.addSpatialCoverageControlled(getSpatial(ddm).map(SpatialCoverage::toControlledSpatialValue)); // TS006
            temporalSpatialFields.addSpatialCoverageUncontrolled(getSpatial(ddm).map(SpatialCoverage::toUncontrolledSpatialValue)); // TS007
        }

        if (activeMetadataBlocks.contains("dansDataVaultMetadata") && vaultMetadata != null) {
            dataVaultFieldBuilder.addBagId(vaultMetadata.getBagId());
            dataVaultFieldBuilder.addNbn(vaultMetadata.getNbn());
            dataVaultFieldBuilder.addDansOtherId(vaultMetadata.getOtherId());
            dataVaultFieldBuilder.addDansOtherIdVersion(vaultMetadata.getOtherIdVersion());
            dataVaultFieldBuilder.addSwordToken(vaultMetadata.getSwordToken());

        }

        return assembleDataverseDataset(termsOfAccess);
    }

    private Stream<Node> getPersonalData(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/ddm:personalData");
    }

    private Stream<Node> getPersonalDataPresent(Document agreements) {
        if (agreements == null) {
            return Stream.empty();
        }

        return XPathEvaluator.nodes(agreements, "//agreements:personalDataStatement");
    }

    void processMetadataBlock(boolean deduplicate, Map<String, MetadataBlock> fields, String title, String displayName, FieldBuilder builder) {
        // TODO figure out how to deduplicate compound fields (just on key, or also on value?)
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
            .collect(Collectors.toList());

        var block = new MetadataBlock();
        block.setDisplayName(displayName);
        block.setFields(result);

        fields.put(title, block);
    }

    Dataset assembleDataverseDataset(String termsOfAccess) {
        var fields = new HashMap<String, MetadataBlock>();

        processMetadataBlock(deduplicate, fields, "citation", "Citation Metadata", citationFields);
        processMetadataBlock(deduplicate, fields, "dansRights", "Rights Metadata", rightsFields);
        processMetadataBlock(deduplicate, fields, "dansRelationMetadata", "Relation Metadata", relationFields);
        processMetadataBlock(deduplicate, fields, "dansArchaeologyMetadata", "Archaeology-Specific Metadata", archaeologyFields);
        processMetadataBlock(deduplicate, fields, "dansTemporalSpatial", "Temporal and Spatial Coverage", temporalSpatialFields);
        processMetadataBlock(deduplicate, fields, "dansDataVaultMetadata", "Dans Vault Metadata", dataVaultFieldBuilder);

        var version = new DatasetVersion();
        version.setTermsOfAccess(termsOfAccess);
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
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/dcterms:description | /ddm:DDM/ddm:profile/dc:description");
    }

    Stream<Node> getDcmiDctermsDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:description");
    }

    Stream<Node> getDcmiDdmDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/ddm:description");
    }

    Stream<Node> getProvenance(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:provenance");
    }

    Stream<Node> getDctermsTemporal(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:temporal");
    }

    Stream<Node> getSpatial(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:spatial");
    }

    Stream<Node> getDcxGmlSpatial(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/gml:spatial");
    }

    Stream<Node> getBoundedBy(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/gml:spatial//gml:boundedBy");
    }

    Stream<Node> getSubjects(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:subject");
    }

    Stream<Node> getLanguages(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/ddm:language");
    }

    Stream<Node> getAcquisitionMethods(Document ddm) {
        var expr = String.format(
            "/ddm:DDM/ddm:dcmiMetadata/ddm:acquisitionMethod[@subjectScheme = '%s' and @schemeURI = '%s']",
            SCHEME_ABR_VERWERVINGSWIJZE, SCHEME_URI_ABR_VERWERVINGSWIJZE
        );

        return XPathEvaluator.nodes(ddm, expr);
    }

    Stream<Node> getReportNumbers(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata/ddm:reportNumber");
    }

    Stream<Node> getRelations(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata//*");
    }

    Stream<Node> getInCollections(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata/ddm:inCollection");
    }

    Stream<String> getLanguageAttributes(Document ddm) {
        return XPathEvaluator.strings(ddm, "/ddm:DDM/ddm:profile//@xml:lang | /ddm:DDM/ddm:dcmiMetadata//@xml:lang");
    }

    Stream<Node> getContributorDetailsOrganizations(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:organization");
    }

    Stream<Node> getContributorDetailsAuthors(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:author");
    }

    Stream<Node> getContributorDetails(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcx-dai:contributorDetails[dcx-dai:author] | /ddm:DDM/ddm:dcmiMetadata/dcx-dai:contributorDetails[dcx-dai:organization]");
    }

    Stream<Node> getCreated(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/ddm:created");
    }

    Stream<String> getAudiences(Document ddm) {
        return XPathEvaluator.strings(ddm, "/ddm:DDM/ddm:profile/ddm:audience");
    }

    Stream<Node> getIdentifiers(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:identifier");
    }

    Stream<String> getTitles(Document ddm) {
        return XPathEvaluator.strings(ddm, "/ddm:DDM/ddm:profile/dc:title");
    }

    Stream<Node> getAlternativeTitles(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:title", "/ddm:DDM/ddm:dcmiMetadata/dcterms:alternative");
    }

    Stream<Node> getCreators(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:profile/dcx-dai:creatorDetails | /ddm:DDM/ddm:profile/dcx-dai:creator | /ddm:DDM/ddm:profile/dc:creator");
    }

    Stream<Node> getOtherDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:date",
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:dateAccepted",
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:dateCopyrighted",
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:modified",
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:issued",
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:valid",
            "/ddm:DDM/ddm:dcmiMetadata/dcterms:coverage");
    }

    Stream<Node> getDctAccessRights(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:accessRights");
    }

    Stream<Node> getPublishers(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:publisher");
    }

    Stream<Node> getAvailable(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/ddm:available");
    }

    Stream<Node> getDatesOfCollection(Document ddm) {
        return XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:dcmiMetadata/ddm:datesOfCollection");
    }

    Stream<String> getDataSources(Document ddm) {
        return XPathEvaluator.strings(ddm, "/ddm:DDM/ddm:dcmiMetadata/dc:source");
    }

    Stream<String> getRightsHolders(Document ddm) {
        return XPathEvaluator.strings(ddm, "/ddm:DDM/ddm:dcmiMetadata/dcterms:rightsHolder");
    }

    void checkRequiredField(String fieldName, Stream<String> nodes) {
        var result = nodes
            .map(String::trim)
            .filter(StringUtils::isNotBlank)
            .findFirst();

        if (result.isEmpty()) {
            throw new MissingRequiredFieldException(fieldName);
        }
    }
}
