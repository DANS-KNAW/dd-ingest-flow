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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.builder.ArchaeologyFieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.CitationFieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.DataVaultFieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.FieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.PrimitiveFieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.RelationFieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.RightsFieldBuilder;
import nl.knaw.dans.ingest.core.service.builder.TemporalSpatialFieldBuilder;
import nl.knaw.dans.ingest.core.service.exception.MissingRequiredFieldException;
import nl.knaw.dans.ingest.core.service.mapping.AbrAcquisitionMethod;
import nl.knaw.dans.ingest.core.service.mapping.AbrReportType;
import nl.knaw.dans.ingest.core.service.mapping.Audience;
import nl.knaw.dans.ingest.core.service.mapping.Author;
import nl.knaw.dans.ingest.core.service.mapping.Base;
import nl.knaw.dans.ingest.core.service.mapping.Contact;
import nl.knaw.dans.ingest.core.service.mapping.Contributor;
import nl.knaw.dans.ingest.core.service.mapping.DatesOfCollection;
import nl.knaw.dans.ingest.core.service.mapping.DcxDaiAuthor;
import nl.knaw.dans.ingest.core.service.mapping.DcxDaiOrganization;
import nl.knaw.dans.ingest.core.service.mapping.DepositPropertiesOtherDoi;
import nl.knaw.dans.ingest.core.service.mapping.DepositPropertiesVaultMetadata;
import nl.knaw.dans.ingest.core.service.mapping.Description;
import nl.knaw.dans.ingest.core.service.mapping.Identifier;
import nl.knaw.dans.ingest.core.service.mapping.InCollection;
import nl.knaw.dans.ingest.core.service.mapping.Language;
import nl.knaw.dans.ingest.core.service.mapping.PersonalStatement;
import nl.knaw.dans.ingest.core.service.mapping.Publisher;
import nl.knaw.dans.ingest.core.service.mapping.Relation;
import nl.knaw.dans.ingest.core.service.mapping.SpatialBox;
import nl.knaw.dans.ingest.core.service.mapping.SpatialCoverage;
import nl.knaw.dans.ingest.core.service.mapping.SpatialPoint;
import nl.knaw.dans.ingest.core.service.mapping.Subject;
import nl.knaw.dans.ingest.core.service.mapping.SubjectAbr;
import nl.knaw.dans.ingest.core.service.mapping.TemporalAbr;
import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.user.AuthenticatedUser;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_VERWERVINGSWIJZE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_VERWERVINGSWIJZE;

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

    public DepositToDvDatasetMetadataMapper(boolean deduplicate, Set<String> activeMetadataBlocks, Map<String, String> iso1ToDataverseLanguage,
        Map<String, String> iso2ToDataverseLanguage) {
        this.deduplicate = deduplicate;
        this.activeMetadataBlocks = activeMetadataBlocks;
        this.iso1ToDataverseLanguage = iso1ToDataverseLanguage;
        this.iso2ToDataverseLanguage = iso2ToDataverseLanguage;
    }

    // TODO check nullable state
    public Dataset toDataverseDataset(
        @NonNull Document ddm,
        @Nullable String otherDoiId,
        @Nullable Document agreements,
        @Nullable String dateOfDeposit,
        @Nullable AuthenticatedUser contactData,
        @Nullable VaultMetadata vaultMetadata
    ) throws MissingRequiredFieldException {

        // TODO otherDoiId should be "", not null when sending to dataverse

        // TODO check for required fields (title?)
        if (activeMetadataBlocks.contains("citation")) {
            var alternativeTitles = getAlternativeTitles(ddm).collect(Collectors.toList());
            citationFields.addTitle(getTitles(ddm));
            citationFields.addAlternativeTitle(alternativeTitles.stream().map(Node::getTextContent));

            if (vaultMetadata != null) {
                citationFields.addOtherIdsStrings(Stream.ofNullable(vaultMetadata.getOtherId())
                    .filter(DepositPropertiesVaultMetadata::isValidOtherIdValue), DepositPropertiesVaultMetadata.toOtherIdValue);
            }

            citationFields.addOtherIds(getIdentifiers(ddm).filter(Identifier::canBeMappedToOtherId), Identifier.toOtherIdValue);
            citationFields.addOtherIdsStrings(Stream.ofNullable(otherDoiId), DepositPropertiesOtherDoi.toOtherIdValue);

            citationFields.addAuthors(getCreators(ddm), Author.toAuthorValueObject);
            citationFields.addDatasetContact(Stream.ofNullable(contactData), Contact.toOtherIdValue);
            citationFields.addDescription(getDescriptions(ddm), Description.toDescription);

            if (alternativeTitles.size() > 0) {
                citationFields.addDescription(Stream.of(alternativeTitles.get(alternativeTitles.size() - 1)), Description.toDescription);
            }

            citationFields.addDescription(getMetadataDescriptions(ddm)
                .filter(Description::isNonTechnicalInfo), Description.toDescription);

            citationFields.addDescription(getOtherDescriptions(ddm), Description.toPrefixedDescription);
            citationFields.addDescription(getMetadataDescriptions(ddm)
                .filter(Description::isTechnicalInfo), Description.toDescription);

            citationFields.addSubject(getAudiences(ddm), Audience::toCitationBlockSubject);

            citationFields.addKeywords(getSubjects(ddm).filter(Subject::hasNoCvAttributes), Subject.toKeywordValue);
            citationFields.addKeywords(getSubjects(ddm).filter(Subject::isPanTerm), Subject.toPanKeywordValue);
            citationFields.addKeywords(getSubjects(ddm).filter(Subject::isAatTerm), Subject.toAatKeywordValue);
            citationFields.addKeywords(getLanguages(ddm).filter(Language::isNotIsoLanguage), Language.toKeywordValue);
            citationFields.addPublications(getIdentifiers(ddm).filter(Identifier::isRelatedPublication), Identifier.toRelatedPublicationValue);
            citationFields.addLanguages(getLanguages(ddm), node -> Language.isoToDataverse(node.getTextContent(), iso1ToDataverseLanguage, iso2ToDataverseLanguage));
            citationFields.addProductionDate(getCreated(ddm).map(Base::toYearMonthDayFormat));
            citationFields.addContributors(getContributorDetails(ddm).filter(Contributor::isValidContributor), Contributor.toContributorValueObject);
            citationFields.addGrantNumbers(getIdentifiers(ddm).filter(Identifier::isNwoGrantNumber), Identifier.toNwoGrantNumber);

            citationFields.addDistributor(getPublishers(ddm).filter(Publisher::isNotDans), Publisher.toDistributorValueObject);
            citationFields.addDistributionDate(getAvailable(ddm).map(Base::toYearMonthDayFormat));

            citationFields.addDateOfDeposit(dateOfDeposit);

            citationFields.addDateOfCollections(getDatesOfCollection(ddm)
                .filter(DatesOfCollection::isValidDistributorDate), DatesOfCollection.toDistributorValueObject);
            citationFields.addDataSources(getDataSources(ddm));
        }
        else {
            throw new IllegalStateException("Metadatablock citation should always be active");
        }

        if (activeMetadataBlocks.contains("dansRights")) {
            rightsFields.addRightsHolders(getRightsHolders(ddm));

            rightsFields.addPersonalDataPresent(getPersonalDataPresent(agreements).map(PersonalStatement::toHasPersonalDataValue));
            rightsFields.addRightsHolders(getContributorDetailsAuthors(ddm).filter(DcxDaiAuthor::isRightsHolder).map(DcxDaiAuthor::toRightsHolder));
            rightsFields.addRightsHolders(getContributorDetailsOrganizations(ddm).filter(DcxDaiOrganization::isRightsHolder).map(DcxDaiOrganization::toRightsHolder));
            rightsFields.addLanguageOfMetadata(getLanguageAttributes(ddm)
                .map(n -> Language.isoToDataverse(n, iso1ToDataverseLanguage, iso2ToDataverseLanguage)));
        }

        if (activeMetadataBlocks.contains("dansRelationMetadata")) {
            relationFields.addAudiences(getAudiences(ddm).map(Audience::toNarcisTerm));
            relationFields.addCollections(getInCollections(ddm).map(InCollection::toCollection));
            relationFields.addRelations(getRelations(ddm)
                .filter(Relation::isRelation), Relation.toRelationObject);
        }

        if (activeMetadataBlocks.contains("dansArchaeologyMetadata")) {
            archaeologyFields.addArchisZaakId(getIdentifiers(ddm).filter(Identifier::isArchisZaakId).map(Identifier::toArchisZaakId));
            archaeologyFields.addArchisNumber(getIdentifiers(ddm).filter(Identifier::isArchisNumber), Identifier.toArchisNumberValue);
            archaeologyFields.addRapportType(getReportNumbers(ddm).filter(AbrReportType::isAbrReportType).map(AbrReportType::toAbrRapportType));
            archaeologyFields.addRapportNummer(getReportNumbers(ddm).map(Base::asText));
            archaeologyFields.addVerwervingswijze(getAcquisitionMethods(ddm).map(AbrAcquisitionMethod::toVerwervingswijze));
            archaeologyFields.addComplex(getSubjects(ddm).filter(SubjectAbr::isAbrComplex).map(SubjectAbr::toAbrComplex));
            archaeologyFields.addArtifact(getSubjects(ddm).filter(SubjectAbr::isOldAbr).map(SubjectAbr::fromAbrOldToAbrArtifact));
            archaeologyFields.addArtifact(getSubjects(ddm).filter(SubjectAbr::isAbrArtifact).map(SubjectAbr::toAbrArtifact));
            archaeologyFields.addPeriod(getSubjects(ddm).filter(TemporalAbr::isAbrPeriod).map(TemporalAbr::toAbrPeriod));
        }

        if (activeMetadataBlocks.contains("dansTemporalSpatial")) {
            temporalSpatialFields.addTemporalCoverage(getTemporal(ddm).filter(TemporalAbr::isNotAbrPeriod).map(TemporalAbr::asText));
            temporalSpatialFields.addSpatialPoint(getSpatial(ddm)
                .filter(node -> SpatialPoint.hasChildNode(node, "/Point")), SpatialPoint.toEasyTsmSpatialPointValueObject);

            temporalSpatialFields.addSpatialBox(getBoundedBy(ddm), SpatialBox.toEasyTsmSpatialBoxValueObject);
            temporalSpatialFields.addSpatialCoverageControlled(getSpatial(ddm)
                .filter(SpatialCoverage::hasNoChildElement).map(SpatialCoverage::toControlledSpatialValue));

            temporalSpatialFields.addSpatialCoverageUncontrolled(getSpatial(ddm)
                .filter(SpatialCoverage::hasNoChildElement).map(SpatialCoverage::toUncontrolledSpatialValue));
        }

        if (activeMetadataBlocks.contains("dansDataVaultMetadata") && vaultMetadata != null) {
            dataVaultFieldBuilder.addBagId(vaultMetadata.getBagId());
            dataVaultFieldBuilder.addNbn(vaultMetadata.getNbn());
            dataVaultFieldBuilder.addDansOtherId(vaultMetadata.getOtherId());
            dataVaultFieldBuilder.addDansOtherIdVersion(vaultMetadata.getOtherIdVersion());
            dataVaultFieldBuilder.addSwordToken(vaultMetadata.getSwordToken());

        }

        return assembleDataverseDataset();
    }

    private Stream<Node> getPersonalDataPresent(Document agreements) {
        if (agreements == null) {
            return Stream.empty();
        }

        return XPathEvaluator.nodes(agreements, "//agreements:personalDataStatement");
    }

    void processMetadataBlock(Map<String, MetadataBlock> fields, String title, String displayName, FieldBuilder builder) {
        var compoundFields = builder.getCompoundFields().values()
            .stream()
            .map(CompoundFieldBuilder::build)
            .map(m -> (MetadataField) m);

        var primitiveFields = builder.getPrimitiveFields()
            .values()
            .stream()
            .map(PrimitiveFieldBuilder::build);

        var result = Stream.of(compoundFields, primitiveFields)
            .flatMap(i -> i)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        var block = new MetadataBlock();
        block.setDisplayName(displayName);
        block.setFields(result);

        fields.put(title, block);
    }

    Dataset assembleDataverseDataset() {
        var fields = new HashMap<String, MetadataBlock>();

        processMetadataBlock(fields, "citation", "Citation Metadata", citationFields);
        processMetadataBlock(fields, "dansRights", "Rights Metadata", rightsFields);
        processMetadataBlock(fields, "dansRelationMetadata", "Relation Metadata", relationFields);
        processMetadataBlock(fields, "dansArchaeologyMetadata", "Archaeology-Specific Metadata", archaeologyFields);
        processMetadataBlock(fields, "dansTemporalSpatial", "Temporal and Spatial Coverage", temporalSpatialFields);
        processMetadataBlock(fields, "dansDataVaultMetadata", "Dans Vault Metadata", dataVaultFieldBuilder);

        var version = new DatasetVersion();
        version.setMetadataBlocks(fields);
        version.setFiles(new ArrayList<>());

        var dataset = new Dataset();
        dataset.setDatasetVersion(version);

        return dataset;
    }

    Stream<Node> getDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:profile/dcterms:description");
    }

    Stream<Node> getMetadataDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:description");
    }

    Stream<Node> getTemporal(Document ddm) {
        // TODO verify namespace
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/ddm:temporal");
    }

    Stream<Node> getSpatial(Document ddm) {
        // TODO verify namespace
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:spatial");
    }

    Stream<Node> getBoundedBy(Document ddm) {
        // TODO verify namespace
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:spatial//gml:boundedBy");
    }

    Stream<Node> getSubjects(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:subject");
    }

    Stream<Node> getLanguages(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:language");
    }

    Stream<Node> getAcquisitionMethods(Document ddm) {
        // TODO verify namespace of second element
        // TODO: (from scala) also take attribute namespace into account (should be ddm)
        var expr = String.format(
            "//ddm:dcmiMetadata/ddm:acquisitionMethod[@subjectScheme = '%s' and @schemeURI = '%s']",
            SCHEME_ABR_VERWERVINGSWIJZE, SCHEME_URI_ABR_VERWERVINGSWIJZE
        );

        return XPathEvaluator.nodes(ddm, expr);
    }

    Stream<Node> getReportNumbers(Document ddm) {
        // TODO verify namespace
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata/ddm:reportNumber");
    }

    Stream<Node> getRelations(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata//*");
    }

    Stream<Node> getInCollections(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata/ddm:inCollection");
    }

    Stream<String> getLanguageAttributes(Document ddm) {
        return XPathEvaluator.strings(ddm, "//ddm:profile//@xml:lang | //ddm:dcmiMetadata//@xml:lang");
    }

    Stream<Node> getContributorDetailsOrganizations(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:organization");
    }

    Stream<Node> getContributorDetailsAuthors(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:author");
    }

    Stream<Node> getContributorDetails(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcx-dai:contributorDetails[dcx-dai:author] | //ddm:dcmiMetadata/dcx-dai:contributorDetails[dcx-dai:organization]");
    }

    Stream<Node> getCreated(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:profile/ddm:created");
    }

    Stream<String> getAudiences(Document ddm) {
        return XPathEvaluator.strings(ddm, "//ddm:profile/ddm:audience");
    }

    Stream<Node> getIdentifiers(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:identifier");
    }

    Stream<String> getTitles(Document ddm) {
        return XPathEvaluator.strings(ddm, "//ddm:profile/dc:title");
    }

    Stream<Node> getAlternativeTitles(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata/dcterms:title", "//ddm:dcmiMetadata/dcterms:alternative");
    }

    Stream<Node> getCreators(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:profile/dcx-dai:creatorDetails | //ddm:profile/dcx-dai:creator");
    }

    Stream<Node> getOtherDescriptions(Document ddm) {
        return XPathEvaluator.nodes(ddm,
            "//ddm:dcmiMetadata/dcterms:date",
            "//ddm:dcmiMetadata/dcterms:dateAccepted",
            "//ddm:dcmiMetadata/dcterms:dateCopyrighted",
            "//ddm:dcmiMetadata/dcterms:modified",
            "//ddm:dcmiMetadata/dcterms:issued",
            "//ddm:dcmiMetadata/dcterms:valid",
            "//ddm:dcmiMetadata/dcterms:coverage");
    }

    Stream<Node> getPublishers(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:publisher");
    }

    Stream<Node> getAvailable(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:profile/ddm:available");
    }

    Stream<Node> getDatesOfCollection(Document ddm) {
        return XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/ddm:datesOfCollection");
    }

    Stream<String> getDataSources(Document ddm) {
        // TODO verify the correct namespace, there are no examples?
        return XPathEvaluator.strings(ddm, "//ddm:dcmiMetadata/dcterms:source");
    }

    Stream<String> getRightsHolders(Document ddm) {
        return XPathEvaluator.strings(ddm, "//ddm:dcmiMetadata/dcterms:rightsHolder");
    }

}