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
import nl.knaw.dans.easy.dd2d.fieldbuilders.AbstractFieldBuilder;
import nl.knaw.dans.easy.dd2d.fieldbuilders.CompoundFieldBuilder;
import nl.knaw.dans.easy.dd2d.fieldbuilders.PrimitiveFieldBuilder;
import nl.knaw.dans.ingest.core.DatasetAuthor;
import nl.knaw.dans.ingest.core.DatasetOrganization;
import nl.knaw.dans.ingest.core.service.mapping.AbrReportType;
import nl.knaw.dans.ingest.core.service.mapping.Audience;
import nl.knaw.dans.ingest.core.service.mapping.DcxDaiAuthor;
import nl.knaw.dans.ingest.core.service.mapping.DcxDaiOrganization;
import nl.knaw.dans.ingest.core.service.mapping.Identifier;
import nl.knaw.dans.ingest.core.service.mapping.InCollection;
import nl.knaw.dans.ingest.core.service.mapping.Language;
import nl.knaw.dans.ingest.core.service.mapping.Relation;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathExpressionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_RAPPORT_NUMMER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_RAPPORT_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_VERWERVINGSWIJZE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ALTERNATIVE_TITLE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_ZAAK_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUDIENCE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.COLLECTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATA_SOURCES;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_END;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_START;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTION_DATE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_ABBREVIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_LOGO_URL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTOR_URL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VOCABULARY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.KEYWORD_VOCABULARY_URI;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.LANGUAGE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.LANGUAGE_OF_METADATA;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PRODUCTION_DATE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_CITATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_URL;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RIGHTS_HOLDER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TITLE;
import static nl.knaw.dans.ingest.core.service.XmlReader.NAMESPACE_XSI;

@Slf4j
public class DepositToDvDatasetMetadataMapper {

    static Map<String, String> contributoreRoleToContributorType = new HashMap<>();

    static {
        contributoreRoleToContributorType.put("DataCurator", "Data Curator");
        contributoreRoleToContributorType.put("DataManager", "Data Manager");
        contributoreRoleToContributorType.put("Editor", "Editor");
        contributoreRoleToContributorType.put("Funder", "Funder");
        contributoreRoleToContributorType.put("HostingInstitution", "Hosting Institution");
        contributoreRoleToContributorType.put("ProjectLeader", "Project Leader");
        contributoreRoleToContributorType.put("ProjectManager", "Project Manager");
        contributoreRoleToContributorType.put("Related Person", "Related Person");
        contributoreRoleToContributorType.put("Researcher", "Researcher");
        contributoreRoleToContributorType.put("ResearchGroup", "Research Group");
        contributoreRoleToContributorType.put("RightsHolder", "Rights Holder");
        contributoreRoleToContributorType.put("Sponsor", "Sponsor");
        contributoreRoleToContributorType.put("Supervisor", "Supervisor");
        contributoreRoleToContributorType.put("WorkPackageLeader", "Work Package Leader");
        contributoreRoleToContributorType.put("Other", "Other");
        contributoreRoleToContributorType.put("Producer", "Other");
        contributoreRoleToContributorType.put("RegistrationAuthority", "Other");
        contributoreRoleToContributorType.put("RegistrationAgency", "Other");
        contributoreRoleToContributorType.put("Distributor", "Other");
        contributoreRoleToContributorType.put("DataCollector", "Other");
        contributoreRoleToContributorType.put("ContactPerson", "Other");
    }

    private final XmlReader xmlReader;
    private final Set<String> activeMetadataBlocks;
    private final Map<String, AbstractFieldBuilder> citationFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> rightsFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> relationFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> archaeologySpecificFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> temporalSpatialFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> dataVaultFields = new HashMap<>();

    private final String SCHEME_PAN = "PAN thesaurus ideaaltypes";
    private final String SCHEME_URI_PAN = "https://data.cultureelerfgoed.nl/term/id/pan/PAN";

    private final String SCHEME_AAT = "Art and Architecture Thesaurus";
    private final String SCHEME_URI_AAT = "http://vocab.getty.edu/aat/";

    private final Pattern SUBJECT_MATCH_PREFIX = Pattern.compile("^\\s*[a-zA-Z]+\\s+Match:\\s*");

    private final Pattern DATES_OF_COLLECTION_PATTERN = Pattern.compile("^(.*)/(.*)$");
    private final Map<String, String> iso1ToDataverseLanguage;
    private final Map<String, String> iso2ToDataverseLanguage;

    public DepositToDvDatasetMetadataMapper(XmlReader xmlReader, Set<String> activeMetadataBlocks, Map<String, String> iso1ToDataverseLanguage, Map<String, String> iso2ToDataverseLanguage) {
        this.xmlReader = xmlReader;
        this.activeMetadataBlocks = activeMetadataBlocks;
        this.iso1ToDataverseLanguage = iso1ToDataverseLanguage;
        this.iso2ToDataverseLanguage = iso2ToDataverseLanguage;
    }

    public Dataset toDataverseDataset(@NonNull Document ddm, @Nullable Document agreements) throws XPathExpressionException, MissingRequiredFieldException {

        if (activeMetadataBlocks.contains("citation")) {
            var alternativeTitles = getAlternativeTitles(ddm);
            addPrimitiveFieldSingleValue(citationFields, TITLE, getTitles(ddm));
            addPrimitiveFieldSingleValue(citationFields, ALTERNATIVE_TITLE, alternativeTitles);

            // TODO somehow get vault metadata object
            //            addCompoundFieldMultipleValues(citationFields, OTHER_ID, getOtherIdFromDepositProperties());
            addCompoundFieldMultipleValues(citationFields, OTHER_ID, getOtherIdFromDataset(ddm));
            // TODO add other doi's (should be an argument to this method or something)
            //            addCompoundFieldMultipleValues(citationFields, OTHER_ID, getOtherIdFromDataset(ddm));

            addCompoundFieldMultipleValues(citationFields, AUTHOR, getAuthorCreators(ddm));
            addCompoundFieldMultipleValues(citationFields, AUTHOR, getOrganizationCreators(ddm));
            addCompoundFieldMultipleValues(citationFields, AUTHOR, getCreators(ddm));

            // TODO add contactData (from arguments?)
            addCompoundFieldMultipleValues(citationFields, DESCRIPTION, getDescription(ddm, DESCRIPTION));

            if (alternativeTitles.size() > 0) {
                addCompoundFieldMultipleValues(citationFields, DESCRIPTION,
                    List.of(processDescription(DESCRIPTION, alternativeTitles.get(0))));
            }
            addCompoundFieldMultipleValues(citationFields, DESCRIPTION, getNonTechnicalDescription(ddm, DESCRIPTION));
            addCompoundFieldMultipleValues(citationFields, DESCRIPTION, getOtherDescriptions(ddm, DESCRIPTION));

            addCvFieldMultipleValues(citationFields, SUBJECT, getAudiencesDeprecated(ddm));

            addCompoundFieldMultipleValues(citationFields, KEYWORD, getKeywords(ddm));
            addCompoundFieldMultipleValues(citationFields, PUBLICATION, getRelatedPublication(ddm));
            addCvFieldMultipleValues(citationFields, LANGUAGE, getCitationBlockLanguage(ddm, LANGUAGE));
            addPrimitiveFieldSingleValue(citationFields, PRODUCTION_DATE, getProductionDate(ddm));

            addCompoundFieldMultipleValues(citationFields, CONTRIBUTOR, getContributorDetails(ddm));
            addCompoundFieldMultipleValues(citationFields, GRANT_NUMBER, getFunderDetails(ddm));
            addCompoundFieldMultipleValues(citationFields, GRANT_NUMBER, getNwoGrantNumbers(ddm));

            addCompoundFieldMultipleValues(citationFields, DISTRIBUTOR, getPublishers(ddm));
            addPrimitiveFieldSingleValue(citationFields, DISTRIBUTION_DATE, getDistributionDate(ddm));

            // TODO add date of deposit from arguments
            // TODO: from scala: what to set dateOfDeposit to for SWORD or multi-deposits? Take from deposit.properties?
            //            addPrimitiveFieldSingleValue(citationFields, DATE_OF_DEPOSIT, optDateOfDeposit)

            addCompoundFieldMultipleValues(citationFields, DATE_OF_COLLECTION, getDatesOfCollection(ddm));
            addPrimitiveFieldMultipleValues(citationFields, DATA_SOURCES, getDataSources(ddm));

        }
        else {
            throw new IllegalStateException("Metadatablock citation should always be active");
        }

        if (activeMetadataBlocks.contains("dansRights")) {
            // TODO this bit
            //            checkRequiredField(RIGHTS_HOLDER, ddm \ "dcmiMetadata" \ "rightsHolder")
            addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, getRightsHolders(ddm));

            // TODO check how this works
            //            OptionExtensions(optAgreements.map { agreements =>
            //                addCvFieldSingleValue(rightsFields, PERSONAL_DATA_PRESENT, agreements \ "personalDataStatement", PersonalStatement toHasPersonalDataValue)
            //            }).doIfNone(() => addCvFieldSingleValue(rightsFields, PERSONAL_DATA_PRESENT, "Unknown"))

            addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, getContributorDetailsAuthors(ddm)
                .filter(DcxDaiAuthor::isRightsHolder)
                .map(DcxDaiAuthor::toRightsHolder)
                .collect(Collectors.toList()));

            addPrimitiveFieldMultipleValues(rightsFields, RIGHTS_HOLDER, getContributorDetailsOrganizations(ddm)
                .filter(DcxDaiOrganization::isRightsHolder)
                .map(DcxDaiOrganization::toRightsHolder)
                .collect(Collectors.toList()));

            addCvFieldMultipleValues(rightsFields, LANGUAGE_OF_METADATA, getLanguageAttributes(ddm)
                .map(n -> Language.isoToDataverse(n, iso1ToDataverseLanguage, iso2ToDataverseLanguage))
                .collect(Collectors.toList()));
        }

        if (activeMetadataBlocks.contains("dansRelationMetadata")) {
            addPrimitiveFieldMultipleValues(relationFields, AUDIENCE, getAudiences(ddm)
                .map(Audience::toNarcisTerm)
                .collect(Collectors.toList()));

            addPrimitiveFieldMultipleValues(relationFields, COLLECTION, getInCollections(ddm)
                .map(InCollection::toCollection)
                .collect(Collectors.toList()));

            addCompoundFieldMultipleValues(relationFields, COLLECTION, getRelations(ddm)
                .filter(Relation::isRelation)
                .map(Relation::toRelationObject)
                .collect(Collectors.toList()));
        }

        if (activeMetadataBlocks.contains("dansArchaeologyMetadata")) {
            addPrimitiveFieldMultipleValues(archaeologySpecificFields, ARCHIS_ZAAK_ID, getIdentifiers(ddm)
                .filter(Identifier::isArchisZaakId)
                .map(Identifier::toArchisZaakId)
                .collect(Collectors.toList()));

            addCompoundFieldMultipleValues(archaeologySpecificFields, ARCHIS_NUMBER, getIdentifiers(ddm)
                .filter(Identifier::isArchisZaakId)
                .map(Identifier::toArchisNumberValue)
                .collect(Collectors.toList()));

            addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_RAPPORT_TYPE, getReportNumbers(ddm)
                .filter(AbrReportType::isAbrReportType)
                .map(AbrReportType::toAbrRapportType)
                .collect(Collectors.toList()));

            addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_RAPPORT_NUMMER, getReportNumbers(ddm)
                .map(Node::getTextContent)
                .collect(Collectors.toList()));

//            addPrimitiveFieldMultipleValues(archaeologySpecificFields, ABR_VERWERVINGSWIJZE, getAcquisitionMethods(ddm)
//                .map(Node::getTextContent)
//                .collect(Collectors.toList()));
        }

        return null;
    }

    private Stream<Node> getAcquisitionMethods(Document ddm) {
        return null;
    }

    Stream<Node> getReportNumbers(Document ddm) throws XPathExpressionException {
        // TODO verify namespace
        return xmlReader.xpathToStream(ddm,
            "//ddm:dcmiMetadata/ddm:reportNumber");
    }

    Stream<Node> getRelations(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm,
            "//ddm:dcmiMetadata//*");
    }

    Stream<Node> getInCollections(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm,
            "//ddm:dcmiMetadata/ddm:inCollection");
    }

    Stream<String> getLanguageAttributes(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStreamOfStrings(ddm, "//ddm:profile//@xml:lang | //ddm:dcmiMetadata//@xml:lang");
    }

    Stream<Node> getContributorDetailsOrganizations(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStream(ddm,
            "//ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:organization");
    }

    Stream<Node> getContributorDetailsAuthors(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStream(ddm,
            "//ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:author");
    }

    List<String> getProductionDate(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/ddm:created")
            .map(this::formatDate)
            .collect(Collectors.toList());
    }

    String formatDate(String text) {
        var date = DateTime.parse(text);
        return DateTimeFormat.forPattern("YYYY-MM-dd").print(date);
    }

    Stream<String> getAudiences(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/ddm:audience");
    }

    Collection<String> getAudiencesDeprecated(Document ddm) throws XPathExpressionException, MissingRequiredFieldException {
        var items = xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/ddm:audience")
            .collect(Collectors.toList());

        // verify all items are filled
        var hasEmpty = items.stream().filter(StringUtils::isBlank).findFirst();

        if (items.size() == 0 || hasEmpty.isPresent()) {
            throw new MissingRequiredFieldException("title");
        }

        return items.stream()
            .map(Audience::toCitationBlockSubject)
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getDescription(Document ddm, String fieldName) throws XPathExpressionException {
        return xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/dc:description")
            .map(item -> processDescription(fieldName, item))
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getNonTechnicalDescription(Document ddm, String fieldName) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm, "//ddm:dcmiMetadata/dcterms:description")
            // TODO check if the namespace should be set!
            .filter(item -> {
                var attr = item.getAttributes().getNamedItem("descriptionType");
                return attr == null || !"TechnicalInfo".equals(attr.getTextContent());
            })
            .map(Node::getTextContent)
            .map(item -> processDescription(fieldName, item))
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getTechnicalDescription(Document ddm, String fieldName) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm, "//ddm:dcmiMetadata/dcterms:description")
            // TODO check if the namespace should be set!
            .filter(item -> {
                var attr = item.getAttributes().getNamedItem("descriptionType");
                return attr != null && "TechnicalInfo".equals(attr.getTextContent());
            })
            .map(Node::getTextContent)
            .map(item -> processDescription(fieldName, item))
            .collect(Collectors.toList());
    }

    Map<String, MetadataField> processDescription(String fieldName, String value) {
        var newline = "\r\n|\n|\r";
        var paragraph = "(\r\n){2,}|\n{2,}|\r{2,}";

        var content = Arrays.stream(value.trim().split(paragraph))
            .map(String::trim)
            .map(p -> String.format("<p>%s</p>", p))
            .map(p -> Arrays.stream(p.split(newline))
                .map(String::trim)
                .collect(Collectors.joining("<br>")))
            .collect(Collectors.joining(""));

        var result = new HashMap<String, MetadataField>();
        result.put(fieldName, new PrimitiveSingleValueField(fieldName, content));
        return result;
    }

    Stream<Node> getIdentifiers(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm, "//ddm:dcmiMetadata/dcterms:identifier");
    }

    Collection<Map<String, MetadataField>> getOtherIdFromDataset(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm, "//ddm:dcmiMetadata/dcterms:identifier")
            .filter(node -> {
                var attribute = node.getAttributes().getNamedItemNS(NAMESPACE_XSI, "type");

                if (attribute == null) {
                    return true;
                }

                return attribute.getTextContent().endsWith("EASY2");
            })
            .map(node -> {
                var result = new HashMap<String, MetadataField>();
                var attribute = node.getAttributes().getNamedItemNS(NAMESPACE_XSI, "type");

                if (attribute == null) {
                    result.put(OTHER_ID_AGENCY, new PrimitiveSingleValueField(OTHER_ID_AGENCY, ""));
                    result.put(OTHER_ID_VALUE, new PrimitiveSingleValueField(OTHER_ID_VALUE, node.getTextContent()));
                }
                else {
                    result.put(OTHER_ID_AGENCY, new PrimitiveSingleValueField(OTHER_ID_AGENCY, "DANS-KNAW"));
                    result.put(OTHER_ID_VALUE, new PrimitiveSingleValueField(OTHER_ID_VALUE, node.getTextContent()));
                }

                return result;
            })
            .collect(Collectors.toList());

    }

    Optional<Map<String, MetadataField>> vaultMetadataOtherIdValue(String otherId) {
        if (StringUtils.isBlank(otherId)) {
            return Optional.empty();
        }

        var result = new HashMap<String, MetadataField>();
        var trimmed = otherId.trim();

        if (StringUtils.containsWhitespace(otherId)) {
            throw new IllegalArgumentException("Identifier must not contain whitespace");
        }

        var parts = trimmed.split(":");

        if (parts.length != 2) {
            throw new IllegalArgumentException("Other ID value has invalid format. It should be '<prefix>:<suffix>'");
        }

        result.put(OTHER_ID_AGENCY, new PrimitiveSingleValueField(OTHER_ID_AGENCY, parts[0]));
        result.put(OTHER_ID_VALUE, new PrimitiveSingleValueField(OTHER_ID_VALUE, parts[1]));

        return Optional.of(result);
    }

    List<String> getTitles(Document ddm) throws XPathExpressionException, MissingRequiredFieldException {

        var titles = xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/dc:title")
            .collect(Collectors.toList());

        // check if any of the titles are blank
        var hasEmpty = titles.stream()
            .map(StringUtils::isEmpty)
            .findFirst()
            .orElse(false);

        if (titles.size() == 0 || hasEmpty) {
            throw new MissingRequiredFieldException("title");
        }

        return titles;
    }

    List<String> getAlternativeTitles(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStreamOfStrings(ddm,
                "//ddm:dcmiMetadata/dcterms:title", "//ddm:dcmiMetadata/dcterms:alternative")
            .collect(Collectors.toList());
    }

    void addPrimitiveFieldSingleValue(Map<String, AbstractFieldBuilder> fields, String name, List<String> values) {
        var filteredValue = values.stream()
            .filter(Objects::nonNull)
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .orElse(null);

        if (filteredValue != null) {
            var builder = fields.getOrDefault(name, new PrimitiveFieldBuilder(name, false));

            if (builder instanceof PrimitiveFieldBuilder) {
                ((PrimitiveFieldBuilder) builder).addValue(filteredValue);
                fields.put(name, builder);
            }
            else {
                throw new IllegalArgumentException("Trying to add non-primitive value(s) to primitive field");
            }
        }
    }

    private boolean isNoCvAttributedNode(Node node) {
        var subjectScheme = node.getAttributes().getNamedItem("subjectScheme");
        var schemeUri = node.getAttributes().getNamedItem("schemeURI");

        // only return true for items that have no schemeUri and no subjectScheme
        return (subjectScheme == null || subjectScheme.getTextContent().isEmpty()) &&
            (schemeUri == null || schemeUri.getTextContent().isEmpty());
    }

    private boolean isTerm(Node node, String scheme, String uri) {
        var subjectScheme = Optional.ofNullable(node.getAttributes().getNamedItem("subjectScheme"));
        var schemeUri = Optional.ofNullable(node.getAttributes().getNamedItem("schemeURI"));

        return subjectScheme.map(m -> m.getTextContent().equals(scheme)).orElse(false)
            && schemeUri.map(m -> m.getTextContent().equals(uri)).orElse(false);

    }

    private boolean isPanTerm(Node node) {
        return isTerm(node, SCHEME_PAN, SCHEME_URI_PAN);
    }

    private boolean isAatTerm(Node node) {
        return isTerm(node, SCHEME_AAT, SCHEME_URI_AAT);
    }

    private HashMap<String, MetadataField> buildKeywordField(String value, String vocabulary, String vocabularyURI) {
        var result = new HashMap<String, MetadataField>();
        result.put(KEYWORD_VALUE, new PrimitiveSingleValueField(KEYWORD_VALUE, value));
        result.put(KEYWORD_VOCABULARY, new PrimitiveSingleValueField(KEYWORD_VOCABULARY, vocabulary));
        result.put(KEYWORD_VOCABULARY_URI, new PrimitiveSingleValueField(KEYWORD_VOCABULARY_URI, vocabularyURI));
        return result;
    }

    private HashMap<String, MetadataField> buildNoCvAttributedResult(Node node) {
        return buildKeywordField(node.getTextContent().trim(), "", "");
    }

    private HashMap<String, MetadataField> buildPanResult(Node node) {
        return buildKeywordField(replaceMatch(node.getTextContent().trim()), SCHEME_PAN, SCHEME_URI_PAN);
    }

    private HashMap<String, MetadataField> buildAatResult(Node node) {
        return buildKeywordField(replaceMatch(node.getTextContent().trim()), SCHEME_AAT, SCHEME_URI_AAT);
    }

    private String replaceMatch(String text) {
        return SUBJECT_MATCH_PREFIX.matcher(text).replaceAll("");
    }

    Collection<Map<String, MetadataField>> getKeywords(Document ddm) throws XPathExpressionException {
        var subjectExpression = "//ddm:dcmiMetadata/dcterms:subject";

        var s1 = xmlReader.xpathToStream(ddm, subjectExpression)
            .filter(this::isNoCvAttributedNode)
            .map(this::buildNoCvAttributedResult);

        var s2 = xmlReader.xpathToStream(ddm, subjectExpression)
            .filter(this::isPanTerm)
            .map(this::buildPanResult);

        var s3 = xmlReader.xpathToStream(ddm, subjectExpression)
            .filter(this::isAatTerm)
            .map(this::buildAatResult);

        // TODO extract language stuff into separate class and copy scala tests over because they are more extensive
        var languages = xmlReader.xpathToStream(ddm, "//ddm:dcmiMetadata/dcterms:language")
            .filter(node -> !Language.isIsoLanguage(node))
            .map(this::buildNoCvAttributedResult);

        var stream = Stream.concat(s1, s2);
        stream = Stream.concat(stream, s3);
        stream = Stream.concat(stream, languages);

        return stream.collect(Collectors.toList());
    }

    void addPrimitiveFieldMultipleValues(Map<String, AbstractFieldBuilder> fields, String name,
        Collection<String> values) { //metadataBlockFields: mutable.HashMap[String, AbstractFieldBuilder], name: String, sourceNodes: NodeSeq, nodeTransformer: Node => Option[String] = AnyElement toText): Unit = {

        var filteredValue = values.stream()
            .filter(Objects::nonNull)
            .filter(StringUtils::isNotBlank)
            .collect(Collectors.toList());

        var builder = fields.getOrDefault(name, new PrimitiveFieldBuilder(name, true));

        if (builder instanceof PrimitiveFieldBuilder) {
            var pfb = (PrimitiveFieldBuilder) builder;

            for (var value : filteredValue) {
                pfb.addValue(value);
            }
        }
        else {
            throw new IllegalArgumentException("Trying to add non-primitive value(s) to primitive field");
        }
    }

    void addCompoundFieldMultipleValues(Map<String, AbstractFieldBuilder> fields, String name, Collection<Map<String, MetadataField>> valueObjects) {
        var builder = fields.getOrDefault(name, new CompoundFieldBuilder(name, true));

        if (builder instanceof CompoundFieldBuilder) {
            var cfb = (CompoundFieldBuilder) builder;

            for (var o : valueObjects) {
                cfb.addValue(o);
            }
        }
        else {
            throw new IllegalArgumentException("Trying to add non-compound value(s) to compound field");
        }
    }

    void addCvFieldMultipleValues(Map<String, AbstractFieldBuilder> fields, String name, Collection<String> valueObjects) {
        var builder = fields.getOrDefault(name, new CompoundFieldBuilder(name, true));

        if (builder instanceof CompoundFieldBuilder) {
            var cfb = (CompoundFieldBuilder) builder;

            for (var o : valueObjects) {
                // TODO fix this
                //                cfb.addValue(o);
            }
        }
        else {
            throw new IllegalArgumentException("Trying to add non-compound value(s) to compound field");
        }
    }

    String getFirstValue(Node node, String expression) throws XPathExpressionException {
        return xmlReader.xpathToStreamOfStrings(node, expression).map(String::trim).findFirst().orElse(null);
    }

    Collection<Map<String, MetadataField>> getAuthorCreators(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm, "//ddm:profile/dcx-dai:creatorDetails/dcx-dai:author")
            .map(node -> {
                var result = new HashMap<String, MetadataField>();

                try {
                    var author = parseAuthor(node);
                    var name = formatName(author);

                    if (StringUtils.isNotBlank(name)) {
                        result.put(AUTHOR_NAME, new PrimitiveSingleValueField(AUTHOR_NAME, name));
                    }

                    if (author.getOrcid() != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "ORCID"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, author.getOrcid()));
                    }

                    else if (author.getIsni() != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "ISNI"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, author.getIsni()));
                    }

                    else if (author.getDai() != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "DAI"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, author.getDai()));
                    }

                    if (author.getOrganization() != null) {
                        result.put(AUTHOR_AFFILIATION, new PrimitiveSingleValueField(AUTHOR_AFFILIATION, author.getOrganization()));
                    }

                    return result;
                }
                catch (XPathExpressionException e) {
                    log.error("Xpath exception", e);
                }

                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getOrganizationCreators(Document ddm) throws XPathExpressionException {

        return xmlReader.xpathToStream(ddm, "//ddm:profile/dcx-dai:creatorDetails/dcx-dai:organization")
            .map(node -> {
                var result = new HashMap<String, MetadataField>();

                try {
                    var organization = parseOrganization(node);

                    if (StringUtils.isNotBlank(organization.getName())) {
                        result.put(AUTHOR_NAME, new PrimitiveSingleValueField(AUTHOR_NAME, organization.getName()));
                    }

                    if (organization.getIsni() != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "ISNI"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, organization.getIsni()));
                    }
                    else if (organization.getViaf() != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "VIAF"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, organization.getViaf()));
                    }

                    return result;
                }
                catch (XPathExpressionException e) {
                    log.error("Xpath exception", e);
                }

                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getCreators(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/dc:creator")
            .map(str -> {
                var result = new HashMap<String, MetadataField>();
                result.put(AUTHOR_NAME, new PrimitiveSingleValueField(AUTHOR_NAME, str));
                return result;
            })
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getOtherDescriptions(Document ddm, String fieldName) throws XPathExpressionException {
        var labelToPrefix = Map.of(
            "date", "Date",
            "valid", "Valid",
            "issued", "Issued",
            "modified", "Modified",
            "dateAccepted", "Date Accepted",
            "dateCopyrighted", "Date Copyrighted",
            "dateSubmitted", "Date Submitted",
            "coverage", "Coverage"
        );

        return xmlReader.xpathsToStream(ddm,
                "//ddm:dcmiMetadata/dcterms:date",
                "//ddm:dcmiMetadata/dcterms:dateAccepted",
                "//ddm:dcmiMetadata/dcterms:dateCopyrighted",
                "//ddm:dcmiMetadata/dcterms:modified",
                "//ddm:dcmiMetadata/dcterms:issued",
                "//ddm:dcmiMetadata/dcterms:valid",
                "//ddm:dcmiMetadata/dcterms:coverage")
            .map(node -> {
                var name = node.getLocalName();
                var prefix = labelToPrefix.getOrDefault(name, name);
                var result = new HashMap<String, MetadataField>();
                result.put(fieldName, new PrimitiveSingleValueField(fieldName, String.format("%s: %s", prefix, node.getTextContent())));
                return result;
            })
            .collect(Collectors.toList());
    }

    private boolean hasXsiType(Node node, String xsiType) {
        var attributes = node.getAttributes();

        if (attributes == null) {
            return false;
        }

        return Optional.ofNullable(attributes.getNamedItemNS(NAMESPACE_XSI, "type"))
            .map(item -> {
                var text = item.getTextContent();
                return xsiType.equals(text) || text.endsWith(":" + xsiType);
            })
            .orElse(false);
    }

    Collection<Map<String, MetadataField>> getRelatedPublication(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStream(ddm,
                "//ddm:dcmiMetadata/dcterms:identifier[@xsi:type]")
            .filter(node -> hasXsiType(node, "ISBN") || hasXsiType(node, "ISSN"))
            .map(node -> {
                var idType = Optional.ofNullable(node.getAttributes().getNamedItemNS(NAMESPACE_XSI, "type"))
                    .map(item -> {
                        var text = item.getTextContent();
                        return text.substring(text.indexOf(':') + 1);
                    })
                    .orElse("");

                var item = node.getTextContent();
                var result = new HashMap<String, MetadataField>();
                result.put(PUBLICATION_CITATION, new PrimitiveSingleValueField(PUBLICATION_CITATION, ""));
                result.put(PUBLICATION_ID_NUMBER, new PrimitiveSingleValueField(PUBLICATION_ID_NUMBER, item));
                result.put(PUBLICATION_URL, new PrimitiveSingleValueField(PUBLICATION_URL, ""));
                result.put(PUBLICATION_ID_TYPE, new ControlledSingleValueField(PUBLICATION_ID_TYPE, idType));

                return result;
            })
            .collect(Collectors.toList());

    }

    Collection<String> getCitationBlockLanguage(Document ddm, String name) throws XPathExpressionException {
        return xmlReader.xpathsToStream(ddm,
                "//ddm:dcmiMetadata/dcterms:language")
            .map(node -> {
                if (Language.isIsoLanguage(node)) {
                    return Optional.ofNullable(node.getAttributes().getNamedItem("code"))
                        .map(code -> isoToDataverse(code.getTextContent()))
                        .orElse("");
                }
                else {
                    return "";
                }
            })
            .filter(StringUtils::isNotBlank)
            .collect(Collectors.toList());
    }

    private String isoToDataverse(String code) {
        if (code.length() == 2) {
            return iso1ToDataverseLanguage.get(code);
        }

        return iso2ToDataverseLanguage.get(code);
    }

    private DatasetAuthor parseAuthor(Node node) throws XPathExpressionException {
        return DatasetAuthor.builder()
            .titles(getFirstValue(node, "dcx-dai:titles"))
            .initials(getFirstValue(node, "dcx-dai:initials"))
            .insertions(getFirstValue(node, "dcx-dai:insertions"))
            .surname(getFirstValue(node, "dcx-dai:surname"))
            .dai(getFirstValue(node, "dcx-dai:DAI"))
            .isni(getFirstValue(node, "dcx-dai:ISNI"))
            .orcid(getFirstValue(node, "dcx-dai:ORCID"))
            .role(getFirstValue(node, "dcx-dai:role"))
            .organization(getFirstValue(node, "dcx-dai:organization/dcx-dai:name"))
            .build();
    }

    private DatasetOrganization parseOrganization(Node node) throws XPathExpressionException {
        return DatasetOrganization.builder()
            .name(getFirstValue(node, "dcx-dai:name"))
            .role(getFirstValue(node, "dcx-dai:role"))
            .isni(getFirstValue(node, "dcx-dai:ISNI"))
            .viaf(getFirstValue(node, "dcx-dai:VIAF"))
            .build();
    }

    private String formatName(DatasetAuthor author) {
        return String.join(" ", List.of(
                Optional.ofNullable(author.getInitials()).orElse(""),
                Optional.ofNullable(author.getInsertions()).orElse(""),
                Optional.ofNullable(author.getSurname()).orElse("")
            ))
            .trim().replaceAll("\\s+", " ");
    }

    Collection<Map<String, MetadataField>> getContributorDetails(Document ddm) throws XPathExpressionException {
        var nodes = xmlReader.xpathsToStream(ddm, "//ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:author | //ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:organization")
            .filter(node -> {
                // if author, it should not be a rightsholder
                if ("author".equals(node.getLocalName())) {

                    try {
                        var author = parseAuthor(node);
                        return !StringUtils.contains(author.getRole(), "RightsHolder");
                    }
                    catch (XPathExpressionException e) {
                        return false;
                    }
                }

                return true;
            })
            .filter(node -> {
                if ("organization".equals(node.getLocalName())) {
                    try {
                        var organization = parseOrganization(node);
                        return !Set.of("RightsHolder", "Funder").contains(organization.getRole());
                    }
                    catch (XPathExpressionException e) {
                        return false;
                    }
                }
                return true;
            })
            .collect(Collectors.toList());

        var result = new ArrayList<Map<String, MetadataField>>();

        for (var node : nodes) {
            var item = new HashMap<String, MetadataField>();

            if ("author".equals(node.getLocalName())) {
                var author = parseAuthor(node);
                var name = formatName(author);

                if (StringUtils.isNotBlank(name)) {
                    var completeName = author.getOrganization() != null
                        ? String.format("%s (%s)", name, author.getOrganization())
                        : name;

                    item.put(CONTRIBUTOR_NAME, new PrimitiveSingleValueField(CONTRIBUTOR_NAME, completeName));
                }
                else if (StringUtils.isNotBlank(author.getOrganization())) {
                    item.put(CONTRIBUTOR_NAME, new PrimitiveSingleValueField(CONTRIBUTOR_NAME, author.getOrganization()));
                }

                if (StringUtils.isNotBlank(author.getRole())) {
                    var value = contributoreRoleToContributorType.getOrDefault(author.getRole(), "Other");
                    item.put(CONTRIBUTOR_TYPE, new PrimitiveSingleValueField(CONTRIBUTOR_TYPE, value));
                }
            }

            else if ("organization".equals(node.getLocalName())) {
                var organization = parseOrganization(node);

                if (StringUtils.isNotBlank(organization.getName())) {
                    item.put(CONTRIBUTOR_NAME, new PrimitiveSingleValueField(CONTRIBUTOR_NAME, organization.getName()));
                }

                if (StringUtils.isNotBlank(organization.getRole())) {
                    var value = contributoreRoleToContributorType.getOrDefault(organization.getRole(), "Other");
                    item.put(CONTRIBUTOR_TYPE, new PrimitiveSingleValueField(CONTRIBUTOR_TYPE, value));
                }
            }

            if (item.keySet().size() > 0) {
                result.add(item);
            }
        }

        return result;
    }

    Collection<Map<String, MetadataField>> getFunderDetails(Document ddm) throws XPathExpressionException {

        return xmlReader.xpathsToStream(ddm, "//ddm:dcmiMetadata/dcx-dai:contributorDetails/dcx-dai:organization[normalize-space(dcx-dai:role/text()) = 'Funder']")
            .map(node -> {
                try {
                    var org = parseOrganization(node);
                    var item = new HashMap<String, MetadataField>();
                    item.put(GRANT_NUMBER_AGENCY, new PrimitiveSingleValueField(GRANT_NUMBER_AGENCY, org.getName().trim()));
                    item.put(GRANT_NUMBER_VALUE, new PrimitiveSingleValueField(GRANT_NUMBER_VALUE, ""));

                    return item;
                }
                catch (XPathExpressionException e) {
                    log.error("Error parsing organization", e);
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    }

    Collection<Map<String, MetadataField>> getNwoGrantNumbers(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStream(ddm, "//ddm:dcmiMetadata/dcterms:identifier")
            .filter(node -> hasXsiType(node, "NWO-PROJECTNR"))
            .map(node -> {
                var result = new HashMap<String, MetadataField>();
                result.put(GRANT_NUMBER_AGENCY, new PrimitiveSingleValueField(GRANT_NUMBER_AGENCY, "NWO"));
                result.put(GRANT_NUMBER_VALUE, new PrimitiveSingleValueField(GRANT_NUMBER_VALUE, node.getTextContent()));

                return result;
            })
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getPublishers(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStream(ddm, "//ddm:dcmiMetadata/dcterms:publisher")
            .filter(node -> {
                var dansNames = Set.of("DANS", "DANS-KNAW", "DANS/KNAW");
                return !dansNames.contains(node.getTextContent());
            })
            .map(node -> {
                var result = new HashMap<String, MetadataField>();
                result.put(DISTRIBUTOR_NAME, new PrimitiveSingleValueField(DISTRIBUTOR_NAME, node.getTextContent()));
                result.put(DISTRIBUTOR_URL, new PrimitiveSingleValueField(DISTRIBUTOR_URL, node.getTextContent()));
                result.put(DISTRIBUTOR_LOGO_URL, new PrimitiveSingleValueField(DISTRIBUTOR_LOGO_URL, node.getTextContent()));
                result.put(DISTRIBUTOR_ABBREVIATION, new PrimitiveSingleValueField(DISTRIBUTOR_ABBREVIATION, node.getTextContent()));
                result.put(DISTRIBUTOR_AFFILIATION, new PrimitiveSingleValueField(DISTRIBUTOR_AFFILIATION, node.getTextContent()));

                return result;
            })
            .collect(Collectors.toList());
    }

    List<String> getDistributionDate(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStreamOfStrings(ddm, "//ddm:profile/ddm:available")
            .map(this::formatDate)
            .collect(Collectors.toList());
    }

    Collection<Map<String, MetadataField>> getDatesOfCollection(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStreamOfStrings(ddm, "//ddm:dcmiMetadata/ddm:datesOfCollection")
            .map(text -> {
                var result = new HashMap<String, MetadataField>();
                var matches = DATES_OF_COLLECTION_PATTERN.matcher(text.trim());

                if (matches.matches()) {
                    result.put(DATE_OF_COLLECTION_START, new PrimitiveSingleValueField(DATE_OF_COLLECTION_START, matches.group(1)));
                    result.put(DATE_OF_COLLECTION_END, new PrimitiveSingleValueField(DATE_OF_COLLECTION_END, matches.group(2)));
                }

                return result;
            })
            .filter(result -> !result.isEmpty())
            .collect(Collectors.toList());
    }

    Collection<String> getDataSources(Document ddm) throws XPathExpressionException {
        // TODO verify the correct namespace, there are no examples?
        return xmlReader.xpathsToStreamOfStrings(ddm, "//ddm:dcmiMetadata/dcterms:source")
            .collect(Collectors.toList());
    }

    Collection<String> getRightsHolders(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathsToStreamOfStrings(ddm, "//ddm:dcmiMetadata/dcterms:rightsHolder")
            .collect(Collectors.toList());
    }
}