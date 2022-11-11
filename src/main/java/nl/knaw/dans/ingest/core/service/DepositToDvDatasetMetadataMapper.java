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

import com.google.common.collect.Comparators;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.easy.dd2d.fieldbuilders.AbstractFieldBuilder;
import nl.knaw.dans.easy.dd2d.fieldbuilders.CompoundFieldBuilder;
import nl.knaw.dans.easy.dd2d.fieldbuilders.PrimitiveFieldBuilder;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ALTERNATIVE_TITLE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TITLE;
import static nl.knaw.dans.ingest.core.service.XmlReader.NAMESPACE_XSI;

@Slf4j
public class DepositToDvDatasetMetadataMapper {

    // TODO put this somewhere else
    static Map<String, String> narcisToSubject = new HashMap<>();

    static {
        narcisToSubject.put("D11", "Mathematical Sciences");
        narcisToSubject.put("D12", "Physics");
        narcisToSubject.put("D13", "Chemistry");
        narcisToSubject.put("D14", "Engineering");
        narcisToSubject.put("D16", "Computer and Information Science");
        narcisToSubject.put("D17", "Astronomy and Astrophysics");
        narcisToSubject.put("D18", "Agricultural Sciences");
        narcisToSubject.put("D2", "Medicine, Health and Life Sciences");
        narcisToSubject.put("D3", "Arts and Humanities");
        narcisToSubject.put("D41", "Law");
        narcisToSubject.put("D5", "Social Sciences");
        narcisToSubject.put("D6", "Social Sciences");
        narcisToSubject.put("D42", "Social Sciences");
        narcisToSubject.put("D7", "Business and Management");
        narcisToSubject.put("D15", "Earth and Environmental Sciences");
        narcisToSubject.put("E15", "Earth and Environmental Sciences");
    }

    private final XmlReader xmlReader;
    private final Set<String> activeMetadataBlocks;
    private final Map<String, AbstractFieldBuilder> citationFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> rightsFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> relationFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> archaeologySpecificFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> temporalSpatialFields = new HashMap<>();
    private final Map<String, AbstractFieldBuilder> dataVaultFields = new HashMap<>();

    public DepositToDvDatasetMetadataMapper(XmlReader xmlReader, Set<String> activeMetadataBlocks) {
        this.xmlReader = xmlReader;
        this.activeMetadataBlocks = activeMetadataBlocks;
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

            addCvFieldMultipleValues(citationFields, SUBJECT, getAudiences(ddm, DESCRIPTION));
        }

        return null;
    }

    String mapNarcisClassification(String code) {
        if (!code.matches("^[D|E]\\d{5}$")) {
            throw new RuntimeException("NARCIS classification code incorrectly formatted");
        }

        return narcisToSubject.keySet().stream()
            .filter(code::startsWith)
            .max((a, b) -> Comparators.max(a.length(), b.length()))
            .map(narcisToSubject::get)
            .orElse("Other");
    }

    Collection<Map<String, MetadataField>> getAudiences(Document ddm, String fieldName) throws XPathExpressionException {
        var items = xmlReader.xpathToStreamOfStrings(ddm, "//ddm:profile/ddm:audience")
            .collect(Collectors.toList());

        var values = new ArrayList<String>();

        for (var item : items) {
            values.add(mapNarcisClassification(item));
        }

        var result = new HashMap<String, MetadataField>();
        result.put(fieldName, new ControlledMultiValueField(fieldName, values));
        return List.of(result);
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

    void addCvFieldMultipleValues(Map<String, AbstractFieldBuilder> fields, String name, Collection<Map<String, MetadataField>> valueObjects) {
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

    String getFirstValue(Node node, String expression) throws XPathExpressionException {
        return xmlReader.xpathToStreamOfStrings(node, expression).findFirst().orElse(null);
    }

    Collection<Map<String, MetadataField>> getAuthorCreators(Document ddm) throws XPathExpressionException {
        return xmlReader.xpathToStream(ddm, "//ddm:profile/dcx-dai:creatorDetails/dcx-dai:author")
            .map(node -> {
                var result = new HashMap<String, MetadataField>();

                try {
                    var titles = getFirstValue(node, "dcx-dai:titles");
                    var initials = getFirstValue(node, "dcx-dai:initials");
                    var insertions = getFirstValue(node, "dcx-dai:insertions");
                    var surname = getFirstValue(node, "dcx-dai:surname");
                    var dai = getFirstValue(node, "dcx-dai:DAI");
                    var isni = getFirstValue(node, "dcx-dai:ISNI");
                    var orcid = getFirstValue(node, "dcx-dai:ORCID");
                    var role = getFirstValue(node, "dcx-dai:role");
                    var organization = getFirstValue(node, "dcx-dai:organization/dcx-dai:name");

                    var name = String.join(" ", List.of(
                            Optional.ofNullable(initials).orElse(""),
                            Optional.ofNullable(insertions).orElse(""),
                            Optional.ofNullable(surname).orElse("")
                        ))
                        .trim().replaceAll("\\s+", " ");

                    if (StringUtils.isNotBlank(name)) {
                        result.put(AUTHOR_NAME, new PrimitiveSingleValueField(AUTHOR_NAME, name));
                    }

                    if (orcid != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "ORCID"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, orcid));
                    }

                    else if (isni != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "ISNI"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, isni));
                    }

                    else if (dai != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "DAI"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, dai));
                    }

                    if (organization != null) {
                        result.put(AUTHOR_AFFILIATION, new PrimitiveSingleValueField(AUTHOR_AFFILIATION, organization));
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
                    var name = getFirstValue(node, "dcx-dai:name");
                    var isni = getFirstValue(node, "dcx-dai:ISNI");
                    var viaf = getFirstValue(node, "dcx-dai:VIAF");

                    if (StringUtils.isNotBlank(name)) {
                        result.put(AUTHOR_NAME, new PrimitiveSingleValueField(AUTHOR_NAME, name));
                    }

                    if (isni != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "ISNI"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, isni));
                    }
                    else if (viaf != null) {
                        result.put(AUTHOR_IDENTIFIER_SCHEME, new ControlledSingleValueField(AUTHOR_IDENTIFIER_SCHEME, "VIAF"));
                        result.put(AUTHOR_IDENTIFIER, new PrimitiveSingleValueField(AUTHOR_IDENTIFIER, viaf));
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
}