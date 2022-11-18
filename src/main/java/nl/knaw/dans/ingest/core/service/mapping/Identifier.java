package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_CITATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_URL;
import static nl.knaw.dans.ingest.core.service.XmlReader.NAMESPACE_XSI;

public class Identifier extends Base {

    private final static Map<String, String> archisNumberTypeToCvItem = Map.of(
        "ARCHIS-ONDERZOEK", "onderzoek",
        "ARCHIS-VONDSTMELDING", "vondstmelding",
        "ARCHIS-MONUMENT", "monument",
        "ARCHIS-WAARNEMING", "waarneming"
    );

    public static CompoundFieldGenerator<Node> toRelatedPublicationValue = (builder, node) -> {
        var text = node.getTextContent();

        builder.addSubfield(PUBLICATION_CITATION, "");
        builder.addSubfield(PUBLICATION_ID_NUMBER, text);
        builder.addSubfield(PUBLICATION_ID_TYPE, getIdType(node));
        builder.addSubfield(PUBLICATION_URL, "");
    };

    public static CompoundFieldGenerator<Node> toOtherIdValue = (builder, node) -> {
        var text = node.getTextContent();

        if (hasXsiType(node, "EASY2")) {
            builder.addSubfield(OTHER_ID_AGENCY, "DANS-KNAW");
            builder.addSubfield(OTHER_ID_VALUE, text);
        }
        else if (!hasAttribute(node, XmlReader.NAMESPACE_XML, "type")) {
            builder.addSubfield(OTHER_ID_AGENCY, "");
            builder.addSubfield(OTHER_ID_VALUE, text);
        }
    };

    public static CompoundFieldGenerator<Node> toArchisNumberValue = (builder, node) -> {
        var numberType = getAttribute(node, XmlReader.NAMESPACE_XML, "type")
            .map(Node::getTextContent)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("Archis number without type"));

        var numberId = Optional.ofNullable(node.getTextContent())
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("Archis number without ID"));

        var type = archisNumberTypeToCvItem.get(numberType.substring(numberType.indexOf(':') + 1));
        builder.addControlledSubfield(ARCHIS_NUMBER_TYPE, type);
        builder.addSubfield(ARCHIS_NUMBER_ID, numberId);
    };

    private static String getIdType(Node node) {
        return Optional.ofNullable(node.getAttributes().getNamedItemNS(NAMESPACE_XSI, "type"))
            .map(item -> {
                var text = item.getTextContent();
                return text.substring(text.indexOf(':') + 1);
            })
            .orElse("");
    }

    public static boolean isArchisZaakId(Node node) {
        return hasXsiType(node, "ARCHIS-ZAAK-IDENTIFICATIE");
    }

    public static boolean isRelatedPublication(Node node) {
        return hasXsiType(node, "ISBN") || hasXsiType(node, "ISSN");
    }

    public static String toArchisZaakId(Node node) {
        return node.getTextContent();
    }

    public static Map<String, MetadataField> toArchisNumberValue(Node node) {
        var result = new HashMap<String, MetadataField>();

        var numberType = getAttribute(node, XmlReader.NAMESPACE_XML, "type")
            .map(Node::getTextContent)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("Archis number without type"));

        var numberId = Optional.ofNullable(node.getTextContent())
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("Archis number without ID"));

        var type = archisNumberTypeToCvItem.get(numberType.substring(numberType.indexOf(':') + 1));
        result.put(ARCHIS_NUMBER_TYPE, new ControlledSingleValueField(ARCHIS_NUMBER_TYPE, type));
        result.put(ARCHIS_NUMBER_ID, new PrimitiveSingleValueField(ARCHIS_NUMBER_ID, numberId));

        return result;
    }

    public static boolean canBeMappedToOtherId(Node node) {
        return hasXsiType(node, "EASY2") || !hasAttribute(node, XmlReader.NAMESPACE_XML, "type");
    }

    public static boolean isArchisNumber(Node node) {
        return hasXsiType(node, "ARCHIS-ONDERZOEK") ||
            hasXsiType(node, "ARCHIS-VONDSTMELDING") ||
            hasXsiType(node, "ARCHIS-MONUMENT") ||
            hasXsiType(node, "ARCHIS-WAARNEMING");
    }
}
