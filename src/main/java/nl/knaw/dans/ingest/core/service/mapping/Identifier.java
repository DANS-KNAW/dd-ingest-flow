package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_TYPE;

public class Identifier extends Base {

    private final static Map<String, String> archisNumberTypeToCvItem = Map.of(
        "ARCHIS-ONDERZOEK", "onderzoek",
        "ARCHIS-VONDSTMELDING", "vondstmelding",
        "ARCHIS-MONUMENT", "monument",
        "ARCHIS-WAARNEMING", "waarneming"
    );

    public static boolean isArchisZaakId(Node node) {
        return hasXsiType(node, "ARCHIS-ZAAK-IDENTIFICATIE");
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
}
