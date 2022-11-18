package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_URI;

public class Relation extends Base {

    private static Map<String, String> labelToType = new HashMap<>();
    public static CompoundFieldGenerator<Node> toRelationObject = (builder, node) -> {
        var href = getAttribute(node, "href")
            .map(Node::getTextContent)
            .orElse("");

        var nodeName = node.getLocalName();

        builder.addControlledSubfield(RELATION, labelToType.getOrDefault(nodeName, nodeName));
        builder.addSubfield(RELATION_URI, href);
        builder.addSubfield(RELATION_TEXT, node.getTextContent());
    };

    static {
        labelToType.put("relation", "relation");
        labelToType.put("conformsTo", "conforms to");
        labelToType.put("hasFormat", "has format");
        labelToType.put("hasPart", "has part");
        labelToType.put("references", "references");
        labelToType.put("replaces", "replaces");
        labelToType.put("requires", "requires");
        labelToType.put("hasVersion", "has version");
        labelToType.put("isFormatOf", "is format of");
        labelToType.put("isPartOf", "is part of");
        labelToType.put("isReferencedBy", "is referenced by");
        labelToType.put("isReplacedBy", "is replaced by");
        labelToType.put("isRequiredBy", "is required by");
        labelToType.put("isVersionOf", "is version of");
    }

    public static boolean isRelation(Node node) {
        return labelToType.containsKey(node.getLocalName());
    }

    public static Map<String, MetadataField> toRelationObject(Node node) {
        var href = Optional.ofNullable(node.getAttributes())
            .map(n -> Optional.ofNullable(n.getNamedItem("href")))
            .flatMap(i -> i)
            .map(Node::getTextContent)
            .orElse("");

        var nodeName = node.getLocalName();

        var result = new HashMap<String, MetadataField>();
        result.put(RELATION, new ControlledSingleValueField(RELATION, labelToType.getOrDefault(nodeName, nodeName)));
        result.put(RELATION_URI, new PrimitiveSingleValueField(RELATION_URI, href));
        result.put(RELATION_TEXT, new PrimitiveSingleValueField(RELATION_TEXT, node.getTextContent()));

        return result;
    }
}
