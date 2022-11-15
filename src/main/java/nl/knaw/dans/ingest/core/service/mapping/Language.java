package nl.knaw.dans.ingest.core.service.mapping;

import org.w3c.dom.Node;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Language extends Base {
    public static boolean isIsoLanguage(Node node) {
        var isoLanguages = Set.of("ISO639-1", "ISO639-2");
        var hasTypes = hasXsiType(node, "ISO639-1") || hasXsiType(node, "ISO639-2");

        var attributes = Optional.ofNullable(node.getAttributes());
        var hasEncoding = attributes.map(a -> Optional.ofNullable(a.getNamedItem("encodingScheme")))
            .flatMap(i -> i)
            .map(Node::getTextContent)
            .map(isoLanguages::contains)
            .orElse(false);

        return hasTypes || hasEncoding;
    }

    public static String isoToDataverse(String code, Map<String, String> iso1ToDataverseLanguage, Map<String, String> iso2ToDataverseLanguage) {
        if (code.length() == 2) {
            return iso1ToDataverseLanguage.get(code);
        }

        return iso2ToDataverseLanguage.get(code);
    }
}
