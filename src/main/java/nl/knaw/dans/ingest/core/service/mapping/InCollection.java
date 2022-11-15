package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.XmlReader;
import org.w3c.dom.Node;

import java.util.Optional;

@Slf4j
public final class InCollection extends Base {

    public static String toCollection(Node node) {
        return Optional.ofNullable(node.getAttributes())
            .map(n -> Optional.ofNullable(n.getNamedItemNS(XmlReader.NAMESPACE_DDM, "valueUri")))
            .flatMap(i -> i)
            .map(Node::getTextContent)
            .orElseGet(() -> {
                log.error("Missing valueURI attribute on ddm:inCollection node");
                return null;
            });
    }
}
