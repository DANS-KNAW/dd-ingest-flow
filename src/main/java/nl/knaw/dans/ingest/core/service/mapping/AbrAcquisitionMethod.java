package nl.knaw.dans.ingest.core.service.mapping;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.service.XmlReader;
import org.w3c.dom.Node;

@Slf4j
public class AbrAcquisitionMethod extends Base {
    public static String toVerwervingswijze(Node node) {
        // TODO (from scala): also take attribute namespace into account (should be ddm)

        return getAttribute(node, XmlReader.NAMESPACE_DDM, "valueURI")
            .map(Node::getTextContent)
            .orElseGet(() -> {
                log.error("Missing valueURI attribute on ddm:acquisitionMethod node");
                return null;
            });
    }
}
