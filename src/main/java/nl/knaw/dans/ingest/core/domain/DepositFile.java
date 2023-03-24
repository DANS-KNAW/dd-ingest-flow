package nl.knaw.dans.ingest.core.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.w3c.dom.Node;

import java.nio.file.Path;

@Data
@AllArgsConstructor
public class DepositFile {
    private Path path;
    private Path physicalPath;
    private String checksum;
    private Node xmlNode;
}
