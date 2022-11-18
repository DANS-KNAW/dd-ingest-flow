package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.w3c.dom.Node;

public class Author extends Base {

    public static CompoundFieldGenerator<Node> toAuthorValueObject = (builder, node) -> {

        var author = getChildNode(node, "dcx-dai:author");
        var organization = getChildNode(node, "dcx-dai:organization");

        var localName = node.getLocalName();

        if (localName.equals("creatorDetails") && author.isPresent()) {
            DcxDaiAuthor.toAuthorValueObject.build(builder, node);
        }
        else if (localName.equals("creatorDetails") && organization.isPresent()) {
            DcxDaiOrganization.toAuthorValueObject.build(builder, node);
        }
        else if (localName.equals("creator")) {
            Creator.toAuthorValueObject.build(builder, node);
        }
    };

}
