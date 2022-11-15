package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class BaseTest {
    protected final XmlReader xmlReader = new XmlReaderImpl();

    Document readDocument(String name) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlFile(Path.of(
            Objects.requireNonNull(getClass().getResource(String.format("/xml/%s", name))).getPath()
        ));
    }

    Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlString(xml);
    }
}
