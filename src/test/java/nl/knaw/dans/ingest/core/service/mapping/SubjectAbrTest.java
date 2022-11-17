package nl.knaw.dans.ingest.core.service.mapping;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class SubjectAbrTest extends BaseTest {

    @Test
    void isAbrComplex() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = xmlReader.xpathToStream(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrComplex)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("ABR COMPLEX");

    }

    @Test
    void isOldAbr() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = xmlReader.xpathToStream(doc, "//ddm:subject")
            .filter(SubjectAbr::isOldAbr)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("ABR BASIS REGISTER OLD");
    }

    @Test
    void isAbrArtifact() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = xmlReader.xpathToStream(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrArtifact)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("ABR ARTEFACTEN");
    }

    @Test
    void toAbrComplex() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = xmlReader.xpathToStream(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrComplex)
            .map(SubjectAbr::toAbrComplex)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(String::trim)
            .containsOnly("https://test3.com/");
    }

    @Test
    void toAbrArtifact() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = xmlReader.xpathToStream(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrArtifact)
            .map(SubjectAbr::toAbrArtifact)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(String::trim)
            .containsOnly("https://test5.com/");
    }

    @Test
    void fromAbrOldToAbrArtifact() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = xmlReader.xpathToStream(doc, "//ddm:subject")
            .filter(SubjectAbr::isOldAbr)
            .map(SubjectAbr::fromAbrOldToAbrArtifact)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(String::trim)
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/supersecret");
    }
}