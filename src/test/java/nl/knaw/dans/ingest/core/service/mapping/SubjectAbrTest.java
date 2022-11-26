/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class SubjectAbrTest extends BaseTest {

    @Test
    void is_abr_complex() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = XPathEvaluator.nodes(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrComplex)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("ABR COMPLEX");

    }

    @Test
    void is_old_abr() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = XPathEvaluator.nodes(doc, "//ddm:subject")
            .filter(SubjectAbr::isOldAbr)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("ABR BASIS REGISTER OLD");
    }

    @Test
    void is_abr_artifact() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = XPathEvaluator.nodes(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrArtifact)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(Node::getTextContent)
            .map(String::trim)
            .containsOnly("ABR ARTEFACTEN");
    }

    @Test
    void to_abr_complex() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = XPathEvaluator.nodes(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrComplex)
            .map(SubjectAbr::toAbrComplex)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(String::trim)
            .containsOnly("https://test3.com/");
    }

    @Test
    void to_abr_artifact() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = XPathEvaluator.nodes(doc, "//ddm:subject")
            .filter(SubjectAbr::isAbrArtifact)
            .map(SubjectAbr::toAbrArtifact)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(String::trim)
            .containsOnly("https://test5.com/");
    }

    @Test
    void from_abr_old_to_abr_artifact() throws Exception {
        var doc = readDocument("abrs.xml");
        var nodes = XPathEvaluator.nodes(doc, "//ddm:subject")
            .filter(SubjectAbr::isOldAbr)
            .map(SubjectAbr::fromAbrOldToAbrArtifact)
            .collect(Collectors.toList());

        assertThat(nodes)
            .map(String::trim)
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/supersecret");
    }
}