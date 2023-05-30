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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BaseTest {
    protected final XmlReader xmlReader = new XmlReaderImpl();

    Document readDocument(String name) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlFile(Path.of(
            Objects.requireNonNull(getClass().getResource(String.format("/xml/%s", name))).getPath()
        ));
    }

    public Document readDocumentFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        return xmlReader.readXmlString(xml);
    }

    Map<String, String> getMap(Path path) throws IOException {
        return loadCsvToMap(path, "Variant", "Normalized");
    }

    List<URI> getUriList(Path path) throws URISyntaxException, IOException {
        var uris = FileUtils.readLines(path.toFile(), StandardCharsets.UTF_8);
        var result = new ArrayList<URI>();

        for (var u : uris) {
            result.add(new URI(u));
        }

        return result;
    }

    Map<String, String> loadCsvToMap(Path path, String keyColumn, String valueColumn) throws IOException {
        try (var parser = CSVParser.parse(path, StandardCharsets.UTF_8, CSVFormat.RFC4180.withFirstRecordAsHeader())) {
            var result = new HashMap<String, String>();

            for (var record : parser) {
                result.put(record.get(keyColumn), record.get(valueColumn));
            }

            return result;
        }
    }

}
