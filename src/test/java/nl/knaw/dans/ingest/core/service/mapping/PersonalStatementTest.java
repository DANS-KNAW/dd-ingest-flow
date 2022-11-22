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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PersonalStatementTest extends BaseTest {

    @Test
    void testIfFalse() throws Exception {
        var str = "<personalDataStatement>\n"
            + "        <signerId easy-account=\"user001\" email=\"info@dans.knaw.nl\">MisterX</signerId>\n"
            + "        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>\n"
            + "        <containsPrivacySensitiveData>false</containsPrivacySensitiveData>\n"
            + "    </personalDataStatement>";

        var node = xmlReader.readXmlString(str);
        assertEquals("No", PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }

    @Test
    void testIfTrue() throws Exception {
        var str = "<personalDataStatement>\n"
            + "        <signerId easy-account=\"user001\" email=\"info@dans.knaw.nl\">MisterX</signerId>\n"
            + "        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>\n"
            + "        <containsPrivacySensitiveData>true</containsPrivacySensitiveData>\n"
            + "    </personalDataStatement>";

        var node = xmlReader.readXmlString(str);
        assertEquals("Yes", PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }

    @Test
    void testIfNotAvailable() throws Exception {
        var str = "<personalDataStatement><notAvailable/></personalDataStatement>";
        var node = xmlReader.readXmlString(str);
        assertEquals("Unknown", PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }

    @Test
    void testIfNoProperElementsExist() throws Exception {
        var str = "<personalDataStatement><blah/></personalDataStatement>";
        var node = xmlReader.readXmlString(str);
        assertNull(PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }
}