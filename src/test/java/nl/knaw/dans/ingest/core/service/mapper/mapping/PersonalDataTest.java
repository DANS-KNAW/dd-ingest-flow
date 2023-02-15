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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersonalDataTest extends BaseTest {
    @Test
    void getPersonalData_should_return_Yes_from_present_attribute() throws Exception {
        var doc = readDocumentFromString("<ddm:personalData present=\"Yes\" \n"
            + "            xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "            </ddm:personalData>");

        assertEquals("Yes", PersonalData.toPersonalDataPresent(doc.getDocumentElement()));
    }

    @Test
    void getPersonalData_should_return_No_from_present_attribute() throws Exception {
        var doc = readDocumentFromString("<ddm:personalData present=\"No\" \n"
            + "            xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "            </ddm:personalData>");

        assertEquals("No", PersonalData.toPersonalDataPresent(doc.getDocumentElement()));
    }

    @Test
    void getPersonalData_should_ignore_element_text() throws Exception {
        var doc = readDocumentFromString("<ddm:personalData present=\"No\" \n"
            + "            xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "            SOME ELEMENT TEXT"
            + "            </ddm:personalData>");

        assertEquals("No", PersonalData.toPersonalDataPresent(doc.getDocumentElement()));
    }

}
