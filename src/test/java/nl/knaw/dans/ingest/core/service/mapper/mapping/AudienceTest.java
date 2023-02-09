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

import nl.knaw.dans.ingest.core.service.mapper.mapping.Audience;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AudienceTest {

    @Test
    void toCitationBlockSubject_should_map_the_audience_codes_to_correct_subjects() {
        var tests = new HashMap<String, String>();
        tests.put("D11000", "Mathematical Sciences");
        tests.put("D12300", "Physics");
        tests.put("D13200", "Chemistry");
        tests.put("D14320", "Engineering");
        tests.put("D16000", "Computer and Information Science");
        tests.put("D17000", "Astronomy and Astrophysics");
        tests.put("D18220", "Agricultural Sciences");
        tests.put("D22200", "Medicine, Health and Life Sciences");
        tests.put("D36000", "Arts and Humanities");
        tests.put("D41100", "Law");
        tests.put("D65000", "Social Sciences");
        tests.put("D42100", "Social Sciences");
        tests.put("D70100", "Business and Management");
        tests.put("D15300", "Earth and Environmental Sciences");

        for (var test : tests.entrySet()) {
            var result = Audience.toCitationBlockSubject(test.getKey());
            assertEquals(test.getValue(), result);
        }
    }

    @Test
    void toCitationBlockSubject_should_map_unknown_audience_codes_to_Other() {
        assertEquals("Other", Audience.toCitationBlockSubject("D99999"));
    }

    @Test
    void toCitationBlockSubject_should_throw_a_RuntimeException_for_an_invalid_NARCIS_audience_code() {
        assertThrows(RuntimeException.class, () -> Audience.toCitationBlockSubject("INVALID"));
    }

}