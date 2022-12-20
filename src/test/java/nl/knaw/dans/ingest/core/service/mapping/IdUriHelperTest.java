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

import static nl.knaw.dans.ingest.core.service.mapping.IdUriHelper.reduceUriToOrcidId;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IdUriHelperTest {
    @Test
    void reduceUriToOrcidId_should_fix_input() {
        assertEquals("0000-0000-1234-567X", reduceUriToOrcidId("http://bla/0012-34567X"));
    }
    @Test
    void reduceUriToOrcidId_should_not_touch_garbage() {
        assertEquals("http://bla/001x2-34567X", reduceUriToOrcidId("http://bla/001x2-34567X"));
    }
}
