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

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HasOrganizationalIdentifierTest extends BaseTest {

    @Test
    void isValidOtherIdValue_should_return_true_for_valid_value() {
        assertTrue(HasOrganizationalIdentifier.isValidOtherIdValue("1234:test"));
        assertTrue(HasOrganizationalIdentifier.isValidOtherIdValue("1234:test:second"));
    }

    @Test
    void isValidOtherIdValue_should_return_false_for_invalid_values() {
        assertFalse(HasOrganizationalIdentifier.isValidOtherIdValue(":test"));
        assertFalse(HasOrganizationalIdentifier.isValidOtherIdValue("test:"));
        assertFalse(HasOrganizationalIdentifier.isValidOtherIdValue("no colon"));
    }

    @Test
    void toOtherIdValue_should_correctly_split_value() {
        var builder = new CompoundFieldBuilder("", true);
        HasOrganizationalIdentifier.toOtherIdValue.build(builder, "test:123");
        var field = builder.build();

        assertThat(((CompoundMultiValueField) field).getValue())
            .extracting(OTHER_ID_VALUE)
            .extracting("value")
            .containsOnly("123");

        assertThat(((CompoundMultiValueField) field).getValue())
            .extracting(OTHER_ID_AGENCY)
            .extracting("value")
            .containsOnly("test");
    }
}