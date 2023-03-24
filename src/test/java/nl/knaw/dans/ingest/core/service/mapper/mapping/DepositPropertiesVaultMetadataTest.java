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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DepositPropertiesVaultMetadataTest extends BaseTest {

    @Test
    void toOtherIdValue_should_create_OtherId_Json_object_for_other_ID_if_specified_in_correct_format() {
        var builder = new CompoundFieldBuilder("", true);
        DepositPropertiesVaultMetadata.toOtherIdValue.build(builder, "PAN:123");
        var field = (CompoundMultiValueField)builder.build();

        assertThat(field.getValue()).extracting(OTHER_ID_AGENCY).extracting("value")
            .containsOnly("PAN");

        assertThat(field.getValue()).extracting(OTHER_ID_VALUE).extracting("value")
            .containsOnly("123");
    }

    @Test
    void toOtherIdValue_should_throw_error_if_value_is_invalid() {
        var builder = new CompoundFieldBuilder("", true);

        assertThrows(IllegalArgumentException.class,
            () -> DepositPropertiesVaultMetadata.toOtherIdValue.build(builder, "in/valid"));
    }

    @Test
    void toOtherIdValue_should_throw_error_with_empty_values() {
        var builder = new CompoundFieldBuilder("", true);

        assertThrows(IllegalArgumentException.class,
            () -> DepositPropertiesVaultMetadata.toOtherIdValue.build(builder, ""));
        assertThrows(IllegalArgumentException.class,
            () -> DepositPropertiesVaultMetadata.toOtherIdValue.build(builder, " "));
        assertThrows(IllegalArgumentException.class,
            () -> DepositPropertiesVaultMetadata.toOtherIdValue.build(builder, null));
    }

    @Test
    void isValidOtherIdValue_should_return_false_for_invalid_other_ids() {
        assertFalse(DepositPropertiesVaultMetadata.isValidOtherIdValue(""));
        assertFalse(DepositPropertiesVaultMetadata.isValidOtherIdValue(" "));
        assertFalse(DepositPropertiesVaultMetadata.isValidOtherIdValue("   \n"));
        assertFalse(DepositPropertiesVaultMetadata.isValidOtherIdValue("some words"));
        assertFalse(DepositPropertiesVaultMetadata.isValidOtherIdValue("oneword"));
    }

    @Test
    void isValidOtherIdValue_should_return_true_for_valid_other_ids() {
        assertTrue(DepositPropertiesVaultMetadata.isValidOtherIdValue("x:y"));
        assertTrue(DepositPropertiesVaultMetadata.isValidOtherIdValue("  test:123 "));
    }
}