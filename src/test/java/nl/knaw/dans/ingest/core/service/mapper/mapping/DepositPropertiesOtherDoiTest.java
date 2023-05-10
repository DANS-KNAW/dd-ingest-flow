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

class DepositPropertiesOtherDoiTest extends BaseTest {
    @Test
    void toOtherIdValue_should_correctly_split_value() {
        var builder = new CompoundFieldBuilder("", true);
        DepositPropertiesOtherDoi.toOtherIdValue.build(builder, "PREF:some:id:value");
        var field = builder.build();

        assertThat(((CompoundMultiValueField) field).getValue())
            .extracting(OTHER_ID_AGENCY)
            .extracting("value")
            .containsOnly("PREF");

        assertThat(((CompoundMultiValueField) field).getValue())
            .extracting(OTHER_ID_VALUE)
            .extracting("value")
            .containsOnly("some:id:value");
    }
}