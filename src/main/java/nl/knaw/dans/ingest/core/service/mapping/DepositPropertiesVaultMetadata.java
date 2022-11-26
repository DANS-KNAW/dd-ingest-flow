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

import nl.knaw.dans.ingest.core.service.builder.CompoundFieldGenerator;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;

public class DepositPropertiesVaultMetadata extends Base {

    public static final CompoundFieldGenerator<String> toOtherIdValue = (builder, value) -> {
        var str = Optional.ofNullable(value).orElse("").trim();

        if (StringUtils.containsWhitespace(str)) {
            throw new IllegalArgumentException("Identifier must not contain whitespace");
        }

        var parts = str.split(":");

        if (parts.length != 2) {
            throw new IllegalArgumentException("Other ID value has invalid format. It should be '<prefix>:<suffix>'");
        }

        builder.addSubfield(OTHER_ID_AGENCY, parts[0]);
        builder.addSubfield(OTHER_ID_VALUE, parts[1]);
    };

    public static boolean isValidOtherIdValue(String value) {
        return StringUtils.isNotBlank(value) && value.split(":").length == 2;
    }
}
