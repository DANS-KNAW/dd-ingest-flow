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
package nl.knaw.dans.ingest.core.service.builder;

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class FieldBuilder {
    private final Map<String, CompoundFieldBuilder> fields;

    public FieldBuilder() {
        this.fields = new HashMap<>();
    }

    public Map<String, CompoundFieldBuilder> getFields() {
        return fields;
    }

    CompoundFieldBuilder getBuilder(String name, boolean multiple) {
        var result = Optional.ofNullable(fields.get(name))
            .map(CompoundFieldBuilder::nextValue)
            .orElse(new CompoundFieldBuilder(name, multiple));

        fields.put(name, result);
        return result;
    }

    public void addSingleString(String name, Stream<String> data) {
        data
            .filter(Objects::nonNull)
            .filter(StringUtils::isNotBlank)
            .findFirst().ifPresent(value -> getBuilder(name, false).addSubfield(name, value));

    }

    public void addMultipleControlledFields(String name, Stream<String> data) {
        data
            .filter(Objects::nonNull)
            .filter(StringUtils::isNotBlank)
            .forEach(value -> {
                getBuilder(name, true).addControlledSubfield(name, value);
            });

    }

    public void addSingleControlledField(String name, Stream<String> data) {
        data
            .filter(Objects::nonNull)
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .ifPresent(value -> {
                getBuilder(name, true).addControlledSubfield(name, value);
            });

    }

    public void addMultiplePrimitivesString(String name, Stream<String> data) {
        data.forEach(value -> {
            var builder = getBuilder(name, true);
            builder.addSubfield(name, value);
        });
    }

    public void addMultiple(String name, Stream<Node> data, CompoundFieldGenerator<Node> generator) {
        data.forEach(value -> {
            var builder = getBuilder(name, true);
            generator.build(builder, value);
        });
    }

    public void addMultipleString(String name, Stream<String> data, CompoundFieldGenerator<String> generator) {
        data.forEach(value -> {
            var builder = getBuilder(name, true);
            generator.build(builder, value);
        });
    }
}
