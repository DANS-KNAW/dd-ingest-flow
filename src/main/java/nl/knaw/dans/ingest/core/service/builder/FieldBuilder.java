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

    private CompoundFieldBuilder getBuilder(String name, boolean multiple) {
        var result = Optional.ofNullable(fields.get(name))
            .map(CompoundFieldBuilder::nextValue)
            .orElse(new CompoundFieldBuilder(name, multiple));

        fields.put(name, result);
        return result;
    }

    public void addSingleString(String name, String data) {
        getBuilder(name, false).addSubfield(name, data);
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

    public void addMultipleStrings(String name, Stream<String> data) {
        data.forEach(value -> {
            var builder = getBuilder(name, true);
            builder.addSubfield(name, value);
        });
    }

    public void addSingle(String name, Node data, CompoundFieldGenerator<Node> generator) {
        generator.build(getBuilder(name, false), data);
    }

    public void addMultiple(String name, Stream<Node> data, CompoundFieldGenerator<Node> generator) {
        data.forEach(value -> {
            var builder = getBuilder(name, true);
            generator.build(builder, value);
        });
    }

}
