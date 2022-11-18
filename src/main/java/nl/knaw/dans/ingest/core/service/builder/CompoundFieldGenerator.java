package nl.knaw.dans.ingest.core.service.builder;

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;

@FunctionalInterface
public interface CompoundFieldGenerator<T> {

    void build(CompoundFieldBuilder builder, T value);
}
