package nl.knaw.dans.ingest.core.service.builder;

import org.w3c.dom.Node;

import java.util.function.Function;
import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.LANGUAGE_OF_METADATA;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PERSONAL_DATA_PRESENT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RIGHTS_HOLDER;

public class RightsFieldBuilder extends FieldBuilder {

    public void addRightsHolders(Stream<String> nodes) {
        addMultipleStrings(RIGHTS_HOLDER, nodes);
    }

    public void addPersonalDataPresent(Stream<String> values) {
        addSingleControlledField(PERSONAL_DATA_PRESENT, values);
    }

    public void addLanguageOfMetadata(Stream<String> stream) {
        addMultipleControlledFields(LANGUAGE_OF_METADATA, stream);
    }
}
