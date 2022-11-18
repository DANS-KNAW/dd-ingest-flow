package nl.knaw.dans.ingest.core.service.builder;

import org.w3c.dom.Node;

import java.util.stream.Stream;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_ARTIFACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_COMPLEX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_PERIOD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_RAPPORT_NUMMER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_RAPPORT_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ABR_VERWERVINGSWIJZE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_ZAAK_ID;

public class ArchaeologyFieldBuilder extends FieldBuilder {

    public void addArchisZaakId(Stream<String> nodes) {
        addMultipleStrings(ARCHIS_ZAAK_ID, nodes);
    }

    public void addArchisNumber(Stream<Node> stream, CompoundFieldGenerator<Node> generator) {
        addMultiple(ARCHIS_NUMBER, stream, generator);
    }

    public void addRapportType(Stream<String> nodes) {
        addMultipleStrings(ABR_RAPPORT_TYPE, nodes);
    }

    public void addRapportNummer(Stream<String> nodes) {
        addMultipleStrings(ABR_RAPPORT_NUMMER, nodes);
    }

    public void addVerwervingswijze(Stream<String> nodes) {
        addMultipleStrings(ABR_VERWERVINGSWIJZE, nodes);
    }

    public void addComplex(Stream<String> nodes) {
        addMultipleStrings(ABR_COMPLEX, nodes);
    }

    public void addArtifact(Stream<String> nodes) {
        addMultipleStrings(ABR_ARTIFACT, nodes);
    }

    public void addPeriod(Stream<String> nodes) {
        addMultipleStrings(ABR_PERIOD, nodes);
    }

}
