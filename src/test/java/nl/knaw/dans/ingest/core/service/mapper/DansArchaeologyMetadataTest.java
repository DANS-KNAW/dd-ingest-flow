package nl.knaw.dans.ingest.core.service.mapper;

import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveMultipleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static org.assertj.core.api.Assertions.assertThat;

public class DansArchaeologyMetadataTest {

    // AR001 + AR002 see IdentifierTest

    @Test
    void AR003_AR004_abr_report_type_and_number() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>"
            + "        <dct:rightsHolder>M.A.N. Datory</dct:rightsHolder>"
            + "        <!-- AR003 and AR004 -->"
            + "        <ddm:reportNumber"
            + "                subjectScheme='ABR Rapporten'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/d6b2e162-3f49-4027-8f03-28194db2905e'"
            + "                reportNo='123-A'>"
            + "            BAAC 123-A"
            + "        </ddm:reportNumber>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true, true);

        // AR003
        assertThat(getPrimitiveMultipleValueField("dansArchaeologyMetadata", "dansAbrRapportType", result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/d6b2e162-3f49-4027-8f03-28194db2905e");

        // AR004
        assertThat(getPrimitiveMultipleValueField("dansArchaeologyMetadata", "dansAbrRapportNummer", result))
            .containsOnly("BAAC 123-A");
    }

    @Test
    void AR005_aquisition_method() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>"
            + "        <dct:rightsHolder>M.A.N. Datory</dct:rightsHolder>"
            + "        <ddm:acquisitionMethod"
            + "                subjectScheme='ABR verwervingswijzen'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/554ca1ec-3ed8-42d3-ae4b-47bcb848b238'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/967bfdf8-c44d-4c69-8318-34ed1ab1e784'>"
            + "            archeologisch: onderwaterarcheologie"
            + "        </ddm:acquisitionMethod>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true, true);

        assertThat(getPrimitiveMultipleValueField("dansArchaeologyMetadata", "dansAbrVerwervingswijze", result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/967bfdf8-c44d-4c69-8318-34ed1ab1e784");
    }

    @Test
    void AR006_abr_complex() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>"
            + "        <dct:rightsHolder>M.A.N. Datory</dct:rightsHolder>"
            + "        <ddm:subject"
            + "                subjectScheme='ABR Complextypen'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/9a758542-8d0d-4afa-b664-104b938fe13e'>"
            + "            houtwinning"
            + "        </ddm:subject>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true, true);

        assertThat(getPrimitiveMultipleValueField("dansArchaeologyMetadata", "dansAbrComplex", result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/9a758542-8d0d-4afa-b664-104b938fe13e");
    }

    @Test
    void AR007_abr_artifact() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>"
            + "        <dct:rightsHolder>M.A.N. Datory</dct:rightsHolder>"
            + "        <ddm:subject"
            + "                subjectScheme='ABR Artefacten'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/22cbb070-6542-48f0-8afe-7d98d398cc0b'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/5bd97bc0-697c-4128-b7b2-d2324bc4a2e1'>"
            + "            rammelaar"
            + "        </ddm:subject>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true, true);

        // AR001
        assertThat(getPrimitiveMultipleValueField("dansArchaeologyMetadata", "dansAbrArtifact", result))
            .containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/5bd97bc0-697c-4128-b7b2-d2324bc4a2e1");
    }

    @Test
    void AR008_abr_period() throws Exception {

        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>"
            + "        <dct:rightsHolder>M.A.N. Datory</dct:rightsHolder>"
            + "        <ddm:temporal"
            + "                subjectScheme='ABR Periodes'"
            + "                schemeURI='https://data.cultureelerfgoed.nl/term/id/abr/9b688754-1315-484b-9c89-8817e87c1e84'"
            + "                valueURI='https://data.cultureelerfgoed.nl/term/id/abr/5b253754-ddd0-4ae0-a5bb-555176bca858'>"
            + "            Midden Romeinse Tijd A"
            + "        </ddm:temporal>"
            + "    </ddm:dcmiMetadata>"
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true, true);

        var field = getPrimitiveMultipleValueField("dansArchaeologyMetadata", "dansAbrPeriod", result);
        assertThat(field).containsOnly("https://data.cultureelerfgoed.nl/term/id/abr/5b253754-ddd0-4ae0-a5bb-555176bca858");
    }

}
