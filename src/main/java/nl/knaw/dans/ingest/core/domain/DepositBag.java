package nl.knaw.dans.ingest.core.domain;

import gov.loc.repository.bagit.domain.Bag;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.w3c.dom.Document;

import java.nio.file.Path;

@Getter
@ToString
public class DepositBag {
    private Path bagDir;
    private Document ddm;
    private Document filesXml;
    private Document amd;
    private Document agreements;

    private Bag bag;
    @Setter
    private String dataStationUserAccount;
}
