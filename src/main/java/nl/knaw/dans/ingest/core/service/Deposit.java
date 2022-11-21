package nl.knaw.dans.ingest.core.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import nl.knaw.dans.ingest.core.DepositState;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;

import java.nio.file.Path;
import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Deposit {

    private String id;
    private Path dir;
    private Path bagDir;

    private String doi;
    private String urn;

    private String filename;
    private String mimeType;
    private String packaging;
    private String depositorUserId;
    private String bagName;
    private String otherId;
    private String otherIdVersion;
    private OffsetDateTime created;
    private DepositState state;
    private String stateDescription;
    private String collectionId;
    private boolean update;

    private String dataverseIdProtocol;
    private String dataverseIdAuthority;
    private String dataverseId;
    private String dataverseBagId;
    private String dataverseNbn;
    private String dataverseOtherId;
    private String dataverseOtherIdVersion;
    private String dataverseSwordToken;

    private Document ddm;
    private Document filesXml;
    private Document amd;
    private Document agreements;

    public VaultMetadata getVaultMetadata() {
        return new VaultMetadata(getDataversePid(), getDataverseBagId(), getDataverseNbn(), getDataverseOtherId(), getOtherIdVersion(), getDataverseSwordToken());
    }

    public String getDataversePid() {
        return String.format("%s:%s/%s", dataverseIdProtocol, dataverseIdAuthority, dataverseId);
    }

    public String getOtherDoiId() {
        var result = String.format("doi:%s", doi);

        if (StringUtils.isNotBlank(getDataverseId()) && !StringUtils.equals(getDataversePid(), result)) {
            return result;
        }

        return null;
    }

    public Path getDdmPath() {
        return bagDir.resolve("metadata/dataset.xml");
    }

    public Path getFilesXmlPath() {
        return bagDir.resolve("metadata/files.xml");
    }

    public Path getAgreementsXmlPath() {
        return bagDir.resolve("metadata/depositor-info/agreements.xml");
    }

    public Path getAmdPath() {
        return bagDir.resolve("metadata/amd.xml");
    }
}
