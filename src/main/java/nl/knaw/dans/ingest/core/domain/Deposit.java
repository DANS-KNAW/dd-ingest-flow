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
package nl.knaw.dans.ingest.core.domain;

import gov.loc.repository.bagit.domain.Bag;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Deposit {

    private Path dir;
    private Path bagDir;

    private String doi;
    private String urn;
    private String dataverseDoi;
    private String depositorUserId;
    private String otherId;
    private String otherIdVersion;
    private OffsetDateTime created;
    private DepositState state;
    private String stateDescription;
    private boolean update;

    private String dataverseIdProtocol;
    private String dataverseIdAuthority;
    private String dataverseId;
    private String dataverseBagId;
    private String dataverseNbn;
    private String dataverseOtherId;
    private String dataverseOtherIdVersion;
    private String dataverseSwordToken;
    private String hasOrganizationalIdentifier;

    private String isVersionOf;

    private Instant bagCreated;

    private Document ddm;
    private Document filesXml;
    private Document amd;
    private Bag bag;

    private List<DepositFile> files;

    public VaultMetadata getVaultMetadata() {
        return new VaultMetadata(getDataversePid(), getDataverseBagId(), getDataverseNbn(), getDataverseOtherId(), getOtherIdVersion(), getDataverseSwordToken());
    }

    public String getDataversePid() {
        return String.format("%s:%s/%s", dataverseIdProtocol, dataverseIdAuthority, dataverseId);
    }

    public String getOtherDoiId() {
        // prevent "doi:null" values
        if (StringUtils.isBlank(doi)) {
            return null;
        }

        var result = getDataversePid();

        if (StringUtils.equals(String.format("doi:%s", doi), result)) {
            return null;
        }
        else {
            return result;
        }
    }

    /**
     * Are access requests allowed according to this deposit. Note that if this is an update-deposit, the result from this method should be combined with the current setting. If the dataset has access
     * requests disabled this overrules a <code>true</code> value returned from this method.
     *
     * @return whether this deposit allows access requests
     */
    public boolean allowAccessRequests() {
        var accessRightsNode = XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/ddm:accessRights")
            .findFirst()
            .orElseThrow();

        var isNoAccessDataset = "NO_ACCESS".equals(accessRightsNode.getTextContent().trim());
        var accessibleToNoneFilesPresent = XPathEvaluator
            .strings(filesXml, "/files:files/files:file/files:accessibleToRights")
            .map(String::trim)
            .anyMatch("NONE"::equals);

        return !(isNoAccessDataset || accessibleToNoneFilesPresent);
    }

    public boolean restrictedFilesPresent() {
        var numberOfFiles = files.size();
        var explicitAccessibleToValues = XPathEvaluator
            .strings(filesXml, "/files:files/files:file/files:accessibleToRights")
            .map(String::trim).collect(Collectors.toList());
        var explicitlyRestrictedFilesPresent = explicitAccessibleToValues.stream()
            .anyMatch(a -> !"ANONYMOUS".equals(a));
        var accessRights = XPathEvaluator.nodes(ddm, "/ddm:DDM/ddm:profile/ddm:accessRights")
            .findFirst()
            .orElseThrow()
            .getTextContent().trim();
        var implicitFilesAreRestricted = !"OPEN_ACCESS".equals(accessRights);
        var implicitFilesPresent = numberOfFiles > explicitAccessibleToValues.size();
        return explicitlyRestrictedFilesPresent || (implicitFilesPresent && implicitFilesAreRestricted);
    }

    public String getDepositId() {
        return this.dir.getFileName().toString();
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
