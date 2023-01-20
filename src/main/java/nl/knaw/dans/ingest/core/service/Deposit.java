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
package nl.knaw.dans.ingest.core.service;

import gov.loc.repository.bagit.creator.CreatePayloadManifestsVistor;
import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.hash.Hasher;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import gov.loc.repository.bagit.hash.SupportedAlgorithm;
import gov.loc.repository.bagit.util.PathUtils;
import gov.loc.repository.bagit.writer.ManifestWriter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import nl.knaw.dans.ingest.core.DepositState;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static gov.loc.repository.bagit.hash.StandardSupportedAlgorithms.SHA1;

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

    private String isVersionOf;

    private Instant bagCreated;

    private Document ddm;
    private Document filesXml;
    private Document amd;
    private Document agreements;

    private Bag bag;

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
        } else {
            return result;
        }
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
