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
package nl.knaw.dans.ingest.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.dans.lib.util.ExecutorServiceFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class IngestFlowConfig {

    private List<String> skipFields;

    @NotNull
    @Valid
    @JsonProperty("import")
    private IngestAreaConfig importConfig;

    @NotNull
    @Valid
    private IngestAreaConfig migration;

    @NotNull
    @Valid
    private IngestAreaConfig autoIngest;

    @NotNull
    @Valid
    private Path zipWrappingTempDir;

    @NotNull
    @Valid
    private Path mappingDefsDir;

    @NotNull
    @Valid
    private String fileExclusionPattern;

    @NotNull
    @Valid
    private String depositorRole;

    @Valid
    @NotNull
    private DatasetAuthorizationConfig authorization;

    @Valid
    private boolean deduplicate;

    @NotNull
    @Valid
    private ExecutorServiceFactory taskQueue;

    private Map<String, String> iso1ToDataverseLanguage;
    private Map<String, String> iso2ToDataverseLanguage;
    private Map<String, String> reportIdToTerm;
    private List<String> spatialCoverageCountryTerms;

    @NotNull
    private Map<String, String> dataSuppliers;

    @NotNull
    @Valid
    private String vaultMetadataKey;

    public IngestAreaConfig getImportConfig() {
        return importConfig;
    }

    public void setImportConfig(IngestAreaConfig importConfig) {
        this.importConfig = importConfig;
    }

    public IngestAreaConfig getMigration() {
        return migration;
    }

    public void setMigration(IngestAreaConfig migration) {
        this.migration = migration;
    }

    public IngestAreaConfig getAutoIngest() {
        return autoIngest;
    }

    public void setAutoIngest(IngestAreaConfig autoIngest) {
        this.autoIngest = autoIngest;
    }

    public Path getZipWrappingTempDir() {
        return zipWrappingTempDir;
    }

    public void setZipWrappingTempDir(Path zipWrappingTempDir) {
        this.zipWrappingTempDir = zipWrappingTempDir;
    }

    public Path getMappingDefsDir() {
        return mappingDefsDir;
    }

    public void setMappingDefsDir(Path mappingDefsDir) {
        this.mappingDefsDir = mappingDefsDir;
    }

    public String getFileExclusionPattern() {
        return fileExclusionPattern;
    }

    public void setFileExclusionPattern(String fileExclusionPattern) {
        this.fileExclusionPattern = fileExclusionPattern;
    }

    public String getDepositorRole() {
        return depositorRole;
    }

    public void setDepositorRole(String depositorRole) {
        this.depositorRole = depositorRole;
    }

    public boolean isDeduplicate() {
        return deduplicate;
    }

    public void setDeduplicate(boolean deduplicate) {
        this.deduplicate = deduplicate;
    }

    public ExecutorServiceFactory getTaskQueue() {
        return taskQueue;
    }

    public void setTaskQueue(ExecutorServiceFactory taskQueue) {
        this.taskQueue = taskQueue;
    }

    public Map<String, String> getIso1ToDataverseLanguage() {
        return iso1ToDataverseLanguage;
    }

    public void setIso1ToDataverseLanguage(Map<String, String> iso1ToDataverseLanguage) {
        this.iso1ToDataverseLanguage = iso1ToDataverseLanguage;
    }

    public Map<String, String> getIso2ToDataverseLanguage() {
        return iso2ToDataverseLanguage;
    }

    public void setIso2ToDataverseLanguage(Map<String, String> iso2ToDataverseLanguage) {
        this.iso2ToDataverseLanguage = iso2ToDataverseLanguage;
    }

    public Map<String, String> getReportIdToTerm() {
        return reportIdToTerm;
    }

    public void setReportIdToTerm(Map<String, String> reportIdToTerm) {
        this.reportIdToTerm = reportIdToTerm;
    }

    public List<String> getSpatialCoverageCountryTerms() {
        return spatialCoverageCountryTerms;
    }

    public void setSpatialCoverageCountryTerms(List<String> spatialCoverageCountryTerms) {
        this.spatialCoverageCountryTerms = spatialCoverageCountryTerms;
    }

    public DatasetAuthorizationConfig getAuthorization() {
        return authorization;
    }

    public void setAuthorization(DatasetAuthorizationConfig authorization) {
        this.authorization = authorization;
    }

    public Map<String, String> getDataSuppliers() {
        return dataSuppliers;
    }

    public void setDataSuppliers(Map<String, String> dataSuppliers) {
        this.dataSuppliers = dataSuppliers == null ? Map.of() : dataSuppliers;
    }

    public List<String> getSkipFields() {
        return skipFields;
    }

    public void setSkipFields(List<String> skipFields) {
        this.skipFields = skipFields;
    }

    public String getVaultMetadataKey() {
        return vaultMetadataKey;
    }

    public void setVaultMetadataKey(String vaultMetadataKey) {
        this.vaultMetadataKey = vaultMetadataKey;
    }
}
