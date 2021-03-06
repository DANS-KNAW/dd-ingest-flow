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

public class IngestFlowConfig {
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
    private boolean deduplicate;

    @NotNull
    @Valid
    private ExecutorServiceFactory taskQueue;

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
}
