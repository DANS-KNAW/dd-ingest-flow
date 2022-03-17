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
package nl.knaw.dans.ingest.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;

public class StartImport {
    private Path batch;
    private boolean continuePrevious = false;

    public Path getBatch() {
        return batch;
    }

    public void setBatch(Path batch) {
        this.batch = batch;
    }

    @JsonProperty("continue")
    public boolean isContinue() {
        return continuePrevious;
    }

    @JsonProperty("continue")
    public void setContinue(boolean continuePrevious) {
        this.continuePrevious = continuePrevious;
    }

    @Override
    public String toString() {
        return "StartImport{" +
            "batch=" + batch +
            ", continuePrevious=" + continuePrevious +
            '}';
    }
}
