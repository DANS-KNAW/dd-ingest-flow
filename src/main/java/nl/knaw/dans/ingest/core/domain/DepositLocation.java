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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.file.Path;
import java.time.OffsetDateTime;

/**
 * This class represents the location of a deposit that was done in one of the inboxes. It is intended to be a lightweight pointer to the deposit to be used for enqueuing a large number of deposits
 * without incurring the overhead of loading all the deposit metadata into memory.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DepositLocation {
    private Path dir;
    private String target;
    private String depositId;
    private OffsetDateTime created;
}
