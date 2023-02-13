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
package nl.knaw.dans.ingest.core.deposit;

import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.exception.InvalidDepositException;

import java.io.IOException;
import java.nio.file.Path;

public interface DepositWriter {

    void saveDeposit(Deposit deposit) throws InvalidDepositException;

    void moveDeposit(Deposit deposit, Path target) throws IOException;

    void moveDeposit(Path source, Path target) throws IOException;
}
