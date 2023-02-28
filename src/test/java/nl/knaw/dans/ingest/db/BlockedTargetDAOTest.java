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
package nl.knaw.dans.ingest.db;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.ingest.core.BlockedTarget;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(DropwizardExtensionsSupport.class)
class BlockedTargetDAOTest {

    public DAOTestExtension database = DAOTestExtension
        .newBuilder()
        .addEntityClass(BlockedTarget.class)
        .build();

    private BlockedTargetDAO blockedTargetDAO;

    @BeforeEach
    void setUp() {
        blockedTargetDAO = new BlockedTargetDAO(database.getSessionFactory());
    }

    @Test
    void getTarget_should_return_empty_list_if_no_matches_exist() {
        assertEquals(0, blockedTargetDAO.getTarget("empty").size());
    }

    @Test
    void getTarget_should_return_items_if_matches_exist() {
        database.inTransaction(() -> {
            var blockedTarget1 = new BlockedTarget("depositid", "target1", "FAILED", "Failed reason");
            var blockedTarget2 = new BlockedTarget("depositid", "target2", "FAILED", "Failed reason");

            blockedTargetDAO.save(blockedTarget1);
            blockedTargetDAO.save(blockedTarget2);
        });

        assertEquals(1, blockedTargetDAO.getTarget("target1").size());
    }

    @Test
    void getTarget_should_return_no_items_if_records_exist_but_with_different_target() {
        database.inTransaction(() -> {
            var blockedTarget1 = new BlockedTarget("depositid", "target1", "FAILED", "Failed reason");
            var blockedTarget2 = new BlockedTarget("depositid", "target2", "FAILED", "Failed reason");

            blockedTargetDAO.save(blockedTarget1);
            blockedTargetDAO.save(blockedTarget2);
        });

        assertEquals(0, blockedTargetDAO.getTarget("target3").size());
    }
}