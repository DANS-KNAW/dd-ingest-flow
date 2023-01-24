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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DepositTest {

    @Test
    void getOtherDoiId_should_return_null_if_doi_is_null() {
        var deposit = new Deposit();
        deposit.setDataverseIdProtocol("protocol");
        deposit.setDataverseIdAuthority("authority");
        deposit.setDataverseId("id");

        assertNull(deposit.getOtherDoiId());
    }
    @Test
    void getOtherDoiId_should_return_null_if_doi_is_empty_string() {
        var deposit = new Deposit();
        deposit.setDataverseIdProtocol("protocol");
        deposit.setDataverseIdAuthority("authority");
        deposit.setDataverseId("id");
        deposit.setDoi(" ");

        assertNull(deposit.getOtherDoiId());
    }

    @Test
    void getOtherDoiId_should_return_null_if_doi_is_the_same() {
        var deposit = new Deposit();
        deposit.setDataverseIdProtocol("doi");
        deposit.setDataverseIdAuthority("a");
        deposit.setDataverseId("b");
        deposit.setDoi("a/b");

        assertNull(deposit.getOtherDoiId());
    }

    @Test
    void getOtherDoiId_should_return_doi_if_doi_is_different() {
        var deposit = new Deposit();
        deposit.setDataverseIdProtocol("doi");
        deposit.setDataverseIdAuthority("a");
        deposit.setDataverseId("c");
        deposit.setDoi("a/b");

        assertEquals("doi:a/c", deposit.getOtherDoiId());
    }
}