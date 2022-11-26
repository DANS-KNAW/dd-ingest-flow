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
package nl.knaw.dans.ingest.core.service.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessRightsTest extends BaseTest {

    @Test
    void test_to_default_restrict_return_false_when_access_rights_is_open_access() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  OPEN_ACCESS"
            + "</ddm:accessRights>\n");

        assertFalse(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void test_to_default_restrict_return_false_when_access_rights_is_open_access_for_registered_users() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  OPEN_ACCESS_FOR_REGISTERED_USERS"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void test_to_default_restrict_return_false_when_access_rights_is_request_permission() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  REQUEST_PERMISSION"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void test_to_default_restrict_return_false_when_access_rights_is_no_access() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void test_to_default_restrict_return_false_when_access_rights_is_something_else() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  SOMETHING"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void test_is_enable_request_is_false_if_one_file_has_accessible_to_set_to_none() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  OPEN_ACCESS"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "        <ddm:accessibleToRights>ANONYMOUS</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <ddm:accessibleToRights>NONE</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/sub/vacio.txt\">\n"
            + "        <ddm:accessibleToRights>NONE</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "</files>");

        assertFalse(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }

    @Test
    void test_is_enable_request_is_false_if_one_file_has_implicit_accessible_to_is_none() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <ddm:accessibleToRights>ANONYMOUS</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "</files>");

        assertFalse(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }

    @Test
    void test_is_enable_request_is_true_if_all_files_explicitly_permission_request() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "        <ddm:accessibleToRights>RESTRICTED_REQUEST</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <ddm:accessibleToRights>RESTRICTED_REQUEST</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "</files>");

        assertTrue(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }

    @Test
    void test_is_enable_request_is_true_if_all_implicitly_and_explicitly_defined_accessible_to_is_restricted_request_or_more_open() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  REQUEST_PERMISSION"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "        <ddm:accessibleToRights>RESTRICTED_REQUEST</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <ddm:accessibleToRights>RESTRICTED_REQUEST</ddm:accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg3.txt\">\n"
            + "    </file>\n"
            + "</files>");

        assertTrue(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }
}