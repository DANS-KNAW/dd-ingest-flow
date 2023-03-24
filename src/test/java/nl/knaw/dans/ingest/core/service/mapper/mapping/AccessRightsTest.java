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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessRightsTest extends BaseTest {

    @Test
    void toDefaultRestrict_should_return_false_when_access_rights_is_OPEN_ACCESS() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  OPEN_ACCESS"
            + "</ddm:accessRights>\n");

        assertFalse(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void toDefaultRestrict_should_return_true_when_access_rights_is_OPEN_ACCESS_FOR_REGISTERED_USERS() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  OPEN_ACCESS_FOR_REGISTERED_USERS"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void toDefaultRestrict_should_return_true_when_access_rights_is_REQUEST_PERMISSION() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  REQUEST_PERMISSION"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void toDefaultRestrict_should_return_true_when_access_rights_is_NO_ACCESS() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void toDefaultRestrict_should_return_true_when_access_rights_is_something_else() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  SOMETHING"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void isEnableRequests_should_be_false_if_one_file_has_explicitly_accessibleTo_equals_NONE() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  OPEN_ACCESS"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "        <accessibleToRights>ANONYMOUS</accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <accessibleToRights>NONE</accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/sub/vacio.txt\">\n"
            + "        <accessibleToRights>NONE</accessibleToRights>\n"
            + "    </file>\n"
            + "</files>");

        assertFalse(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }

    @Test
    void isEnableRequests_should_be_false_if_one_file_has_implicitly_accessibleTo_equals_NONE() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <accessibleToRights>ANONYMOUS</accessibleToRights>\n"
            + "    </file>\n"
            + "</files>");

        assertFalse(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }

    @Test
    void isEnableRequests_should_be_false_if_accessRights_NO_ACCESS() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "        <accessibleToRights>RESTRICTED_REQUEST</accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <accessibleToRights>RESTRICTED_REQUEST</accessibleToRights>\n"
            + "    </file>\n"
            + "</files>");

        assertFalse(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }

    @Test
    void isEnableRequests_should_be_true_if_all_implicitly_and_explicitly_defined_accessibleTo_is_RESTRICTED_REQUEST_or_more_open() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "  REQUEST_PERMISSION"
            + "</ddm:accessRights>\n");

        var files = readDocumentFromString("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" \n"
            + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "    <file filepath=\"data/leeg.txt\">\n"
            + "        <accessibleToRights>RESTRICTED_REQUEST</accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg2.txt\">\n"
            + "        <accessibleToRights>RESTRICTED_REQUEST</accessibleToRights>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/sub/leeg3.txt\">\n"
            + "    </file>\n"
            + "</files>");

        assertTrue(AccessRights.isEnableRequests(doc.getDocumentElement(), files));
    }
}