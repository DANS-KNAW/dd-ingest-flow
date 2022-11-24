package nl.knaw.dans.ingest.core.service.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessRightsTest extends BaseTest {

    @Test
    void testToDefaultRestrictReturnFalseWhenAccessRightsIsOpenAccess() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  OPEN_ACCESS"
            + "</ddm:accessRights>\n");

        assertFalse(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void testToDefaultRestrictReturnFalseWhenAccessRightsIsOpenAccessForRegisteredUsers() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  OPEN_ACCESS_FOR_REGISTERED_USERS"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void testToDefaultRestrictReturnFalseWhenAccessRightsIsRequestPermission() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  REQUEST_PERMISSION"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void testToDefaultRestrictReturnFalseWhenAccessRightsIsNoAccess() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  NO_ACCESS"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void testToDefaultRestrictReturnFalseWhenAccessRightsIsSomethingElse() throws Exception {
        var doc = readDocumentFromString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<ddm:accessRights xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "  SOMETHING"
            + "</ddm:accessRights>\n");

        assertTrue(AccessRights.toDefaultRestrict(doc.getDocumentElement()));
    }

    @Test
    void testIsEnableRequestIsFalseIfOneFileHasAccessibleToSetToNone() throws Exception {
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
    void testIsEnableRequestIsFalseIfOneFileHasImplicitAccessibleToIsNone() throws Exception {
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
    void testIsEnableRequestIsTrueIfAllFilesExplicitlyPermissionRequest() throws Exception {
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
    void testIsEnableRequestIsTrueIfAllImplicitlyAndExplicitlyDefinedAccessibleToIsRestrictedRequestOrMoreOpen() throws Exception {
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