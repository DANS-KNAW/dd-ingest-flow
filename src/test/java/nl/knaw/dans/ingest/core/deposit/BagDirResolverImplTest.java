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

import nl.knaw.dans.ingest.core.io.FileService;
import nl.knaw.dans.ingest.core.exception.InvalidDepositException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BagDirResolverImplTest {
    @Test
    void getValidBagDir_should_return_path_if_everything_is_ok() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);
        var subdir = Path.of("subdir");

        Mockito.doReturn(true).when(fileService).fileExists(Mockito.any());
        Mockito.doReturn(true).when(fileService).isDirectory(Mockito.any());
        Mockito.doReturn(Stream.of(subdir)).when(fileService).listDirectories(Mockito.any());

        var result = resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c"));
        assertEquals(subdir, result);
    }

    @Test
    void getValidBagDir_should_throw_InvalidDepositException_if_directory_does_not_exist() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);
        var subdir = Path.of("subdir");

        Mockito.doReturn(true).when(fileService).fileExists(Mockito.any());
        // return false
        Mockito.doReturn(false).when(fileService).isDirectory(Mockito.any());
        Mockito.doReturn(Stream.of(subdir)).when(fileService).listDirectories(Mockito.any());

        var exception = assertThrows(InvalidDepositException.class,
            () -> resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c")));

        assertTrue(exception.getMessage().contains("is not a directory"));
    }

    @Test
    void getValidBagDir_should_throw_InvalidDepositException_if_multiple_subfolders_exist() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);
        var subdir = Path.of("subdir");
        var subdir2 = Path.of("subdir2");

        Mockito.doReturn(true).when(fileService).fileExists(Mockito.any());
        Mockito.doReturn(true).when(fileService).isDirectory(Mockito.any());
        // multiple subfolders
        Mockito.doReturn(Stream.of(subdir, subdir2)).when(fileService).listDirectories(Mockito.any());

        var exception = assertThrows(InvalidDepositException.class,
            () -> resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c")));

        assertTrue(exception.getMessage().contains("has more or fewer than one subdirectory"));
    }

    @Test
    void getValidBagDir_should_propagate_IOException_if_IOException_is_thrown_by_fileService() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);
        Mockito.doReturn(true).when(fileService).fileExists(Mockito.any());
        Mockito.doReturn(true).when(fileService).isDirectory(Mockito.any());

        // throw exception
        Mockito.doThrow(IOException.class).when(fileService).listDirectories(Mockito.any());

        assertThrows(IOException.class,
            () -> resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c")));
    }

    @Test
    void getValidBagDir_should_throw_InvalidDepositException_if_no_subfolders_exist() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);

        Mockito.doReturn(true).when(fileService).fileExists(Mockito.any());
        Mockito.doReturn(true).when(fileService).isDirectory(Mockito.any());
        // empty stream
        Mockito.doReturn(Stream.empty()).when(fileService).listDirectories(Mockito.any());

        var exception = assertThrows(InvalidDepositException.class,
            () -> resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c")));

        assertTrue(exception.getMessage().contains("has more or fewer than one subdirectory"));
    }

    @Test
    void getValidBagDir_should_throw_InvalidDepositException_if_deposit_properties_does_not_exist() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);
        var subdir = Path.of("subdir");

        Mockito.doReturn(false).when(fileService).fileExists(Mockito.eq(Path.of("deposit.properties")));
        Mockito.doReturn(true).when(fileService).isDirectory(Mockito.any());
        Mockito.doReturn(Stream.of(subdir)).when(fileService).listDirectories(Mockito.any());

        var exception = assertThrows(InvalidDepositException.class,
            () -> resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c")));

        assertTrue(exception.getMessage().contains("deposit.properties"));
    }

    @Test
    void getValidBagDir_should_throw_InvalidDepositException_if_not_a_bagit_folder() throws Throwable {
        var fileService = Mockito.mock(FileService.class);
        var resolver = new BagDirResolverImpl(fileService);
        var subdir = Path.of("subdir");

        Mockito.doReturn(true).when(fileService).fileExists(Mockito.any());
        Mockito.doReturn(false).when(fileService).fileExists(Mockito.eq(subdir.resolve("bagit.txt")));
        Mockito.doReturn(true).when(fileService).isDirectory(Mockito.any());
        Mockito.doReturn(Stream.of(subdir)).when(fileService).listDirectories(Mockito.any());

        var exception = assertThrows(InvalidDepositException.class,
            () -> resolver.getValidBagDir(Path.of("/path/to/deposit/a5378287-eab9-4dfd-885f-98d61c0a4c4c")));

        assertTrue(exception.getMessage().contains("does not contain a bag"));
    }
}