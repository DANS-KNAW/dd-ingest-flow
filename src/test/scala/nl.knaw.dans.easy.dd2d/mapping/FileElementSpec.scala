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
package nl.knaw.dans.easy.dd2d.mapping

import nl.knaw.dans.easy.dd2d.TestSupportFixture
import nl.knaw.dans.lib.scaladv.model.file.FileMeta

class FileElementSpec extends TestSupportFixture {
  "toFileMetadata" should "strip data/ prefix from path to get directoryLabel" in {
    val xml =
      <file filepath="data/this/is/the/directoryLabel/filename.txt">
      </file>
    val result = FileElement.toFileMeta(xml, defaultRestrict = true)
    result shouldBe FileMeta(
      directoryLabel = Option("this/is/the/directoryLabel"),
      label = Option("filename.txt"),
      restrict = Option(true)
    )
  }

  it should "represent key-value pairs in the description, for keys on the fixed keys list" in {
    val xml =
      <file filepath="data/test.txt">
        <othmat_codebook>the code book</othmat_codebook>
      </file>
    val result = FileElement.toFileMeta(xml, defaultRestrict = true)
    result shouldBe FileMeta(
      directoryLabel = None,
      label = Option("test.txt"),
      description = Option("""othmat_codebook: "the code book""""),
      restrict = Option(true))
  }

  it should "include original_filepath if directoryLabel or label change during sanitation" in {
    val xml =
      <file filepath="data/directory/path/with/&lt;for'bidden&gt;/(chars)/strange?filename*.txt">
      </file>
    val result = FileElement.toFileMeta(xml, defaultRestrict = true)
    result shouldBe FileMeta(
      directoryLabel = Option("directory/path/with/_for_bidden_/_chars_"),
      label = Option("strange_filename_.txt"),
      description = Option("""original_filepath: "directory/path/with/<for'bidden>/(chars)/strange?filename*.txt""""),
      restrict = Option(true))
  }

  it should "*not* include original_filepath if directoryLabel or label stay unchanged during sanitation" in {
    val xml =
      <file filepath="data/directory/path/with/all/legal/chars/normal_filename.txt">
      </file>
    val result = FileElement.toFileMeta(xml, defaultRestrict = true)
    result shouldBe FileMeta(
      directoryLabel = Option("directory/path/with/all/legal/chars"),
      label = Option("normal_filename.txt"),
      description = None,
      restrict = Option(true))
  }

  it should "only replace non-ASCII chars in directory names during sanitization" in {
    val originalFilePath = "data/directory/path/with/all/leg\u00e5l/chars/n\u00f8rmal_filename.txt"
    val xml =
      <file filepath={originalFilePath}>
      </file>
    val result = FileElement.toFileMeta(xml, defaultRestrict = true)
    result shouldBe FileMeta(
      directoryLabel = Option("directory/path/with/all/leg_l/chars"),
      label = Option("n\u00f8rmal_filename.txt"),
      description = Option(s"""original_filepath: "directory/path/with/all/leg\u00e5l/chars/n\u00f8rmal_filename.txt""""),
      restrict = Option(true))
  }
}
