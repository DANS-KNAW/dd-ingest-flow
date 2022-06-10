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
package nl.knaw.dans.easy.dd2d

import nl.knaw.dans.lib.scaladv.model.file.FileMeta

import java.nio.file.Paths

class DepositSpec extends TestSupportFixture {

  "checkDeposit" should "succeed if directory is deposit" in {
    Deposit(testDirValid / "valid-easy-submitted") shouldBe a[Deposit]
  }

  it should "fail if it is not a directory but a file" in {
    val file = testDirNonValid / "no-dir.txt"
    the[InvalidDepositException] thrownBy Deposit(file) should have message (s"Not a deposit: $file is not a directory")
  }

  it should "fail if it has no sub-directoires" in {
    val file = testDirNonValid / "no-subdir"
    the[InvalidDepositException] thrownBy Deposit(file) should have message (s"Not a deposit: $file has more or fewer than one subdirectory")
  }

  it should "fail if it has no deposit.properties" in {
    val file = testDirNonValid / "no-deposit-properties"
    the[InvalidDepositException] thrownBy Deposit(file) should have message (s"Not a deposit: $file does not contain a deposit.properties file")
  }

  it should "return the DOI value in deposit.properties" in {
    Deposit(testDirValid / "valid-easy-submitted").doi shouldBe "10.17026/dans-ztg-q3s4"
  }

  it should "return empty string when there is no identifier.doi defined in deposit.properties" in {
    Deposit(testDirValid / "valid-easy-submitted-no-doi").doi shouldBe ""
  }

  "getPathToFileInfo" should "correctly map local path to FileInfo object" in {
    val pathToFileInfo = Deposit(testDirValid / "valid-easy-submitted").getPathToFileInfo.get

    pathToFileInfo(Paths.get("data/README.md")) shouldBe FileInfo(
      file = testDirValid / "valid-easy-submitted" / "example-bag-medium" / "data" / "README.md",
      "f50380cd3a4ae5b8ea3d524a4b1e8582eca50893",
      metadata = FileMeta(
        label = Option("README.md"),
        directoryLabel = None,
        restrict = Option(true)
      )
    )
  }

  "getOptOtherDoiId" should "be other doi if identifier.doi different from dataversePid" in {
    val deposit = Deposit(testDirValid / "doi-and-other-doi")
    deposit.getOptOtherDoiId shouldBe Option("doi:10.17026/OTHER-DOI")
  }

  it should "be None if identifier.doi equals dataversePid" in {
    val deposit = Deposit(testDirValid / "doi-and-no-other-doi")
    deposit.getOptOtherDoiId shouldBe empty
  }

  it should "be None if dataversePid was not provided in vault metadata" in {
    val deposit = Deposit(testDirValid / "no-doi-in-vault-metadata")
    deposit.getOptOtherDoiId shouldBe empty
  }
}
