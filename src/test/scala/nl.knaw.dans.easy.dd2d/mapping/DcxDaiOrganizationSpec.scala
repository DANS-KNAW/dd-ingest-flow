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
import nl.knaw.dans.ingest.core.legacy.MetadataObjectMapper
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }

class DcxDaiOrganizationSpec extends TestSupportFixture with BlockCitation {
  private implicit val jsonFormats: Formats = DefaultFormats

  "toContributorValueObject" should "create correct contributor details in Json object" in {
    val organization =
      <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
          <dcx-dai:role xml:lang="en">DataCurator</dcx-dai:role>
      </dcx-dai:organization>
    val result = objectMapper.writeValueAsString(DcxDaiOrganization.toContributorValueObject(organization))
    debug(result)
    findString(result, s"$CONTRIBUTOR_NAME.value") shouldBe "Anti-Vampire League"
    findString(result, s"$CONTRIBUTOR_TYPE.value") shouldBe "Data Curator"
  }

  it should "give 'other' as contributor type" in {
    val organization =
      <dcx-dai:organization>
          <dcx-dai:role xml:lang="en">ContactPerson</dcx-dai:role>
      </dcx-dai:organization>
    val result = objectMapper.writeValueAsString(DcxDaiOrganization.toContributorValueObject(organization))
    debug(result)
    findString(result, s"$CONTRIBUTOR_TYPE.value") shouldBe "Other"
  }

  "toAuthorValueObject" should "use organization name as author name" in {
    val organization =
      <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
      </dcx-dai:organization>
    val result = objectMapper.writeValueAsString(DcxDaiOrganization.toAuthorValueObject(organization))
    debug(result)
    findString(result, s"$AUTHOR_NAME.value") shouldBe "Anti-Vampire League"
  }

  it should "use ISNI if present" in {
    val organization =
      <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
          <dcx-dai:ISNI>http://isni.org/isni/0000000121032683</dcx-dai:ISNI>
      </dcx-dai:organization>
    val result = objectMapper.writeValueAsString(DcxDaiOrganization.toAuthorValueObject(organization))
    debug(result)
    findString(result, s"$AUTHOR_NAME.value") shouldBe "Anti-Vampire League"
    findString(result, s"$AUTHOR_IDENTIFIER_SCHEME.value") shouldBe "ISNI"
    findString(result, s"$AUTHOR_IDENTIFIER.value") shouldBe "http://isni.org/isni/0000000121032683"
  }

  "toGrantNumberValueObject" should "create a grantnumber with only an organization subfield" in {
    val contributor =
      <dcx-dai:organization>
          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
          <dcx-dai:role>Funder</dcx-dai:role>
          <dcx-dai:ISNI>http://isni.org/isni/0000000121032683</dcx-dai:ISNI>
      </dcx-dai:organization>
    DcxDaiOrganization.inAnyOfRoles(List("Funder"))(contributor) shouldBe true

    val result = DcxDaiOrganization.toGrantNumberValueObject(contributor)
    val s = objectMapper.writeValueAsString(result)
    debug(s)
    findString(s, s"$GRANT_NUMBER_VALUE.value") shouldBe ""
    findString(s, s"$GRANT_NUMBER_AGENCY.value") shouldBe "Anti-Vampire League"
  }
}
