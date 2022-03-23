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

import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, Dataset, PrimitiveSingleValueField, toFieldMap }
import org.json4s.DefaultFormats

import scala.util.Success

class DepositToDataverseMapperSpec extends TestSupportFixture {

  implicit val format: DefaultFormats.type = DefaultFormats
  private val mapper = new DepositToDvDatasetMetadataMapper(deduplicate = false, List("citation", "dansDataVaultMetadata"), null, null, null, null)
  private val vaultMetadata = Deposit(testDirValid / "valid-easy-submitted").vaultMetadata
  private val optAgreements = Deposit(testDirValid / "valid-easy-submitted").tryOptAgreementsXml.get
  private val contactData = List(toFieldMap(
    PrimitiveSingleValueField("datasetContactName", "Contact Name"),
    PrimitiveSingleValueField("datasetContactEmail", "contact@example.org")
  ))

  "toDataverseDataset" should "map profile/title to citation/title" in {
    val ddm =
      <ddm:DDM>
        <ddm:profile>
           <dc:title>A title</dc:title>
           <ddm:audience>D10000</ddm:audience>
        </ddm:profile>
        <ddm:dcmiMetadata>
           <dct:rightsHolder>Mr Rights</dct:rightsHolder>
        </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, None, optAgreements, None, contactData, vaultMetadata)
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(Dataset(dsv)) =>
        dsv.metadataBlocks("citation").fields should contain(
          PrimitiveSingleValueField("title", "A title")
        )
    }
  }

  it should "map profile/descriptions to citation/descriptions" in {
    val ddm =
      <ddm:DDM>
        <ddm:profile>
           <dc:title>A title</dc:title>
           <dc:description>Descr 1</dc:description>
           <dc:description>Descr 2</dc:description>
          <ddm:audience>D10000</ddm:audience>
        </ddm:profile>
        <ddm:dcmiMetadata>
            <dct:rightsHolder>Mrs Rights</dct:rightsHolder>
        </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, None, optAgreements, None, contactData, vaultMetadata)
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(Dataset(dsv)) =>
        dsv.metadataBlocks("citation").fields should contain(
          CompoundField("dsDescription",
            List(
              Map("dsDescriptionValue" -> PrimitiveSingleValueField("dsDescriptionValue", "<p>Descr 1</p>")),
              Map("dsDescriptionValue" -> PrimitiveSingleValueField("dsDescriptionValue", "<p>Descr 2</p>"))
            )))
    }
  }

  it should "map profile/creatorDetails to citation/author" in {
    val ddm =
      <ddm:DDM>
          <ddm:profile>
              <dc:title>A title</dc:title>
              <dcx-dai:creatorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Dr</dcx-dai:titles>
                      <dcx-dai:initials>A</dcx-dai:initials>
                      <dcx-dai:insertions>van</dcx-dai:insertions>
                      <dcx-dai:surname>Helsing</dcx-dai:surname>
                      <dcx-dai:organization>
                          <dcx-dai:name xml:lang="en">Anti-Vampire League</dcx-dai:name>
                      </dcx-dai:organization>
                  </dcx-dai:author>
              </dcx-dai:creatorDetails>
              <dcx-dai:creatorDetails>
                  <dcx-dai:author>
                      <dcx-dai:titles>Professor</dcx-dai:titles>
                      <dcx-dai:initials>T</dcx-dai:initials>
                      <dcx-dai:insertions></dcx-dai:insertions>
                      <dcx-dai:surname>Zonnebloem</dcx-dai:surname>
                      <dcx-dai:organization>
                          <dcx-dai:name xml:lang="en">Uitvindersgilde</dcx-dai:name>
                      </dcx-dai:organization>
                  </dcx-dai:author>
              </dcx-dai:creatorDetails>
            <ddm:audience>D10000</ddm:audience>
          </ddm:profile>
          <ddm:dcmiMetadata>
            <dct:rightsHolder>Mrs Rights</dct:rightsHolder>
          </ddm:dcmiMetadata>
      </ddm:DDM>

    val result = mapper.toDataverseDataset(ddm, None, optAgreements, None, contactData, vaultMetadata)
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(Dataset(dsv)) =>
        val valueObjectsOfCompoundFields = dsv.metadataBlocks("citation").fields.filter(_.isInstanceOf[CompoundField]).map(_.asInstanceOf[CompoundField]).flatMap(_.value)
        valueObjectsOfCompoundFields should contain(
          Map(
            "authorName" -> PrimitiveSingleValueField("authorName", "A van Helsing"),
            "authorAffiliation" -> PrimitiveSingleValueField("authorAffiliation", "Anti-Vampire League")
          ))
        valueObjectsOfCompoundFields should contain(
          Map(
            "authorName" -> PrimitiveSingleValueField("authorName", "T Zonnebloem"),
            "authorAffiliation" -> PrimitiveSingleValueField("authorAffiliation", "Uitvindersgilde")
          ))
    }
  }

  it should "map other DOI to Other ID" in {
    val otherDoi = "10.123/other-doi"
    val ddm = <ddm:DDM>
      <ddm:profile>
          <dc:title>A title</dc:title>
          <dc:description>Descr 1</dc:description>
          <dc:description>Descr 2</dc:description>
          <ddm:audience>D10000</ddm:audience>
        </ddm:profile>
        <ddm:dcmiMetadata>
          <dct:rightsHolder>Mrs Rights</dct:rightsHolder>
        </ddm:dcmiMetadata>
      </ddm:DDM>
    val result = mapper.toDataverseDataset(ddm, Option(otherDoi), optAgreements, None, contactData, vaultMetadata)
    result shouldBe a[Success[_]]
    inside(result) {
      case Success(Dataset(dsv)) =>
        val valueObjectsOfCompoundFields = dsv.metadataBlocks("citation").fields.filter(_.isInstanceOf[CompoundField]).map(_.asInstanceOf[CompoundField]).flatMap(_.value)
        valueObjectsOfCompoundFields should contain(
          Map(
            "otherIdAgency" -> PrimitiveSingleValueField("otherIdAgency", ""),
            "otherIdValue" -> PrimitiveSingleValueField("otherIdValue", otherDoi)
          ))
    }
  }
}