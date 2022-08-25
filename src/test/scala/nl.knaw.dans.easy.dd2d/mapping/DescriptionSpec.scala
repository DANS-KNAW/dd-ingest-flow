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

class DescriptionSpec extends TestSupportFixture with BlockCitation {
  private implicit val jsonFormats: Formats = new DefaultFormats {}

  "toDescriptionValueObject" should "create Json object for the description value" in {
    val description = <dcterms:description>The poise of the head strikes me at once...</dcterms:description>
    val result = objectMapper.writeValueAsString(Description.toDescriptionValueObject(description))
    findObject(result, s"$DESCRIPTION_VALUE") shouldBe Map("typeName" -> "dsDescriptionValue", "multiple" -> false, "typeClass" -> "primitive", "value" -> "<p>The poise of the head strikes me at once...</p>")
  }

  "toDescriptionValueObject" should "create Json object with correct html elements added for the description value" in {
    val description = <dcterms:description>This is the first paragraph
      with a newline.

      This is the second paragraph</dcterms:description>

    val result = objectMapper.writeValueAsString(Description.toDescriptionValueObject(description))
    findObject(result, s"$DESCRIPTION_VALUE") shouldBe Map("typeName" -> "dsDescriptionValue", "multiple" -> false, "typeClass" -> "primitive", "value" -> "<p>This is the first paragraph<br>      with a newline.</p><p>      This is the second paragraph</p>")
  }
}
