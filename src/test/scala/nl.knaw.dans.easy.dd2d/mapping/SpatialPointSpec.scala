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

import scala.util.{ Failure, Try }

class SpatialPointSpec extends TestSupportFixture with BlockTemporalAndSpatial with Spatial {
  private implicit val jsonFormats: Formats = DefaultFormats

  "toEasyTsmSpatialPointValueObject" should "create correct spatial point details in Json object" in {
    val spatialPoint =
      <spatial>
        <Point>52.08113 4.34510</Point>
      </spatial>
    val result = objectMapper.writeValueAsString(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint))
    findString(result, s"$SPATIAL_POINT_SCHEME.value") shouldBe LONLAT_SCHEME
    findString(result, s"$SPATIAL_POINT_X.value") shouldBe "4.34510"
    findString(result, s"$SPATIAL_POINT_Y.value") shouldBe "52.08113"
  }

  it should "give 'RD (in m.)' as spatial point scheme and coordinates as integers" in {
    val spatialPoint =
      <spatial srsName={RD_SRS_NAME}>
        <Point>469470 209942</Point>
      </spatial>
    val result = objectMapper.writeValueAsString(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint))
    findString(result, s"$SPATIAL_POINT_SCHEME.value") shouldBe RD_SCHEME
    findString(result, s"$SPATIAL_POINT_X.value") shouldBe "469470"
    findString(result, s"$SPATIAL_POINT_Y.value") shouldBe "209942"
  }

  it should "throw exception when spatial point coordinates are given incorrectly" in {
    val spatialPoint =
      <spatial srsName={RD_SRS_NAME}>
        <Point>52.08113, 4.34510</Point>
      </spatial>
    inside(Try(SpatialPoint.toEasyTsmSpatialPointValueObject(spatialPoint))) {
      case Failure(e: NumberFormatException) => e.getMessage should include("52.08113,")
    }
  }
}
