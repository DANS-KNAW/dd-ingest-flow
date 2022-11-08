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
package nl.knaw.dans.easy.dd2d.fieldbuilders

import nl.knaw.dans.easy.dd2d.mapping.FieldMap
import nl.knaw.dans.lib.dataverse.model.dataset.{ CompoundField, MetadataField, SingleValueField }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.collection.JavaConverters.{ mapAsJavaMapConverter, seqAsJavaListConverter }
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.{ immutable, mutable }

class CompoundFieldBuilder(name: String, multipleValues: Boolean = true) extends AbstractFieldBuilder with DebugEnhancedLogging{
  private val values = new mutable.ListBuffer[FieldMap]

  def addValue(v: FieldMap): Unit = {
    println(v)
    if (!multipleValues && values.nonEmpty) throw new IllegalArgumentException("Trying to add a second value to a single value field")
    values.append(v)
  }

  override def build(deduplicate: Boolean = false): Option[MetadataField] = {
    if (values.nonEmpty) Option {
      val stringToFields: immutable.Seq[FieldMap] = if (deduplicate) values.toList.distinct
                                                    else values.toList
      val valueList: java.util.List[java.util.Map[String, SingleValueField]] = stringToFields.map(convert).asJava
      new CompoundField(name, multipleValues, valueList)
    }
    else Option.empty
  }

  private def convert(fieldMap: FieldMap):java.util.Map[String, SingleValueField] = {
    fieldMap.mapValues(_.asInstanceOf[SingleValueField]).asJava
  }
}
