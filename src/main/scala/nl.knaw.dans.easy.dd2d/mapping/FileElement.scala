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

import nl.knaw.dans.lib.scaladv.model.file.FileMeta

import java.nio.file.Paths
import scala.collection.mutable
import scala.language.postfixOps
import scala.xml.Node

object FileElement {
  private val forbiddenCharactersInFileName = List(':', '*', '?', '"', '<', '>', '|', ';', '#')
  private val allowedCharactersInDirectoryLabel = List('_', '-', '.', '\\', '/', ' ') ++ ('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')

  private val accessibilityToRestrict = Map(
    "KNOWN" -> true,
    "NONE" -> true,
    "RESTRICTED_REQUEST" -> true,
    "ANONYMOUS" -> false
  )

  def toFileMeta(node: Node, defaultRestrict: Boolean): FileMeta = {

    val pathAttr = node.attribute("filepath").flatMap(_.headOption).getOrElse { throw new RuntimeException("File node without a filepath attribute") }.text
    if (!pathAttr.startsWith("data/")) throw new RuntimeException(s"file outside data folder: $pathAttr")
    val pathInDataverseDataset = Paths.get(pathAttr.substring("data/".length))
    val fileName = Option(pathInDataverseDataset.getFileName).map(_.toString)
    val sanitizedFileName = fileName.map(replaceForbiddenCharactersInFileName)
    val dirPath = Option(pathInDataverseDataset.getParent).map(_.toString)
    val sanitizedDirectoryLabel = dirPath.map(replaceForbiddenCharactersInPath)
    val restr = (node \ "accessibleToRights").headOption.map(_.text).flatMap(accessibilityToRestrict.get).orElse(Some(defaultRestrict))
    val keyValuePairs = getKeyValuePairs(node, fileName.get, if (fileName != sanitizedFileName || dirPath != sanitizedDirectoryLabel) Option(pathInDataverseDataset.toString) else None)
    val descr = if (keyValuePairs.nonEmpty) {
      if (keyValuePairs.size == 1 && keyValuePairs.keySet.contains("description")) Option(keyValuePairs("description").head)
      else Option(formatKeyValuePairs(keyValuePairs))
    }
                else None

    FileMeta(
      label = sanitizedFileName,
      directoryLabel = sanitizedDirectoryLabel,
      description = descr,
      restrict = restr,
    )
  }

  private def replaceForbiddenCharactersInPath(s: String): String = {
    s.map(char => if (allowedCharactersInDirectoryLabel.contains(char)) char
                  else '_')
  }

  private def replaceForbiddenCharactersInFileName(s: String): String = {
    s.map(char => if (forbiddenCharactersInFileName.contains(char)) '_'
                  else char)
  }

  def formatKeyValuePairs(pairs: Map[String, List[String]]): String = {
    pairs.map { case (k, vs) => s"""$k: ${ vs.mkString("\"", ",", "\"") }""" }.mkString("; ")
  }

  def getKeyValuePairs(node: Node, fileName: String, originalFilePath: Option[String]): Map[String, List[String]] = {
    val m = mutable.HashMap[String, mutable.ListBuffer[String]]()
    val fixedKeys = List(
      "hardware",
      "original_OS",
      "software",
      "notes",
      "case_quantity",
      "file_category",
      "description",
      "othmat_codebook",
      "data_collector",
      "collection_date",
      "time_period",
      "geog_cover",
      "geog_unit",
      "local_georef",
      "mapprojection",
      "analytic_units")

    fixedKeys.foreach { key =>
      (node \ key).toList.foreach(n => m.getOrElseUpdate(key, mutable.ListBuffer[String]()).append(n.text))
    }

    (node \ "keyvaluepair").toList.foreach(n => m.getOrElseUpdate((n \ "key").head.text, mutable.ListBuffer[String]()).append((n \ "value").text))
    (node \ "title").filterNot(_.text.toLowerCase == fileName.toLowerCase).foreach(n => m.getOrElseUpdate("title", mutable.ListBuffer[String]()).append(n.text))

    originalFilePath.foreach(of => m.getOrElseUpdate("original_filepath", mutable.ListBuffer(of)))
    m.map { case (k, v) => (k, v.toList) }.toMap
  }
}
