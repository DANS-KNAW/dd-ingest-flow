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
package nl.knaw.dans.easy

import better.files.File
import nl.knaw.dans.lib.scaladv.model.file.FileMeta
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import scala.collection.JavaConverters.{ asScalaBufferConverter, asScalaIteratorConverter }
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

package object dd2d {
  type DepositName = String
  type Sha1Hash = String
  type DatabaseId = Int

  val dateAvailableFormat = new SimpleDateFormat("yyyy-MM-dd")

  case class VaultMetadata(dataversePid: String, dataverseBagId: String, dataverseNbn: String, dataverseOtherId: String, dataverseOtherIdVersion: String, dataverseSwordToken: String) {

    def checkMinimumFieldsForImport(): Try[Unit] = {
      val missing = new mutable.ListBuffer[String]()
      if (StringUtils.isBlank(dataversePid)) missing.append("dataversePid")
      if (StringUtils.isBlank(dataverseNbn)) missing.append("dataverseNbn")
      if (missing.nonEmpty) Failure(new RuntimeException(s"Not enough Data Vault Metadata for import deposit, missing: ${ missing.mkString(", ") }"))
      else Success(())
    }
  }

  case class FileInfo(file: File, checksum: String, metadata: FileMeta)

  case class CannotUpdateDraftDatasetException(deposit: Deposit)
    extends RuntimeException("Latest version must be published before update-deposit can be processed")

  case class InvalidDepositException(deposit: Deposit, msg: String, cause: Throwable = null)
    extends RuntimeException(s"Not a deposit: $msg", cause)

  case class MissingRequiredFieldException(fieldName: String)
    extends RuntimeException(s"No value found for required field: $fieldName")

  case class ExistingResultsInOutboxException(outboxDir: File)
    extends RuntimeException(s"Output directory: $outboxDir already contains results")

  case class OutboxDirIsRegularFileException(outboxDir: File)
    extends RuntimeException(s"Output directory: $outboxDir is a regular file.")

  object OutboxSubdir extends Enumeration {
    type OutboxSubdir = Value
    val PROCESSED = Value("processed")
    val REJECTED = Value("rejected")
    val FAILED = Value("failed")
  }

  def loadCsvToMap(csvFile: File, keyColumn: String, valueColumn: String): Try[Map[String, String]] = {
    import resource.managed

    def csvParse(csvParser: CSVParser): Map[String, String] = {
      csvParser.iterator().asScala
        .map { r => (r.get(keyColumn), r.get(valueColumn)) }.toMap
    }

    managed(CSVParser.parse(
      csvFile.toJava,
      StandardCharsets.UTF_8,
      CSVFormat.RFC4180.withFirstRecordAsHeader())).map(csvParse).tried
  }

  def loadTxtToList(txtFile: File): Try[List[String]] = Try {
    FileUtils.readLines(txtFile.toJava, StandardCharsets.UTF_8).asScala.toList
  }
}
