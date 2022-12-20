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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AmdTest extends BaseTest {

    @Test
    void toDateOfDeposit_should_use_date_of_first_change_to_SUBMITTED_state() throws Exception {
        var str =
            "<damd:administrative-md version=\"0.1\" xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\">\n"
                + "      <datasetState>PUBLISHED</datasetState>\n"
                + "      <previousState>MAINTENANCE</previousState>\n"
                + "      <depositorId>rutgerelsma</depositorId>\n"
                + "      <stateChangeDates>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>DRAFT</fromState>\n"
                + "          <toState>SUBMITTED</toState>\n"
                + "          <changeDate>2017-04-13T11:03:05.000+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>SUBMITTED</fromState>\n"
                + "          <toState>DRAFT</toState>\n"
                + "          <changeDate>2017-04-14T11:03:05.000+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>DRAFT</fromState>\n"
                + "          <toState>SUBMITTED</toState>\n"
                + "          <changeDate>2017-04-15T11:03:05.000+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>SUBMITTED</fromState>\n"
                + "          <toState>PUBLISHED</toState>\n"
                + "          <changeDate>2017-04-16T14:35:11.281+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>PUBLISHED</fromState>\n"
                + "          <toState>MAINTENANCE</toState>\n"
                + "          <changeDate>2017-08-08T16:51:19.886+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>MAINTENANCE</fromState>\n"
                + "          <toState>PUBLISHED</toState>\n"
                + "          <changeDate>2017-08-09T16:52:20.914+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "      </stateChangeDates>\n"
                + "      <groupIds/>\n"
                + "      <damd:workflowData version=\"0.1\" />\n"
                + "    </damd:administrative-md>";

        var doc = readDocumentFromString(str);
        var result = Amd.toDateOfDeposit(doc).orElseThrow();
        assertEquals("2017-04-13", result);
    }

    @Test()
    void toDateOfDeposit_should_use_date_of_first_change_to_PUBLISHED_state_if_no_change_to_SUBMITTED_state_is_found() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <stateChangeDates>\n"
            + "                  <damd:stateChangeDate>\n"
            + "                    <fromState>DRAFT</fromState>\n"
            + "                    <toState>PUBLISHED</toState>\n"
            + "                    <changeDate>2019-04-25T12:20:01.636+02:00</changeDate>\n"
            + "                  </damd:stateChangeDate>\n"
            + "                </stateChangeDates>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";

        var doc = readDocumentFromString(xml);
        var result = Amd.toDateOfDeposit(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

    @Test
    void toDateOfDeposit_should_use_date_lastStateChange_if_no_stateChangeDates_are_available() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <stateChangeDates>\n"
            + "                </stateChangeDates>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";

        var doc = readDocumentFromString(xml);
        var result = Amd.toDateOfDeposit(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

    @Test
    void toDateOfDeposit_should_use_date_lastStateChange_if_no_NONBLANK_stateChangeDates_are_available() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <stateChangeDates>\n"
            + "                  <damd:stateChangeDate>\n"
            + "                    <fromState>DRAFT</fromState>\n"
            + "                    <toState>PUBLISHED</toState>\n"
            + "                    <changeDate></changeDate>\n"
            + "                  </damd:stateChangeDate>\n"
            + "                </stateChangeDates>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";

        var doc = readDocumentFromString(xml);
        var result = Amd.toDateOfDeposit(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

    @Test
    void toDateOfDeposit_should_use_date_lastStateChange_if_no_StateChangeDates_ELEMENT_is_present() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";

        var doc = readDocumentFromString(xml);
        var result = Amd.toDateOfDeposit(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

    @Test
    void toPublicationDate_should_use_date_of_first_change_to_PUBLISHEd_state() throws Exception {
        var xml =
            "    <damd:administrative-md version=\"0.1\" xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\">\n"
                + "      <datasetState>PUBLISHED</datasetState>\n"
                + "      <previousState>MAINTENANCE</previousState>\n"
                + "      <depositorId>rutgerelsma</depositorId>\n"
                + "      <stateChangeDates>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>DRAFT</fromState>\n"
                + "          <toState>SUBMITTED</toState>\n"
                + "          <changeDate>2017-04-13T11:03:05.000+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>SUBMITTED</fromState>\n"
                + "          <toState>DRAFT</toState>\n"
                + "          <changeDate>2017-04-14T11:03:05.000+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>DRAFT</fromState>\n"
                + "          <toState>SUBMITTED</toState>\n"
                + "          <changeDate>2017-04-15T11:03:05.000+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>SUBMITTED</fromState>\n"
                + "          <toState>PUBLISHED</toState>\n"
                + "          <changeDate>2017-04-16T14:35:11.281+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>PUBLISHED</fromState>\n"
                + "          <toState>MAINTENANCE</toState>\n"
                + "          <changeDate>2017-08-08T16:51:19.886+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "        <damd:stateChangeDate>\n"
                + "          <fromState>MAINTENANCE</fromState>\n"
                + "          <toState>PUBLISHED</toState>\n"
                + "          <changeDate>2017-08-09T16:52:20.914+02:00</changeDate>\n"
                + "        </damd:stateChangeDate>\n"
                + "      </stateChangeDates>\n"
                + "      <groupIds/>\n"
                + "      <damd:workflowData version=\"0.1\" />\n"
                + "    </damd:administrative-md>";

        var doc = readDocumentFromString(xml);
        var result = Amd.toPublicationDate(doc).orElseThrow();
        assertEquals("2017-04-16", result);
    }

    @Test
    void toPublicationDate_should_use_date_lastStateChange_if_no_stateChangeDates_are_available() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <stateChangeDates>\n"
            + "                </stateChangeDates>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";

        var doc = readDocumentFromString(xml);
        var result = Amd.toPublicationDate(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

    @Test
    void toPublicationDate_should_use_date_lastStateChange_if_no_NONBLANK_stateChangeDates_are_available() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <stateChangeDates>\n"
            + "                  <damd:stateChangeDate>\n"
            + "                    <fromState>DRAFT</fromState>\n"
            + "                    <toState>PUBLISHED</toState>\n"
            + "                    <changeDate></changeDate>\n"
            + "                  </damd:stateChangeDate>\n"
            + "                </stateChangeDates>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";
        var doc = readDocumentFromString(xml);
        var result = Amd.toPublicationDate(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

    @Test
    void toPublicationDate_should_use_date_lastStateChange_if_no_StateChangeDates_ELEMENT_is_present() throws Exception {
        var xml = "<damd:administrative-md xmlns:damd=\"http://easy.dans.knaw.nl/easy/dataset-administrative-metadata/\" version=\"0.1\">\n"
            + "                <datasetState>PUBLISHED</datasetState>\n"
            + "                <previousState>DRAFT</previousState>\n"
            + "                <lastStateChange>2019-04-25T12:20:01.636+02:00</lastStateChange>\n"
            + "                <depositorId>PANVU</depositorId>\n"
            + "                <groupIds></groupIds>\n"
            + "                <damd:workflowData version=\"0.1\" />\n"
            + "              </damd:administrative-md>";
        var doc = readDocumentFromString(xml);
        var result = Amd.toPublicationDate(doc).orElseThrow();
        assertEquals("2019-04-25", result);
    }

}