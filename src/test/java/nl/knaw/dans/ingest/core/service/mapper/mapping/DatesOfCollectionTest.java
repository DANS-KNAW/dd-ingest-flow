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

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_END;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DATE_OF_COLLECTION_START;
import static org.assertj.core.api.Assertions.assertThat;

class DatesOfCollectionTest extends BaseTest {

    @Test
    void toDateOfCollectionValue_should_split_correctly_formatted_date_range_in_start_and_end_subfields() throws Exception {
        var doc = readDocumentFromString("<ddm:datesOfCollection xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    2022-01-01/2022-02-01\n"
            + "</ddm:datesOfCollection>\n");

        var builder = new CompoundFieldBuilder("", true);
        DatesOfCollection.toDistributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_START)
            .extracting("value")
            .containsOnly("2022-01-01");

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_END)
            .extracting("value")
            .containsOnly("2022-02-01");
    }

    @Test
    void toDateOfCollectionValue_should_handle_ranges_without_start() throws Exception {
        var doc = readDocumentFromString("<ddm:datesOfCollection xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    /2022-02-01\n"
            + "</ddm:datesOfCollection>\n");

        var builder = new CompoundFieldBuilder("", true);
        DatesOfCollection.toDistributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_START)
            .extracting("value")
            .containsOnly("");

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_END)
            .extracting("value")
            .containsOnly("2022-02-01");
    }

    @Test
    void toDateOfCollectionValue_should_handle_ranges_without_end() throws Exception {
        var doc = readDocumentFromString("<ddm:datesOfCollection xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\">\n"
            + "    2022-01-01/   \n"
            + "</ddm:datesOfCollection>\n");

        var builder = new CompoundFieldBuilder("", true);
        DatesOfCollection.toDistributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_START)
            .extracting("value")
            .containsOnly("2022-01-01");

        assertThat(field.getValue())
            .extracting(DATE_OF_COLLECTION_END)
            .extracting("value")
            .containsOnly("");
    }
}