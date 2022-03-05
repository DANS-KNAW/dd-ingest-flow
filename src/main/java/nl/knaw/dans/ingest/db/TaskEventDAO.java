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
package nl.knaw.dans.ingest.db;

import io.dropwizard.hibernate.AbstractDAO;
import nl.knaw.dans.ingest.core.TaskEvent;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.List;

public class TaskEventDAO extends AbstractDAO<TaskEvent> {

    public TaskEventDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public TaskEvent save(TaskEvent taskEvent) {
        return persist(taskEvent);
    }

    public List<TaskEvent> getEventsByBatch(String batchName) {
        Query<TaskEvent> query = currentSession().createQuery("from TaskEvent "
            + "where batch = :batchName", TaskEvent.class);
        query.setParameter("batchName", batchName);
        return query.list();
    }

    public List<TaskEvent> getEventsByDeposit(String depositId) {
        Query<TaskEvent> query = currentSession().createQuery("from TaskEvent "
            + "where depositId = :depositId", TaskEvent.class);
        query.setParameter("depositId", depositId);
        return query.list();
    }
}
