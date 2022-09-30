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
import nl.knaw.dans.ingest.core.BlockedTarget;
import nl.knaw.dans.ingest.core.TaskEvent;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.LinkedList;
import java.util.List;

public class BlockedTargetDAO extends AbstractDAO<BlockedTarget> {
    public BlockedTargetDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }


    public BlockedTarget save(BlockedTarget blockedTarget) {
        return persist(blockedTarget);
    }

    public List<BlockedTarget> getTarget(String target, String depositId) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaQuery<BlockedTarget> crit = cb.createQuery(BlockedTarget.class);
        Root<BlockedTarget> r = crit.from(BlockedTarget.class);
        List<Predicate> predicates = new LinkedList<>();
        if (target != null) {
            predicates.add(cb.equal(r.get("target"), target));
        }
        if (depositId != null) {
            predicates.add(cb.equal(r.get("depositId"), depositId));
        }
        crit
            .select(r)
            .where(cb.and(predicates.toArray(new Predicate[0])));

        return currentSession().createQuery(crit).list();
    }
}
