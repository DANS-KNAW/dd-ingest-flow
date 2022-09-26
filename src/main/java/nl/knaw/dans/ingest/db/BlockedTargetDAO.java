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
