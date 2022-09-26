package nl.knaw.dans.ingest.core;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "blocked_target")

public class BlockedTarget {
    @Column(name = "deposit_id", nullable = false, length = 20)
    private String depositId;

    @Column(name = "target", nullable = false, length = 20)
    private String target;

    @Column(name = "state", nullable = false, length = 20)
    private String state;

    @Column(name = "message", nullable = false, length = 20)
    private String message;

    public BlockedTarget() {}

    public BlockedTarget(String depositId, String target, String state, String message) {
        this.depositId = depositId;
        this.target = target;
        this.state = state;
        this.message = message;
    }

    public String getDepositId() {
        return depositId;
    }

    public void setDepositId(String depositId) {
        this.depositId = depositId;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
