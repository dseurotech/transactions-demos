package com.eurotech.demos.transactions;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Version;

@Entity
public class VersionedEntity extends NonVersionedEntity implements DemoEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @Version
    private Integer version;

    public static VersionedEntity newEntity(String content) {
        VersionedEntity res = new VersionedEntity();
        res.setContent(content);
        return res;
    }

    @Override
    public String toString() {
        return "VersionedEntity [id=" + getId() + ", version=" + version + ", changesCounter=" + getChangesCounter() + ", content=" + getContent() + "]";
    }
}