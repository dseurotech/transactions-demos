package com.eurotech.demos.transactions;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PreUpdate;

@Entity
public class NonVersionedEntity implements DemoEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    Long id;
    String content;
    int changesCounter = 0;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public String getContent() {
        return content;
    }

    @Override
    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public int getChangesCounter() {
        return changesCounter;
    }

    @PreUpdate
//    @PrePersist
    public void upCounter() {
        this.changesCounter = this.changesCounter + 1;
    }

    @Override
    public String toString() {
        return "NonVersionedEntity [id=" + getId() + ", changesCounter=" + getChangesCounter() + ", content=" + getContent() + "]";
    }

    public static NonVersionedEntity newEntity(String content) {
        NonVersionedEntity res = new NonVersionedEntity();
        res.setContent(content);
        return res;
    }
}