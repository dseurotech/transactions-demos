package com.eurotech.demos.transactions;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PreUpdate;

@Entity
public class DemoEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String content;
    private int changesCounter = 0;

    public Long getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getChangesCounter() {
        return changesCounter;
    }

    @PreUpdate
//    @PrePersist
    private void upCounter() {
        this.changesCounter = this.changesCounter + 1;
    }

    @Override
    public String toString() {
        return "DemoEntity [id=" + id + ", changesCounter=" + changesCounter + ", content=" + content + "]";
    }
}