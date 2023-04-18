package com.eurotech.demos.transactions;

public interface DemoEntity {
    Long getId();

    String getContent();

    void setContent(String content);

    int getChangesCounter();

}
