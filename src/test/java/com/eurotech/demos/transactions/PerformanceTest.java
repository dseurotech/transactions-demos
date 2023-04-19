package com.eurotech.demos.transactions;

import com.eurotech.persistence.repositories.DemoEntityRepository;
import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.jpa.JpaTxManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

public class PerformanceTest {

    public static final int LOOPS = 100;
    private JpaTxManagerFactory txManagerFactory;

    @BeforeEach
    void setUp() {
        txManagerFactory = new JpaTxManagerFactory(2);
    }

    @Test
    void timeInsertTime() {
        final TxManager txManager = txManagerFactory.create("demos");
        final DemoEntityRepository<NonVersionedEntity> repo = new DemoEntityRepository<>(NonVersionedEntity.class);

        txManager.execute(tx -> repo.create(tx, NonVersionedEntity.newEntity("Warming up")));
        for (int loops_number = 1; loops_number <= 10000; loops_number = loops_number * 2) {
            System.out.print(String.format("%d inserts: ", loops_number));
            final int finalLoops_number = loops_number;
            {
                final Instant start = Instant.now();
                for (int i = 0; i < loops_number; i++) {
                    final int finalI = i;
                    txManager.execute(tx -> repo.create(tx, NonVersionedEntity.newEntity("Entity number " + finalI)));
                }
                final Instant end = Instant.now();
                final Duration operationDuration = Duration.between(start, end);
                System.out.print(String.format("individual transactions: %d millis", operationDuration.toMillis()));
            }
            {
                final Instant start = Instant.now();
                txManager.execute(tx -> {
                    for (int i = 0; i < finalLoops_number; i++) {
                        final int finalI = i;
                        repo.create(tx, NonVersionedEntity.newEntity("Entity number " + finalI));
                    }
                    return null;
                });
                final Instant end = Instant.now();
                final Duration operationDuration = Duration.between(start, end);
                System.out.println(String.format(", single transaction: %d millis", operationDuration.toMillis()));
            }
        }
    }
}
