/*******************************************************************************
 * Copyright (c) 2016, 2022 Eurotech and/or its affiliates and others
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package com.eurotech.persistence.transactions.jpa;

import com.eurotech.persistence.transactions.TxContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.OptimisticLockException;
import javax.persistence.PessimisticLockException;
import javax.persistence.RollbackException;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Predicate;

public class JpaTxContext implements JpaAwareTxContext, TxContext {
    public final EntityManagerFactory entityManagerFactory;
    Optional<EntityManager> entityManager = Optional.empty();
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public JpaTxContext(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    @Override
    public EntityManager getEntityManager() {
        this.entityManager = Optional.of(this.entityManager
                .orElseGet(() -> entityManagerFactory.createEntityManager()));
        final EntityTransaction tx = entityManager.get().getTransaction();
        if (!tx.isActive()) {
            tx.begin();
        }
        return entityManager.get();
    }

    @Override
    public void commit() {
        entityManager.ifPresent(e -> e.getTransaction().commit());
    }

    @Override
    public void rollback() {
        entityManager.ifPresent(entityManager -> {
            final EntityTransaction tx = entityManager.getTransaction();
            if (tx.isActive()) {
                tx.rollback();
            }
        });
    }

    @Override
    public void close() throws IOException {
        entityManager.ifPresent(entityManager -> entityManager.close());
    }

    @Override
    public RuntimeException convertPersistenceException(Exception ex) {
        return Optional.ofNullable(ex).map(e -> new RuntimeException(e)).orElse(new RuntimeException("no details"));
    }

    private final Predicate isLockExceptionTester = t -> t instanceof OptimisticLockException || t instanceof PessimisticLockException;

    @Override
    public boolean isRecoverableException(Exception e) {
        if (e instanceof EntityExistsException) {

            /*
             * Most KapuaEntities inherit from AbstractKapuaEntity, which auto-generates ids via a method marked with @PrePersist and the use of
             * a org.eclipse.kapua.commons.model.id.IdGenerator. Ids are pseudo-randomic. To deal with potential conflicts, a number of retries
             * is allowed. The entity needs to be detached in order for the @PrePersist method to be invoked once more, generating a new id
             * */
            logger.warn("Conflict on entity creation. Cannot insert the entity, trying again!");
            return true;
        }
        final boolean isValidLockException = isLockExceptionTester.test(e) || (e instanceof RollbackException && isLockExceptionTester.test(e.getCause()));
        if (isValidLockException) {
            logger.warn("Recoverable Lock Exception");
            return true;
        }
        return false;
    }
}
