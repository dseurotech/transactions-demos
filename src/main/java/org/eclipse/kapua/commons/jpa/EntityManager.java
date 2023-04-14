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
package org.eclipse.kapua.commons.jpa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

/**
 * Kapua JPA entity manager wrapper
 *
 * @since 1.0
 */
public class EntityManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntityManagerFactory.class);

    private javax.persistence.EntityManager javaxPersitenceEntityManager;

    /**
     * Constructs a new entity manager wrapping the given {@link javax.persistence.EntityManager}
     *
     * @param javaxPersitenceEntityManager
     */
    public EntityManager(javax.persistence.EntityManager javaxPersitenceEntityManager) {
        this.javaxPersitenceEntityManager = javaxPersitenceEntityManager;
    }

    /**
     * Opens a Jpa Transaction.
     */
    public void beginTransaction() {
        if (javaxPersitenceEntityManager == null) {
            throw new IllegalStateException("null EntityManager");
        }
        javaxPersitenceEntityManager.getTransaction().begin();
    }

    /**
     * Commits the current Jpa Transaction.
     */
    public void commit() {
        if (javaxPersitenceEntityManager == null) {
            throw new IllegalStateException("null EntityManager");
        }
        if (!javaxPersitenceEntityManager.getTransaction().isActive()) {
            throw new IllegalStateException("Transaction Not Active");
        }

        try {
            javaxPersitenceEntityManager.getTransaction().commit();
        } catch (Exception e) {
            throw new IllegalStateException("Commit Error", e);
        }
    }

    /**
     * Rollbacks the current Jpa Transaction. No exception will be thrown when rolling back so that the original exception that caused the rollback can be thrown.
     */
    public void rollback() {
        try {
            if (javaxPersitenceEntityManager != null &&
                    javaxPersitenceEntityManager.getTransaction().isActive()) {
                javaxPersitenceEntityManager.getTransaction().rollback();
            }
        } catch (Exception e) {
            LOG.warn("Rollback Error", e);
        }
    }

    /**
     * Return the transaction status
     *
     * @return
     */
    public boolean isTransactionActive() {
        return (javaxPersitenceEntityManager != null &&
                javaxPersitenceEntityManager.getTransaction().isActive());
    }

    /**
     * Closes the EntityManager
     */
    public void close() {
        if (javaxPersitenceEntityManager != null) {
            javaxPersitenceEntityManager.close();
        }
    }

    public <E> void persist(E entity) {
        javaxPersitenceEntityManager.persist(entity);
    }

    public void flush() {
        javaxPersitenceEntityManager.flush();
    }

    public <E> E find(Class<E> clazz, Object id, LockModeType lockModeType) {
        return javaxPersitenceEntityManager.find(clazz, id, lockModeType);
    }

    public <E> void merge(E entity) {
        javaxPersitenceEntityManager.merge(entity);
    }

    public <E> void refresh(E entity) {
        javaxPersitenceEntityManager.refresh(entity);
    }

    /**
     * Detach the entity
     *
     * @param entity
     */
    public <E> void detach(E entity) {
        javaxPersitenceEntityManager.detach(entity);
    }

    /**
     * Remove the entity
     *
     * @param entity
     */
    public <E> void remove(E entity) {
        javaxPersitenceEntityManager.remove(entity);
    }

    /**
     * Return the {@link CriteriaBuilder}
     *
     * @return
     */
    public CriteriaBuilder getCriteriaBuilder() {
        return javaxPersitenceEntityManager.getCriteriaBuilder();
    }

    /**
     * Return the typed query based on the criteria
     *
     * @param criteriaSelectQuery
     * @return
     */
    public <E> TypedQuery<E> createQuery(CriteriaQuery<E> criteriaSelectQuery) {
        return javaxPersitenceEntityManager.createQuery(criteriaSelectQuery);
    }

    /**
     * Return the typed query based on the query name
     *
     * @param queryName
     * @param clazz
     * @return
     */
    public <E> TypedQuery<E> createNamedQuery(String queryName, Class<E> clazz) {
        return javaxPersitenceEntityManager.createNamedQuery(queryName, clazz);
    }

    /**
     * Return native query based on provided sql query
     *
     * @param querySelectUuidShort
     * @return
     */
    public <E> Query createNativeQuery(String querySelectUuidShort) {
        return javaxPersitenceEntityManager.createNativeQuery(querySelectUuidShort);
    }
}
