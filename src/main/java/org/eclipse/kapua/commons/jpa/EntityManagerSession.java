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
 *     Red Hat
 *******************************************************************************/
package org.eclipse.kapua.commons.jpa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityExistsException;

/**
 * Entity manager session reference implementation.
 *
 * @since 1.0
 */
public class EntityManagerSession {

    private static final Logger logger = LoggerFactory.getLogger(EntityManagerSession.class);

    private final EntityManagerFactory entityManagerFactory;
    private static final int MAX_INSERT_ALLOWED_RETRY = 2;

    private TransactionManager transacted = new TransactionManagerTransacted();
    private TransactionManager notTransacted = new TransactionManagerNotTransacted();

    /**
     * Constructor
     *
     * @param entityManagerFactory
     */
    public EntityManagerSession(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;

    }

    /**
     * Return the execution result invoked on a new entity manager.<br>
     * If the requested action is an insert, it reiterates the execution if it fails due to
     * <br>
     * WARNING!<br>
     * The transactionality (if needed by the code) must be managed internally to the entityManagerCallback.<br>
     * This method performs only a rollback (if the transaction is active and an error occurred)!<br>
     *
     * @param resultHandler
     * @return
     */
    public <T> T doAction(EntityManagerCallback<T> resultHandler) {
        return internalOnResult(EntityManagerContainer.<T>create().onResultHandler(resultHandler), notTransacted, false);
    }

    /**
     * Return the execution result invoked on a new entity manager.<br>
     * If the requested action is an insert, it reiterates the execution if it fails due to
     * <br>
     * WARNING!<br>
     * The transactionality is managed by this method so the called entityManagerResultCallback must leave the transaction open<br>
     *
     * @param resultHandler
     * @return
     */
    public <T> T doTransactedAction(EntityManagerCallback<T> resultHandler) {
        return internalOnResult(EntityManagerContainer.<T>create().onResultHandler(resultHandler), transacted, true);
    }

    /**
     * Return the execution result invoked on a new entity manager.<br>
     * If the requested action is an insert, it reiterates the execution if it fails due to
     * <br>
     * This method allows to set the before and after result handler calls
     * <p>
     * WARNING!<br>
     * The transactionality (if needed by the code) must be managed internally to the entityManagerCallback.<br>
     * This method performs only a rollback (if the transaction is active and an error occurred)!<br>
     *
     * @param container
     * @return
     */
    public <T> T doAction(EntityManagerContainer<T> container) {
        return internalOnResult(container, notTransacted, false);
    }

    /**
     * Return the execution result invoked on a new entity manager.<br>
     * If the requested action is an insert, it reiterates the execution if it fails due to
     * <br>
     * This method allows to set the before and after result handler calls
     * <p>
     * WARNING!<br>
     * The transactionality is managed by this method so the called entityManagerResultCallback must leave the transaction open<br>
     *
     * @param container
     * @return
     */
    public <T> T doTransactedAction(EntityManagerContainer<T> container) {
        return internalOnResult(container, transacted, true);
    }

    private <T> T internalOnResult(EntityManagerContainer<T> container, TransactionManager transactionManager, boolean transacted) {
        boolean succeeded = false;
        int retry = 0;
        T instance = container.onBefore();
        if (instance == null) {
            EntityManager manager = entityManagerFactory.createEntityManager();
            try {
                do {
                    try {
                        transactionManager.beginTransaction(manager);
                        instance = container.onResult(manager);

                        transactionManager.commit(manager);
                        succeeded = true;
                        if (instance != null) {
                            manager.detach(instance);
                            container.onAfter(instance);
                        }
                    } catch (EntityExistsException e) {
                        if (manager != null) {
                            manager.rollback();
                        }
                        if (++retry < MAX_INSERT_ALLOWED_RETRY) {
                            logger.warn("Entity already exists. Cannot insert the entity, try again!");
                        } else {
                            manager.rollback();
                            throw new RuntimeException(e);
                        }
                    }
                }
                while (!succeeded);
            } catch (Exception e) {
                if (manager != null) {
                    manager.rollback();
                }
                throw new RuntimeException(e);
            } finally {
                if (manager != null) {
                    manager.close();
                }
            }
        }
        return instance;
    }

}
