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

import com.eurotech.demos.transactions.DemoEntity;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceException;
import java.sql.SQLException;

/**
 * @since 1.0.0
 */
public class DemoDAO {

    private static final String ATTRIBUTE_SEPARATOR = ".";
    private static final String ATTRIBUTE_SEPARATOR_ESCAPED = "\\.";

    private static final String COMPARE_ERROR_MESSAGE = "Trying to compare a non-comparable value";
    private static final String SQL_ERROR_CODE_CONSTRAINT_VIOLATION = "23505";

    public static DemoEntity create(EntityManager em, DemoEntity entity) {
        try {
            em.persist(entity);
            em.flush();
            em.refresh(entity);
        } catch (EntityExistsException e) {
            throw new EntityExistsException(e);
        } catch (PersistenceException e) {
            if (isInsertConstraintViolation(e)) {
                DemoEntity entityFound = em.find(DemoEntity.class, entity.getId(), null);
                if (entityFound == null) {
                    throw e;
                }
                throw new EntityExistsException(e);
            } else {
                throw e;
            }
        }

        return entity;
    }

    public static DemoEntity find(EntityManager em, Long entityId) {
        // Checking existence
        DemoEntity entityToFind = em.find(DemoEntity.class, entityId, null);
        return entityToFind;
    }

    public static DemoEntity update(EntityManager em, DemoEntity entity) {
        //
        // Checking existence
        DemoEntity entityToUpdate = em.find(DemoEntity.class, entity.getId(), null);

        //
        // Updating if not null
        if (entityToUpdate != null) {
            em.merge(entity);
            em.flush();
            em.refresh(entityToUpdate);
        } else {
            throw new EntityNotFoundException(DemoEntity.class.getSimpleName());
        }

        return entityToUpdate;
    }

    public static DemoEntity delete(EntityManager em, Long entityId) {
        //
        // Checking existence
        DemoEntity entityToDelete = find(em, entityId);

        // Deleting if found
        if (entityToDelete != null) {
            em.remove(entityToDelete);
            em.flush();
        } else {
            throw new EntityNotFoundException(DemoEntity.class.getSimpleName());
        }

        //
        // Returning deleted entity
        return entityToDelete;
    }


    /**
     * Check if the given {@link PersistenceException} is a SQL constraint violation error.
     *
     * @param persistenceException {@link PersistenceException} to check.
     * @return {@code true} if it is a constraint validation error, {@code false} otherwise.
     * @since 1.0.0
     */
    private static boolean isInsertConstraintViolation(PersistenceException persistenceException) {
        Throwable cause = persistenceException.getCause();
        while (cause != null && !(cause instanceof SQLException)) {
            cause = cause.getCause();
        }

        if (cause == null) {
            return false;
        }

        SQLException innerExc = (SQLException) cause;
        return SQL_ERROR_CODE_CONSTRAINT_VIOLATION.equals(innerExc.getSQLState());
    }
}
