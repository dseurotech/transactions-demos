package org.eclipse.kapua.commons.jpa;

import com.eurotech.demos.transactions.DemoEntity;
import com.eurotech.demos.transactions.NonVersionedEntity;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceException;
import java.sql.SQLException;

public class DemoEntityDAO<E extends DemoEntity> {
    private static final String ATTRIBUTE_SEPARATOR = ".";
    private static final String ATTRIBUTE_SEPARATOR_ESCAPED = "\\.";
    private static final String COMPARE_ERROR_MESSAGE = "Trying to compare a non-comparable value";
    private static final String SQL_ERROR_CODE_CONSTRAINT_VIOLATION = "23505";
    private final Class<E> clazz;

    public DemoEntityDAO(Class<E> clazz) {
        this.clazz = clazz;
    }


    public E create(EntityManager em, E entity) {
        try {
            em.persist(entity);
            em.flush();
            em.refresh(entity);
        } catch (EntityExistsException e) {
            throw new EntityExistsException(e);
        } catch (PersistenceException e) {
            if (DemoEntityDAO.isInsertConstraintViolation(e)) {
                NonVersionedEntity entityFound = em.find(NonVersionedEntity.class, entity.getId());
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

    public E find(EntityManager em, Long entityId) {
        // Checking existence
        E entityToFind = em.find(clazz, entityId);
        return entityToFind;
    }

    public E update(EntityManager em, E entity) {
        //
        // Checking existence
        E entityToUpdate = em.find(clazz, entity.getId());

        //
        // Updating if not null
        if (entityToUpdate != null) {
            em.merge(entity);
            em.flush();
            em.refresh(entityToUpdate);
        } else {
            throw new EntityNotFoundException(NonVersionedEntity.class.getSimpleName());
        }

        return entityToUpdate;
    }

    public E delete(EntityManager em, Long entityId) {
        //
        // Checking existence
        E entityToDelete = find(em, entityId);

        // Deleting if found
        if (entityToDelete != null) {
            em.remove(entityToDelete);
            em.flush();
        } else {
            throw new EntityNotFoundException(NonVersionedEntity.class.getSimpleName());
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
