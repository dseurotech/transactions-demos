package com.eurotech.persistence.repositories;

import com.eurotech.demos.transactions.DemoEntity;
import com.eurotech.persistence.transactions.TxContext;
import com.eurotech.persistence.transactions.jpa.JpaAwareTxContext;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceException;
import java.sql.SQLException;
import java.util.Optional;

public class DemoEntityRepository<E extends DemoEntity> {
    private static final String SQL_ERROR_CODE_CONSTRAINT_VIOLATION = "23505";

    private final Class<E> clazz;

    public DemoEntityRepository(Class<E> clazz) {
        this.clazz = clazz;
    }

    public E create(TxContext tx, E entity) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(tx);
        try {
            em.persist(entity);
            em.flush();
            em.refresh(entity);
            return entity;
        } catch (EntityExistsException e) {
            // this will be intercepted by the calling method, that will decide whether to retry or not
            throw new EntityExistsException(Long.toString(entity.getId()), e);
        } catch (PersistenceException e) {
            //if it is a contraint violation....
            if (isInsertConstraintViolation(e)) {
                //then check if an entity with the same id is already present
                final E entityFound = em.find(clazz, entity.getId());
                if (entityFound == null) {
                    //if it is not, just propagate the original exception (cannot be an id clash)
                    throw e;
                }
                //if an entity with the same id is already present, treat it as a generated id conflict and bubble up for potential retry
                throw new EntityExistsException(Long.toString(entity.getId()), e);
            } else {
                throw e;
            }
        }
    }

    public Optional<E> find(TxContext txContext, Long entityId) {
        final javax.persistence.EntityManager em = JpaAwareTxContext.extractEntityManager(txContext);
        return doFind(em, entityId);
    }

    protected Optional<E> doFind(EntityManager em, Long entityId) {
        return Optional.ofNullable(em.find(clazz, entityId));
    }

    public E delete(TxContext txContext, long entityId) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(txContext);
        // Checking existence
        return doFind(em, entityId)
                // Deleting if found
                .map(e -> doDelete(em, e))
                .orElseThrow(() -> new EntityNotFoundException(Long.toString(entityId)));
    }


    public E delete(TxContext txContext, E entityToDelete) {
        final EntityManager em = JpaAwareTxContext.extractEntityManager(txContext);
        return doDelete(em, entityToDelete);
    }

    protected E doDelete(EntityManager em, E entityToDelete) {
        em.remove(entityToDelete);
        em.flush();
        // Returning deleted entity
        return entityToDelete;
    }

    public static boolean isInsertConstraintViolation(PersistenceException persistenceException) {
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

    public E update(TxContext txContext, E updatedEntity) {
        final javax.persistence.EntityManager em = JpaAwareTxContext.extractEntityManager(txContext);
        // Checking existence
        return doFind(em, updatedEntity.getId())
                // Updating if present
                .map(ce -> doUpdate(em, ce, updatedEntity))
                .orElseThrow(() -> new EntityNotFoundException(clazz.getSimpleName()));
    }

    public E update(TxContext txContext, E currentEntity, E updatedEntity) {
        final javax.persistence.EntityManager em = JpaAwareTxContext.extractEntityManager(txContext);
        return doUpdate(em, currentEntity, updatedEntity);
    }

    protected E doUpdate(javax.persistence.EntityManager em, E currentEntity, E updatedEntity) {
        em.merge(updatedEntity);
        em.flush();
        em.refresh(currentEntity);
        return currentEntity;
    }
}
