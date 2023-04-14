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
package com.eurotech.persistence.transactions;


import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Transaction-managing class. Use as transaction boundary, to coordinate operations within a single transaction (ACID).
 */
public interface TxManager {

    /**
     * @param transactionConsumer  The actual set of operations to be executed within the transaction
     * @param afterCommitConsumers Consumers of the result of the main transaction, still executed within the transaction boundary.
     *                             Use for event storing, auditing, etc.
     * @param <R>                  The type of the value ultimately returned by the transaction
     * @return the final result of the transaction
     * @throws RuntimeException for legacy reasons.
     */
    <R> R execute(TxConsumer<R> transactionConsumer, BiConsumer<TxContext, R>... afterCommitConsumers) throws RuntimeException;

    /**
     * @return A {@link Supplier} for a {@link TxContext}. Method provided only to support legacy implementations,
     * use {@link #execute(TxConsumer, BiConsumer[])} whenever possible
     */
    Supplier<TxContext> getSupplier();

    /**
     * This interface is provided only in order to supported the checked exception {@link RuntimeException},
     * otherwise it could have been a simple {@link java.util.function.Function}
     *
     * @param <R> The type of the value ultimately returned by the transaction
     */
    @FunctionalInterface
    public interface TxConsumer<R> {
        R execute(TxContext txHolder) throws RuntimeException;
    }

}
