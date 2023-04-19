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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class TxManagerImpl implements TxManager {

    public TxManagerImpl(Supplier<TxContext> txContextSupplier, Integer maxInsertAttempts) {
        this.txContextSupplier = txContextSupplier;
        this.maxInsertAttempts = maxInsertAttempts;
    }

    @Override
    public <R> R execute(TxConsumer<R> transactionConsumer, BiConsumer<TxContext, R>... additionalTxConsumers)
            throws RuntimeException {
        int retry = 0;
        final TxContext txContext = txContextSupplier.get();
        try {
            while (true) {
                try {
                    final R res = transactionConsumer.execute(txContext);
                    Arrays.stream(additionalTxConsumers)
                            .forEach(additionalTxConsumer -> additionalTxConsumer.accept(txContext, res));
                    txContext.commit();
                    return res;
                } catch (Exception ex) {
                    txContext.rollback();
                    if (!txContext.isRecoverableException(ex)) {
                        throw txContext.convertPersistenceException(ex);
                    }
                    if (++retry >= maxInsertAttempts) {
                        logger.error("Recoverable exception, but retry attempts exceeded, failing", ex);
                        throw txContext.convertPersistenceException(ex);
                    }
                    logger.warn("Recoverable exception, retrying", ex);
                }
            }
        } finally {
            try {
                txContext.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final Supplier<TxContext> txContextSupplier;
    private final int maxInsertAttempts;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Supplier<TxContext> getSupplier() {
        return txContextSupplier;
    }
}
