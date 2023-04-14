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


import com.eurotech.persistence.transactions.TxManager;
import com.eurotech.persistence.transactions.TxManagerImpl;

import javax.persistence.Persistence;

public class JpaTxManagerFactory {
    private final int maxInsertAttempts;

    public JpaTxManagerFactory(int maxInsertAttempts) {
        this.maxInsertAttempts = maxInsertAttempts;
    }

    public TxManager create(String persistenceUnitName) {
        return new TxManagerImpl(() -> new JpaTxContext(Persistence.createEntityManagerFactory(persistenceUnitName)), maxInsertAttempts);
    }
}
