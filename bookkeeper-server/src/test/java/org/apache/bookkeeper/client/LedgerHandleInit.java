package org.apache.bookkeeper.client;

import lombok.SneakyThrows;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mock;

import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.when;

public class LedgerHandleInit {

    private static Random r = null;
    private static Set<Long> usedValues;
    private long ledgerId;

    @Mock
    protected BookKeeper bookKeeper;

    @Mock
    protected LedgerManagerFactory ledgerManagerFactory;

    @Mock
    protected LedgerManager ledgerManager;

    @Mock
    protected ClientContext clientContext;

    @Mock
    protected LedgerMetadata ledgerMetadata;

    @Mock
    protected Versioned<LedgerMetadata> ledgerMetadataVersioned;

    @Mock
    protected LedgerMetadataBuilder ledgerMetadataBuilder;

    @BeforeClass
    public static void configRandomGenerator(){
        r = new Random();
        usedValues = new HashSet<>();
    }

    @AfterClass
    public static void cleanUpEnv(){
        usedValues.clear();
    }


    @After
    public void releaseResources(){
        usedValues.add(this.ledgerId);
    }



    @SneakyThrows
    @Before
    public void configMock(){

        this.ledgerId = setLedgerIdRandom();

        //build ledger
        //init a clientCtx on asyncAddEntry
        when( this.bookKeeper.getClientCtx() ).thenReturn(this.clientContext);

        //init for build a Versioned<LedgerMetadata> for getCurrentEnsemble on asyncAddEntry
        //init metadata versioned
        when( this.ledgerManagerFactory.newLedgerManager() ).thenReturn(this.ledgerManager);
        when( this.ledgerMetadataBuilder.build() ).thenReturn( this.ledgerMetadata );
        when( this.ledgerManager.createLedgerMetadata(this.ledgerId,this.ledgerMetadata).get()).thenReturn(this.ledgerMetadataVersioned);

    }

    protected ClientContext getClientCtx(){
        return clientContext;
    }

    protected Versioned<LedgerMetadata> getLedgerMetadataVersioned(){
        return ledgerMetadataVersioned;
    }

    public long getLedgerId(){
        return this.ledgerId;
    }

    public long setLedgerIdRandom(){
        do {
            this.ledgerId =  r.nextLong(); // generate random
        }while( usedValues.contains(this.ledgerId));
        return this.ledgerId;
    }
}
