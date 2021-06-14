package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Before;
import org.mockito.Mock;

import java.util.EnumSet;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.when;

public class LedgerHandleInit {

    private AsyncAddEntryTest test;

    public LedgerHandleInit( AsyncAddEntryTest test ){
        this.test = test;
    }

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
    protected BookKeeper.DigestType digestType;

    @Mock
    protected EnumSet<WriteFlag> writeFlags;

    @Mock
    protected AsyncCallback.AddCallback cb;

    @Mock
    protected Object ctx;

    @Before
    public void configMock(){
        long ledgerId = test.setLedgerIdRandom();
        //build ledger
        when( this.ledgerManagerFactory.newLedgerManager() ).thenReturn(this.ledgerManager);
        when( ledgerManager.createLedgerMetadata(ledgerId,any())).thenReturn();
        //build client context
        when(clientContext).thenReturn(this.bookKeeper.getClientCtx());

        //build ledger metadata

        //build digest type and write flags
    }

    protected ClientContext getClientCtx(){
        return clientContext;
    }

    protected Versioned<LedgerMetadata> getLedgerMetadataVersioned(){
        return ledgerMetadataVersioned;
    }

    protected BookKeeper.DigestType getDigestType(){
        return digestType;
    }

    protected EnumSet<WriteFlag> getWriteFlags(){
        return writeFlags;
    }

    protected AsyncCallback.AddCallback getCallBack(){
        return cb;
    }

    protected Object getCtx(){
        return ctx;
    }
}
