package org.apache.bookkeeper.bookie.storage.ldb;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.when;

public class LedgerMetadataIndexInit {
    private Map<byte[], byte[]> ledgerDataMap;
    private Iterator<Map.Entry<byte[], byte[]>> iterator;
    private boolean exist;


    @Mock
    private KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator;

    @Mock
    private KeyValueStorageFactory keyValueStorageFactory;

    @Mock
    private KeyValueStorage keyValueStorage;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    protected LedgerMetadataIndexInit(boolean exist){
        this.ledgerDataMap = new HashMap<>();
        this.exist = exist;
    }

    @Before
    public void configMock() throws IOException {
        when(this.keyValueStorageFactory.newKeyValueStorage(any(),any(),any())).thenReturn(this.keyValueStorage);
        when(keyValueStorage.iterator()).then(invocationOnMock -> {
            this.iterator = this.ledgerDataMap.entrySet().iterator();
            return this.closeableIterator;
        });

        if (this.exist) {
            when(this.closeableIterator.hasNext()).then(invocationOnMock -> this.iterator.hasNext());
            when(this.closeableIterator.next()).then(invocationOnMock -> this.iterator.next());
        } else {
            when(this.closeableIterator.hasNext()).thenReturn(false);
        }
    }

    protected void setLedgerDataMap(Map<byte[], byte[]> ledgerDataMap) {
        this.ledgerDataMap = ledgerDataMap;
    }

    protected KeyValueStorageFactory getKeyValueStorageFactory() {
        return keyValueStorageFactory;
    }

    protected KeyValueStorage getKeyValueStorage() {
        return keyValueStorage;
    }


}
