package org.apache.bookkeeper.bookie.storage.ldb;

import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.when;

public class LedgerMetadataIndexInit {
    private Map<byte[], byte[]> ledgerDataMap;
    private Iterator<Map.Entry<byte[], byte[]>> iterator;
    private boolean exist;


    @Mock
    private KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator;

    @Mock
    private KeyValueStorageFactory keyValueStorageFactory; // factory responsabile di instanziare KeyValueStorage

    @Mock
    private KeyValueStorage keyValueStorage;

    // stubbing warning e hints printati con System.out
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
        }); // setup dei dati del db chiave valore con la mappa: K e' l'id del ledger, V sono i metadati

        // setup dell'iteratore mockato
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
