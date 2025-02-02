package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class DeleteLedgerMetadataTest extends LedgerMetadataIndexInit{
	private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;
    
    public DeleteLedgerMetadataTest(TestParameters input) {
        super(input.exist());
        this.ledgerId = input.getLedgerId();
        this.exist = input.exist();
    }
    
    @Parameterized.Parameters
    public static Collection<TestParameters> getTestParameters() {
        List<TestParameters> inputs = new ArrayList<>();

        inputs.add(new TestParameters((long) -1, false));
        inputs.add(new TestParameters( 0, true));
        inputs.add(new TestParameters(1L, true));

        return inputs;
    }
    
    private static class TestParameters{
        private Long ledgerId;
        private boolean exist;

        public TestParameters( long ledgerId,boolean exist){
            this.ledgerId = ledgerId;
            this.exist = exist;
        }

        public Long getLedgerId() {
            return ledgerId;
        }  

		public boolean exist() {
            return exist;
        }
    }
    
    @Before
    public void setup() throws IOException {
        Map<byte[], byte[]> ledgerDataMap = new HashMap<>();
        this.ledgerIdByte = ByteBuffer.allocate(Long.BYTES).putLong(this.ledgerId).array();

        if (this.exist) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.EMPTY).build();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fakePath", new NullStatsLogger());
    }

    @Test(expected = Bookie.NoLedgerException.class)
    public void setLedgerMetadataTest() throws Exception {
        this.ledgerMetadataIndex.delete(this.ledgerId);
        this.ledgerMetadataIndex.flush();

        //verifico che non sia mai stato inserita la entry nel db mockato
        verify(super.getKeyValueStorage(), never()).put(eq(this.ledgerIdByte), any());
        this.ledgerMetadataIndex.removeDeletedLedgers();
        // verifico che e' stato eliminato dal db
        verify(super.getKeyValueStorage()).delete(this.ledgerIdByte);
        //chiamo la get per verificare che il ledger e' stato cancellato -> NoLedgerException
        this.ledgerMetadataIndex.get(this.ledgerId);
    }
    
    
}
