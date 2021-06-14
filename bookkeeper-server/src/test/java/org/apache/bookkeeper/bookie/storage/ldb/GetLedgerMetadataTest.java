package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

@RunWith(Parameterized.class)
public class GetLedgerMetadataTest extends LedgerMetadataIndexInit{
	private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private boolean ledgerInDb;
    private boolean deleted;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
    public GetLedgerMetadataTest(TestParameters input) {
        super(input.exist());
        this.ledgerId = input.getLedgerId();
        this.exist = input.exist();
        this.ledgerInDb = input.isLedgerInDb();
        this.deleted = input.isDeleted();
        Class<? extends Exception> expectedException = input.getExpectedException();
        if (expectedException != null) {
            this.expectedException.expect(expectedException);
        }
    }
    
    @Parameterized.Parameters
    public static Collection<TestParameters> getTestParameters() {
        List<TestParameters> inputs = new ArrayList<>();

        /*
            ledgerId: {>0}, {=0}, {<0}
            exist: {true}, {false}
            ledgerInDb: {true}, {false}
         */
        inputs.add(new TestParameters((long) -1, false, false, false, Bookie.NoLedgerException.class));
        inputs.add(new TestParameters(0L, true, false, true, Bookie.NoLedgerException.class));
        inputs.add(new TestParameters(1L, true, true, false, null));

        return inputs;
    }
    
    private static class TestParameters {
        private Long ledgerId;
        private boolean exist;
        private boolean ledgerInDb;
        private boolean deleted;
        private Class<? extends Exception> expectedException;


        public TestParameters(Long ledgerId, boolean exist, boolean ledgerInDb, boolean deleted, Class<? extends Exception> expectedException) {
            this.ledgerId = ledgerId;
            this.exist = exist;
            this.deleted = deleted;
            this.ledgerInDb = ledgerInDb;
            this.expectedException = expectedException;

        }

        public Long getLedgerId() {
            return ledgerId;
        }

        public boolean exist() {
            return exist;
        }

        public boolean isLedgerInDb() {
            return ledgerInDb;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }
    @Before
    public void setup() throws IOException {
        Map<byte[], byte[]> ledgerDataMap = new HashMap<>();
        if (this.exist && this.ledgerInDb) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true).setMasterKey(ByteString.EMPTY).build();
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(this.ledgerId);
            this.ledgerIdByte = buffer.array();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fooPath", new NullStatsLogger());
    }

    @Test
    public void getTest() throws Exception {
        try {
            if (this.exist && !this.ledgerInDb) {
                this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(true).setMasterKey(ByteString.EMPTY).build();
                this.ledgerMetadataIndex.set(this.ledgerId, this.ledgerData);
            }

            if (this.deleted) {
                this.ledgerMetadataIndex.delete(this.ledgerId);
            }

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        Assert.assertEquals(Arrays.toString(this.ledgerData.toByteArray()), Arrays.toString(actualLedgerData.toByteArray()));

    }
    
}
