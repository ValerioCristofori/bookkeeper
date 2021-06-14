package org.apache.bookkeeper.bookie.storage.ldb;


import com.google.protobuf.ByteString;
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

import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class SetLedgerMetadataTest extends LedgerMetadataIndexInit{
    private LedgerMetadataIndex ledgerMetadataIndex;
    private Long ledgerId;
    private boolean exist;
    private DbLedgerStorageDataFormats.LedgerData ledgerData;
    private byte[] ledgerIdByte;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
	public SetLedgerMetadataTest(TestParameters input) {
		super(input.exist());
		this.ledgerId = input.getLedgerId();
        this.exist = input.exist();
        this.ledgerData = input.getLedgerData();
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
            masterKey: {valid}, {empty}
         */
        inputs.add(new TestParameters((long) -1, false, null, NullPointerException.class));
        inputs.add(new TestParameters(
                (long) 0,
                true,
                DbLedgerStorageDataFormats.LedgerData.newBuilder()
                        .setExists(true)
                        .setFenced(false)
                        .setMasterKey(ByteString.copyFrom("test".getBytes()))
                        .build(),
        		null));
        inputs.add(new TestParameters(
                (long) 1,
                false,
                DbLedgerStorageDataFormats.LedgerData.newBuilder()
                        .setExists(true)
                        .setFenced(false)
                        .setMasterKey(ByteString.copyFrom("test".getBytes()))
                        .build(),
                null));


        return inputs;
    }
	
	
	private static class TestParameters{
        private Long ledgerId;
        private DbLedgerStorageDataFormats.LedgerData ledgerData;
        private boolean exist;
        private Class<? extends Exception> expectedException;

        public TestParameters( long ledgerId,boolean exist, DbLedgerStorageDataFormats.LedgerData ledgerData, Class<? extends Exception> expectedException ){
            this.ledgerId = ledgerId;
            this.ledgerData = ledgerData;
            this.exist = exist;
            this.expectedException = expectedException;
        }

        public Long getLedgerId() {
            return ledgerId;
        }  

        public DbLedgerStorageDataFormats.LedgerData getLedgerData() {
			return ledgerData;
		}

		public boolean exist() {
            return exist;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }
	
	@Before
    public void setup() throws IOException {
        Map<byte[], byte[]> ledgerDataMap = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(this.ledgerId);
        this.ledgerIdByte = buffer.array();

        if (this.exist) {
            this.ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.EMPTY).build();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }
        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fooPath", new NullStatsLogger());

    }

    @Test
    public void setLedgerMetadataTest() throws Exception {
        this.ledgerMetadataIndex.set(this.ledgerId, this.ledgerData);

        this.ledgerMetadataIndex.flush();

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertEquals(Arrays.toString(this.ledgerData.toByteArray()), Arrays.toString(actualLedgerData.toByteArray()));

        verify(super.getKeyValueStorage()).put(this.ledgerIdByte, this.ledgerData.toByteArray());
    }

}
