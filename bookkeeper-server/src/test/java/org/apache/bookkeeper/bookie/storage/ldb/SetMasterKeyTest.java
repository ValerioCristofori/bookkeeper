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
public class SetMasterKeyTest extends LedgerMetadataIndexInit{

    private Long ledgerId;
    private byte[] ledgerIdByte;
    private byte[] masterKey;
    private byte[] prevMasterKey;
    private LedgerMetadataIndex ledgerMetadataIndex;
    private boolean exist;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public SetMasterKeyTest( TestParameters input ) {
        super(input.exist());
        this.ledgerId = input.getLedgerId();
        this.masterKey = input.getMasterKey();
        this.prevMasterKey = input.getPrevMasterKey();
        this.exist = input.exist();
        Class<? extends Exception> expectedException = input.getExpectedException();
        if (expectedException != null) {
            this.expectedException.expect(expectedException);
        }
    }

    @Parameterized.Parameters
    public static Collection<TestParameters> getTestParameters(){
        List<TestParameters> listInput = new ArrayList<>();
        /*
            ledgerId: {>0}, {=0}, {<0}
            exist: {true}, {false}
            masterKey: {valid}, {empty}
         */
        listInput.add(new TestParameters((long) -1, false, "masterKey".getBytes(), new byte[0],null));
        listInput.add(new TestParameters((long) 0, true, new byte[0], new byte[0],null));
        listInput.add(new TestParameters((long) 1, true, "masterKey".getBytes(), new byte[0],null));
        listInput.add(new TestParameters((long) 1, true, new byte[0], "masterKey".getBytes(),null));
        listInput.add(new TestParameters((long) 1, true, "anotherMasterKey".getBytes(), "masterKey".getBytes(),IOException.class));
        listInput.add(new TestParameters((long) 1, true, "masterKey".getBytes(), "masterKey".getBytes(),null));

        return listInput;
    }

    private static class TestParameters{
        private Long ledgerId;
        private byte[] masterKey;
        private byte[] prevMasterKey;
        private boolean exist;
        private Class<? extends Exception> expectedException;

        public TestParameters( long ledgerId,boolean exist, byte[] masterKey, byte[] prevMasterKey, Class<? extends Exception> expectedException ){
            this.ledgerId = ledgerId;
            this.masterKey = masterKey;
            this.prevMasterKey = prevMasterKey;
            this.exist = exist;
            this.expectedException = expectedException;
        }

        public Long getLedgerId() {
            return ledgerId;
        }

        public byte[] getMasterKey() {
            return masterKey;
        }

        public byte[] getPrevMasterKey() {
            return prevMasterKey;
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
        this.ledgerIdByte = ByteBuffer.allocate(Long.BYTES)
                .putLong(this.ledgerId)
                .array();

        if (this.exist) {
            DbLedgerStorageDataFormats.LedgerData ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(this.prevMasterKey)).build();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fooPath", new NullStatsLogger());

    }

    @Test
    public void setMasterKeyTest() throws Exception {
        this.ledgerMetadataIndex.setMasterKey(this.ledgerId, this.masterKey);
        this.ledgerMetadataIndex.flush();

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData);

        String newMasterKey = Arrays.toString(this.masterKey);
        String oldMasterKey = Arrays.toString(this.prevMasterKey);

        String expectedMasterKey = null;
        byte[] expectedMasterKeyBytes = null;
        if (this.prevMasterKey.length == 0 || oldMasterKey.equals(newMasterKey)) {
            expectedMasterKey = newMasterKey;
            expectedMasterKeyBytes = this.masterKey;
        } else {
            expectedMasterKey = oldMasterKey;
            expectedMasterKeyBytes = this.prevMasterKey;
        }

        Assert.assertEquals(expectedMasterKey, Arrays.toString(actualLedgerData.getMasterKey().toByteArray()));

        DbLedgerStorageDataFormats.LedgerData expectedLegerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(expectedMasterKeyBytes)).build();
        verify(super.getKeyValueStorage()).put(this.ledgerIdByte, expectedLegerData.toByteArray());


    }

}
