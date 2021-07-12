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
        // no metadata exist in ledger index
        listInput.add(new TestParameters(-1, false, "masterKey".getBytes(), new byte[0],null));

        // empty master key
        listInput.add(new TestParameters( 0, true, new byte[0], new byte[0],null));
        listInput.add(new TestParameters( 1, true, new byte[0], "masterKey".getBytes(),null));

        // valid master key
        listInput.add(new TestParameters( 1, true, "masterKey".getBytes(), new byte[0],null));
        listInput.add(new TestParameters( 1, true, "anotherMasterKey".getBytes(), "masterKey".getBytes(),IOException.class));
        listInput.add(new TestParameters( 1, true, "masterKey".getBytes(), "masterKey".getBytes(),null));

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
        Map<byte[], byte[]> ledgerDataMap = new HashMap<>(); // chiave e' il ledger id, valore sono i metadati per quel ledger
        this.ledgerIdByte = ByteBuffer.allocate(Long.BYTES)
                .putLong(this.ledgerId)
                .array();

        if (this.exist) {
            // esiste gia' un master key
            // aggiungo il prev master key ai metadati
            DbLedgerStorageDataFormats.LedgerData ledgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(this.prevMasterKey)).build();
            ledgerDataMap.put(ledgerIdByte, ledgerData.toByteArray());
            super.setLedgerDataMap(ledgerDataMap);
        }

        this.ledgerMetadataIndex = new LedgerMetadataIndex(new ServerConfiguration(), super.getKeyValueStorageFactory(), "fooPath", new NullStatsLogger());

    }

    @Test
    public void setMasterKeyTest() throws Exception {
        this.ledgerMetadataIndex.setMasterKey(this.ledgerId, this.masterKey);
        this.ledgerMetadataIndex.flush(); //flush di tutti i cambiamenti non effettuati e in stato pending

        DbLedgerStorageDataFormats.LedgerData actualLedgerData = this.ledgerMetadataIndex.get(this.ledgerId);

        Assert.assertNotNull(actualLedgerData); //fallisci se null

        String newMasterKey = Arrays.toString(this.masterKey);
        String oldMasterKey = Arrays.toString(this.prevMasterKey);

        String expectedMasterKey;
        byte[] expectedMasterKeyBytes;
        // imposto il valore del primo master key che mi aspetto di leggere
        if (this.prevMasterKey.length == 0 || oldMasterKey.equals(newMasterKey)) {
            expectedMasterKey = newMasterKey;
            expectedMasterKeyBytes = this.masterKey;
        } else {
            expectedMasterKey = oldMasterKey;
            expectedMasterKeyBytes = this.prevMasterKey;
        }
        // controllo se il valore e' uguale a quello aspettato
        Assert.assertEquals(expectedMasterKey, Arrays.toString(actualLedgerData.getMasterKey().toByteArray()));
        // costruisco i metadati da passare alla verifica
        DbLedgerStorageDataFormats.LedgerData expectedLedgerData = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(expectedMasterKeyBytes)).build();
        // test passa se la put e' stata chiamata una e una solo volta con i parametri specificati
        verify(super.getKeyValueStorage()).put(this.ledgerIdByte, expectedLedgerData.toByteArray());


    }

}
