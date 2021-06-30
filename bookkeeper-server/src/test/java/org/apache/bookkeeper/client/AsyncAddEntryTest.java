package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.storage.ldb.GetLedgerMetadataTest;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndexInit;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.GeneralSecurityException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class AsyncAddEntryTest extends LedgerHandleInit{


    private LedgerHandle ledgerHandle;

    private byte[] data;
    private int offset;
    private int length;
    private AsyncCallback.AddCallback cb;
    private Object ctx;
    private boolean callBackValid;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public AsyncAddEntryTest( TestParameters input ){
        super();
        this.data = input.getData();
        this.offset = input.getOffset();
        this.length = input.getLength();
        this.callBackValid = input.isCallBackValid();
        Class<? extends Exception> expectedException = input.getExpectedException();
        if (expectedException != null) {
            this.expectedException.expect(expectedException);
        }
    }

    @Parameterized.Parameters
    public static Collection<TestParameters> getTestParameters() {
        List<TestParameters> inputs = new ArrayList<>();

        /*
            data: {0}, {not empty}
            offset: {-1}, {0}, {not valid, entry filled}, {valid}
            length: {true}, {false}
            cb valid: {true}, {false}

         */

        inputs.add( new TestParameters( new byte[0], -1, 0, true, ArrayIndexOutOfBoundsException.class));
        inputs.add( new TestParameters( "test".getBytes(), 0, "test".getBytes().length, true, null));
        inputs.add( new TestParameters( "test".getBytes(), -1, "test".getBytes().length + 2, true, ArrayIndexOutOfBoundsException.class));
        inputs.add( new TestParameters( "test".getBytes(), 1, "test".getBytes().length + 1, true, ArrayIndexOutOfBoundsException.class));
        inputs.add( new TestParameters( "test".getBytes(), 0, "test".getBytes().length - 1, false, null));
        inputs.add( new TestParameters( "test".getBytes(), 1, "test".getBytes().length, false, ArrayIndexOutOfBoundsException.class));


        return inputs;
    }

    private static class TestParameters{
        private byte[] data;
        private int offset;
        private int length;
        private boolean callBackValid;
        private Class<? extends Exception> expectedException;

        public TestParameters(byte[] data, int offset, int length,
        boolean callBackValid, Class<? extends Exception> expectedException){
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.callBackValid = callBackValid;
            this.expectedException = expectedException;
        }

        public byte[] getData() {
            return data;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }

        public boolean isCallBackValid() {
            return callBackValid;
        }

        public Class<? extends Exception> getExpectedException() {
            return expectedException;
        }
    }

    @Before
    public void setup() throws GeneralSecurityException {

        byte[] password = new byte[0];
        this.ledgerHandle = new LedgerHandle( super.getClientCtx(), super.getLedgerId(), super.getLedgerMetadataVersioned(), BookKeeper.DigestType.CRC32, password, WriteFlag.NONE );
        this.cb = new AsyncCallback.AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                LedgerMetadataIndexInit.MySync sync = (LedgerMetadataIndexInit.MySync) ctx;
                sync.setReturnCode(rc);
                synchronized (sync) {
                    sync.counter++;
                    sync.notify();
                }
            }
        };
        this.ctx = null;
    }

    @Test
    public void addEntryTest() throws Exception{
        try {
            //call method
            this.ledgerHandle.asyncAddEntry(this.data,this.offset, this.length, this.cb, this.ctx);

            //check the result -> modifications done
            //check for the first entry
            Enumeration<LedgerEntry> enumEntries = this.ledgerHandle.readEntries(0,0);

            while(enumEntries.hasMoreElements() ){
                byte[] actualBytes = enumEntries.nextElement().getEntry();
                byte[] expectedBytes = Arrays.copyOfRange( this.data, 0, this.data.length );

                String actual = new String( actualBytes, "UTF-8");
                String expected = new String( expectedBytes, "UTF-8");

                assertEquals(actual,expected);
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }



}
