package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.storage.ldb.GetLedgerMetadataTest;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.GeneralSecurityException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class AsyncAddEntryTest {
    private static Random r = null;
    private static Set<Long> usedValues;
    private long ledgerId;

    private LedgerHandle ledgerHandle;
    private LedgerHandleInit handler;

    private byte[] data;
    private int offset;
    private int length;
    private AsyncCallback.AddCallback cb;
    private Object ctx;
    private boolean callBackValid;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public AsyncAddEntryTest( TestParameters input ){
        this.handler = new LedgerHandleInit(this);
        this.data = input.getData();
        this.offset = input.getOffset();
        this.length = input.getLength();
        this.callBackValid = input.isCallBackValid();
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

        inputs.add( new TestParameters( new byte[0], -1, 0, true, null));
        inputs.add( new TestParameters( "test".getBytes(), 1, "test".getBytes().length, true, null));
        inputs.add( new TestParameters( "test".getBytes(), 1, 0, true, null));
        inputs.add( new TestParameters( "test".getBytes(), 1, "test".getBytes().length + 1, true, null));
        inputs.add( new TestParameters( "test".getBytes(), 0, "test".getBytes().length, true, null));
        inputs.add( new TestParameters( "test".getBytes(), 1, "test".getBytes().length, false, null));


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

    @BeforeClass
    public static void configRandomGenerator(){
        r = new Random();
        usedValues = new HashSet<>();
    }

    @AfterClass
    public static void cleanUpEnv(){
        usedValues.clear();
    }


    @Before
    public void setup() throws GeneralSecurityException {


        byte[] password = new byte[0];
        this.ledgerHandle = new LedgerHandle( this.handler.getClientCtx(), ledgerId, this.handler.getLedgerMetadataVersioned(), this.handler.getDigestType(), password, this.handler.getWriteFlags() );
        this.cb = this.handler.getCallBack();
        this.ctx = this.handler.getCtx();
    }

    @After
    public void releaseResources(){
        usedValues.add(this.ledgerId);
    }

    @Test
    public void addEntryTest() throws Exception{

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
