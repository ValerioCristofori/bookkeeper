package org.apache.bookkeeper.proto.checksum;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class ComputeDigestTest {

    private ByteBuf data;
    private long lastAddConfirmed;
    private static long entryId;
    private long length;
    private DigestType type;
    private Object result;
    private DigestManager digestManager;
    private ByteBuf testEntry;
    private boolean useV2Protocol;

    private static long ledgerId;
    private static long lac;

    public ComputeDigestTest(TestParameters input){
        this.data = input.getData();
        this.lastAddConfirmed = input.getLastAddConfirmed();
        this.entryId = input.getEntryId();
        this.length = input.getLength();
        this.type = input.getType();
        this.result = input.getResult();
        this.useV2Protocol = input.isUseV2Protocol();
    }

    @Parameterized.Parameters
    public static Collection BufferedChannelParameters() throws Exception {
        List<TestParameters> inputs = new ArrayList<>();

        inputs.add(new TestParameters(null, -1, -1, 0, DigestType.HMAC,true, NullPointerException.class));
        inputs.add(new TestParameters(generateEntry(1), 1, 2, 1, DigestType.CRC32, true, 0));
        inputs.add(new TestParameters(generateEntry(0), 0, 2, 0, DigestType.CRC32C, false, 0));
        inputs.add(new TestParameters(generateEntry(1), 1, 2, 1, DigestType.DUMMY, true, 0));

        return inputs;
    }

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();


    public static class TestParameters{
        private ByteBuf data;
        private long lastAddConfirmed;
        private long entryId;
        private long length;
        private DigestType type;
        private Object result;
        private boolean useV2Protocol;

        TestParameters(ByteBuf data, long lastAddConfirmed, long entryId, long length, DigestType type, boolean useV2Protocol, Object result) {
            this.data = data;
            this.lastAddConfirmed = lastAddConfirmed;
            this.entryId = entryId;
            this.length = length;
            this.type = type;
            this.result = result;
            this.useV2Protocol = useV2Protocol;
        }

        public ByteBuf getData() {
            return data;
        }

        public long getLastAddConfirmed() {
            return lastAddConfirmed;
        }

        public long getEntryId() {
            return entryId;
        }

        public long getLength() {
            return length;
        }

        public DigestType getType() {
            return type;
        }

        public Object getResult() {
            return result;
        }

        public boolean isUseV2Protocol() {
            return useV2Protocol;
        }
    }


    @Before
    public void setUp() throws GeneralSecurityException {
        //instanzio un oggetto Digest Manager
        digestManager = DigestManager.instantiate(1, "testPassword".getBytes(), type, UnpooledByteBufAllocator.DEFAULT, useV2Protocol);

        testEntry = generateEntry((int)length);
    }

    @Test
    public void testComputeDigestData() {

        try {
            // controllo che il buffer contenuto in byteBuf sia uguale a quello inviato
            ByteBufList byteBuf = digestManager.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length, data);
            Assert.assertEquals(testEntry.readLong(), byteBuf.getBuffer(1).readLong());

        } catch (Exception e) {
            // controllo se il risultato e' l'eccezione che mi aspettavo
            Assert.assertEquals(result, e.getClass());
        }
    }

    private static ByteBuf generateEntry(int length) {
        byte[] data = new byte[length];
        ByteBuf byteBuffer = Unpooled.buffer(1024);
        byteBuffer.writeLong(ledgerId); // Ledger
        byteBuffer.writeLong(entryId); // Entry
        byteBuffer.writeLong(lac); // LAC
        byteBuffer.writeLong(length); // Length
        byteBuffer.writeBytes(data);
        return byteBuffer;
    }

}
