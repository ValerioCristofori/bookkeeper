package org.apache.bookkeeper.proto.checksum;


import java.security.GeneralSecurityException;
import java.util.*;

import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class VerifyDigestTest {
    private Object result;
    private static final int LENGTH = 5;
    private int entryId;
    private int ledgerId;
    private DigestType type;
    private ByteBufList receivedData;
    private DigestManager digestManager;
    private ByteBuf mineByteBuf;

    public VerifyDigestTest(TestParameters input){
        this.entryId = input.getEntryId();
        this.ledgerId = input.getLedgerId();
        this.type = input.getType();
        this.receivedData = input.getReceivedData();
        this.result = input.getResult();

    }

    @Parameterized.Parameters
    public static Collection<TestParameters> BufferedChannelParameters() throws Exception {
        List<TestParameters> inputs = new ArrayList<>();

        // digest verificato
        inputs.add(new TestParameters(1, 1,  DigestType.CRC32, generateDataWithDigest(1, 1, DigestType.CRC32), 0));

        // verifica non avvenuta
        //entry id no match
        inputs.add(new TestParameters(0, 0, DigestType.HMAC,  generateDataWithDigest(0, 1, DigestType.HMAC), BKDigestMatchException.class));
        inputs.add(new TestParameters(1, 1,  DigestType.CRC32, generateDataWithDigest(1, 0, DigestType.CRC32), BKDigestMatchException.class));
        //ledger id no match
        inputs.add(new TestParameters(-1, 1,  DigestType.DUMMY, generateDataWithDigest(1, 1, DigestType.DUMMY), BKDigestMatchException.class));
        //digest type no match
        inputs.add(new TestParameters(1, 1,  DigestType.CRC32, generateDataWithDigest(1, 1, DigestType.HMAC), BKDigestMatchException.class));
        inputs.add(new TestParameters(1, 1,  DigestType.CRC32C, generateDataWithDigest(1, 1, DigestType.HMAC), BKDigestMatchException.class));

        // verifica non avvenuta: pacchetto ricevuto header troppo piccolo
        inputs.add(new TestParameters(1, 1,  DigestType.CRC32C, generateBadDataWithDigest(), BKDigestMatchException.class));


        return inputs;
    }

    public static class TestParameters{
        private int entryId;
        private int ledgerId;
        private DigestType type;
        private ByteBufList receivedData;
        private Object result;

        TestParameters(int ledgerId, int entryId, DigestType type, ByteBufList received, Object result) {
            this.entryId = entryId;
            this.ledgerId = ledgerId;
            this.type = type;
            this.receivedData = received;
            this.result = result;
        }

        public int getEntryId() {
            return entryId;
        }

        public int getLedgerId() {
            return ledgerId;
        }

        public DigestType getType() {
            return type;
        }

        public ByteBufList getReceivedData() {
            return receivedData;
        }

        public Object getResult() {
            return result;
        }
    }

    @Before
    public void beforeTest() throws GeneralSecurityException {

        digestManager = DigestManager.instantiate(ledgerId, "testPassword".getBytes(), type, UnpooledByteBufAllocator.DEFAULT, false);
        // buffer con dati per la verifica
        mineByteBuf = generateByteBuf();
    }

    @Test
    public void testVerifyDigestData(){

        try {
            Assert.assertEquals(mineByteBuf, digestManager.verifyDigestAndReturnData(entryId, receivedData.coalesce(receivedData)));
        } catch (Exception e) {

            Assert.assertEquals(result, e.getClass());
        }
    }


    private static ByteBufList generateDataWithDigest(int receivedLedgerId, int receivedEntryId, DigestType receivedType) throws GeneralSecurityException {
        DigestManager digest = DigestManager.instantiate(receivedLedgerId, "testPassword".getBytes(), receivedType, UnpooledByteBufAllocator.DEFAULT, false);
        ByteBuf byteBuf = generateByteBuf(); //genero i dati
        ByteBufList byteBufList = digest.computeDigestAndPackageForSending(receivedEntryId, 0,  LENGTH, byteBuf);
        return byteBufList;
    }

    private static ByteBufList generateBadDataWithDigest(){
        ByteBuf byteBuf = generateByteBuf();
        ByteBuf badHeader = Unpooled.buffer(LENGTH);
        return ByteBufList.get(badHeader, byteBuf);
    }

    private static ByteBuf generateByteBuf() {
        byte[] data = "testString".getBytes();
        ByteBuf byteBuf = Unpooled.buffer(27+LENGTH); //4 byte
        byteBuf.writeBytes(data);
        return byteBuf;
    }
}



