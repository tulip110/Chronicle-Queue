/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

/*
 * Created by daniel on 16/05/2016.
 */
public class WriteBytesTest {
    final Bytes outgoingBytes = Bytes.elasticByteBuffer();
    private final byte[] incomingMsgBytes = new byte[100];
    private final byte[] outgoingMsgBytes = new byte[100];

    @Test
    public void testWriteBytes() {
        File dir = DirectoryUtils.tempDir("WriteBytesTest");
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();

            outgoingMsgBytes[0] = 'A';
            outgoingBytes.write(outgoingMsgBytes);
            postOneMessage(appender);
            fetchOneMessage(tailer, incomingMsgBytes);
            System.out.println(new String(incomingMsgBytes));

            outgoingBytes.clear();

            outgoingMsgBytes[0] = 'A';
            outgoingMsgBytes[1] = 'B';
            outgoingBytes.write(outgoingMsgBytes);

            postOneMessage(appender);
            fetchOneMessage(tailer, incomingMsgBytes);
            System.out.println(new String(incomingMsgBytes));

        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

    @Test
    public void testWriteBytesAndDump() {
        File dir = DirectoryUtils.tempDir("WriteBytesTestAndDump");
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .rollCycle(TEST4_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
                byte finalI = (byte) i;
                appender.writeBytes(b ->
                        b.writeLong(finalI * 0x0101010101010101L));
            }

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 4336,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 256\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 34359742704\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 2\n" +
                    "  704,\n" +
                    "  2540,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 704, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  992,\n" +
                    "  1040,\n" +
                    "  1088,\n" +
                    "  1136,\n" +
                    "  1184,\n" +
                    "  1232,\n" +
                    "  1280,\n" +
                    "  1328,\n" +
                    "  1376,\n" +
                    "  1424,\n" +
                    "  1472,\n" +
                    "  1520,\n" +
                    "  1568,\n" +
                    "  1616,\n" +
                    "  1664,\n" +
                    "  1712,\n" +
                    "  1760,\n" +
                    "  1808,\n" +
                    "  1856,\n" +
                    "  1904,\n" +
                    "  1952,\n" +
                    "  2000,\n" +
                    "  2048,\n" +
                    "  2096,\n" +
                    "  2144,\n" +
                    "  2192,\n" +
                    "  2240,\n" +
                    "  2288,\n" +
                    "  2336,\n" +
                    "  2384,\n" +
                    "  2432,\n" +
                    "  2480\n" +
                    "]\n" +
                    "# position: 992, header: 0\n" +
                    "--- !!data #binary\n" +
                    "000003e0             80 7f 7f 7f  7f 7f 7f 7f                 ···· ····    \n" +
                    "# position: 1004, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000003f0 81 80 80 80 80 80 80 80                          ········         \n" +
                    "# position: 1016, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000003f0                                      82 81 81 81              ····\n" +
                    "00000400 81 81 81 81                                      ····             \n" +
                    "# position: 1028, header: 3\n" +
                    "--- !!data #binary\n" +
                    "00000400                          83 82 82 82 82 82 82 82          ········\n" +
                    "# position: 1040, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 1052, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 1064, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x86\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 1076, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000430                          87 86 86 86 86 86 86 86          ········\n" +
                    "# position: 1088, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000440             88 87 87 87  87 87 87 87                 ···· ····    \n" +
                    "# position: 1100, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000450 89 88 88 88 88 88 88 88                          ········         \n" +
                    "# position: 1112, header: 10\n" +
                    "--- !!data #binary\n" +
                    "\"\\x89\\x89\\x89\\x89\\x89\\x89\\x89\"\n" +
                    "# position: 1124, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "\"\\x8A\\x8A\\x8A\\x8A\\x8A\\x8A\"\n" +
                    "# position: 1136, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 1148, header: 13\n" +
                    "--- !!data #binary\n" +
                    "00000480 8d 8c 8c 8c 8c 8c 8c 8c                          ········         \n" +
                    "# position: 1160, header: 14\n" +
                    "--- !!data #binary\n" +
                    "00000480                                      8e 8d 8d 8d              ····\n" +
                    "00000490 8d 8d 8d 8d                                      ····             \n" +
                    "# position: 1172, header: 15\n" +
                    "--- !!data #binary\n" +
                    "00000490                          8f 8e 8e 8e 8e 8e 8e 8e          ········\n" +
                    "# position: 1184, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-1.4156185439721035E-29\n" +
                    "# position: 1196, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-5.702071897398123E-29\n" +
                    "# # EndOfFile\n" +
                    "# position: 1208, header: 18\n" +
                    "--- !!data #binary\n" +
                    "-753555055760.82\n" +
                    "# position: 1220, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "-48698841.79\n" +
                    "# position: 1232, header: 20\n" +
                    "--- !!data #binary\n" +
                    "-8422085917.3268\n" +
                    "# position: 1244, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "-541098.2421\n" +
                    "# position: 1256, header: 22\n" +
                    "--- !!data #binary\n" +
                    "-93086212.770454\n" +
                    "# position: 1268, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "-5952.080663\n" +
                    "# position: 1280, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1292, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1304, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1316, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1328, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1340, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1352, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1364, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1376, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1388, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1400, header: 34\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int 161\n" +
                    "!int 161\n" +
                    "!int -1\n" +
                    "# position: 1412, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "!int 41634\n" +
                    "# position: 1424, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1436, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "!byte 0\n" +
                    "# position: 1448, header: 38\n" +
                    "--- !!data #binary\n" +
                    "!int -1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1460, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "!int -1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1472, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!int 167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1484, header: 41\n" +
                    "--- !!data #binary\n" +
                    "!int 43176\n" +
                    "!int 168\n" +
                    "!int 168\n" +
                    "!int -1\n" +
                    "# position: 1496, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "!int 43433\n" +
                    "!int 43433\n" +
                    "# position: 1508, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1520, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1532, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1544, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1556, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1568, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1580, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1592, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000630                                      b2 b1 b1 b1              ····\n" +
                    "00000640 b1 b1 b1 b1                                      ····             \n" +
                    "# position: 1604, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000640                          b3 b2 b2 b2 b2 b2 b2 b2          ········\n" +
                    "# position: 1616, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000650             b4 b3 b3 b3  b3 b3 b3 b3                 ···· ····    \n" +
                    "# position: 1628, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000660 b5 b4 b4 b4 b4 b4 b4 b4                          ········         \n" +
                    "# position: 1640, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000660                                      b6 b5 b5 b5              ····\n" +
                    "00000670 b5 b5 b5 b5                                      ····             \n" +
                    "# position: 1652, header: 55\n" +
                    "--- !!data #binary\n" +
                    "00000670                          b7 b6 b6 b6 b6 b6 b6 b6          ········\n" +
                    "# position: 1664, header: 56\n" +
                    "--- !!data #binary\n" +
                    "00000680             b8 b7 b7 b7  b7 b7 b7 b7                 ···· ····    \n" +
                    "# position: 1676, header: 57\n" +
                    "--- !!data #binary\n" +
                    "00000690 b9 b8 b8 b8 b8 b8 b8 b8                          ········         \n" +
                    "# position: 1688, header: 58\n" +
                    "--- !!data #binary\n" +
                    "\"-252662577519802\": \n" +
                    "# position: 1700, header: 59\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-2008556674363\": \n" +
                    "# position: 1712, header: 60\n" +
                    "--- !!data #binary\n" +
                    "000006b0             bc bb bb bb  bb bb bb bb                 ···· ····    \n" +
                    "# position: 1724, header: 61\n" +
                    "--- !!data #binary\n" +
                    "000006c0 bd bc bc bc bc bc bc bc                          ········         \n" +
                    "# position: 1736, header: 62\n" +
                    "--- !!data #binary\n" +
                    "000006c0                                      be bd bd bd              ····\n" +
                    "000006d0 bd bd bd bd                                      ····             \n" +
                    "# position: 1748, header: 63\n" +
                    "--- !!data #binary\n" +
                    "000006d0                          bf be be be be be be be          ········\n" +
                    "# position: 1760, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1772, header: 65\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC0\": \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1784, header: 66\n" +
                    "--- !!data #binary\n" +
                    "000006f0                                      c2 c1 c1 c1              ····\n" +
                    "00000700 c1 c1 c1 c1                                      ····             \n" +
                    "# position: 1796, header: 67\n" +
                    "--- !!data #binary\n" +
                    "00000700                          c3 c2 c2 c2 c2 c2 c2 c2          ········\n" +
                    "# position: 1808, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000710             c4 c3 c3 c3  c3 c3 c3 c3                 ···· ····    \n" +
                    "# position: 1820, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000720 c5 c4 c4 c4 c4 c4 c4 c4                          ········         \n" +
                    "# position: 1832, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000720                                      c6 c5 c5 c5              ····\n" +
                    "00000730 c5 c5 c5 c5                                      ····             \n" +
                    "# position: 1844, header: 71\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\": \n" +
                    "# position: 1856, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000740             c8 c7 c7 c7  c7 c7 c7 c7                 ···· ····    \n" +
                    "# position: 1868, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000750 c9 c8 c8 c8 c8 c8 c8 c8                          ········         \n" +
                    "# position: 1880, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000750                                      ca c9 c9 c9              ····\n" +
                    "00000760 c9 c9 c9 c9                                      ····             \n" +
                    "# position: 1892, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000760                          cb ca ca ca ca ca ca ca          ········\n" +
                    "# position: 1904, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000770             cc cb cb cb  cb cb cb cb                 ···· ····    \n" +
                    "# position: 1916, header: 77\n" +
                    "--- !!data #binary\n" +
                    "00000780 cd cc cc cc cc cc cc cc                          ········         \n" +
                    "# position: 1928, header: 78\n" +
                    "--- !!data #binary\n" +
                    "00000780                                      ce cd cd cd              ····\n" +
                    "00000790 cd cd cd cd                                      ····             \n" +
                    "# position: 1940, header: 79\n" +
                    "--- !!data #binary\n" +
                    "00000790                          cf ce ce ce ce ce ce ce          ········\n" +
                    "# position: 1952, header: 80\n" +
                    "--- !!data #binary\n" +
                    "000007a0             d0 cf cf cf  cf cf cf cf                 ···· ····    \n" +
                    "# position: 1964, header: 81\n" +
                    "--- !!data #binary\n" +
                    "000007b0 d1 d0 d0 d0 d0 d0 d0 d0                          ········         \n" +
                    "# position: 1976, header: 82\n" +
                    "--- !!data #binary\n" +
                    "000007b0                                      d2 d1 d1 d1              ····\n" +
                    "000007c0 d1 d1 d1 d1                                      ····             \n" +
                    "# position: 1988, header: 83\n" +
                    "--- !!data #binary\n" +
                    "000007c0                          d3 d2 d2 d2 d2 d2 d2 d2          ········\n" +
                    "# position: 2000, header: 84\n" +
                    "--- !!data #binary\n" +
                    "000007d0             d4 d3 d3 d3  d3 d3 d3 d3                 ···· ····    \n" +
                    "# position: 2012, header: 85\n" +
                    "--- !!data #binary\n" +
                    "000007e0 d5 d4 d4 d4 d4 d4 d4 d4                          ········         \n" +
                    "# position: 2024, header: 86\n" +
                    "--- !!data #binary\n" +
                    "000007e0                                      d6 d5 d5 d5              ····\n" +
                    "000007f0 d5 d5 d5 d5                                      ····             \n" +
                    "# position: 2036, header: 87\n" +
                    "--- !!data #binary\n" +
                    "000007f0                          d7 d6 d6 d6 d6 d6 d6 d6          ········\n" +
                    "# position: 2048, header: 88\n" +
                    "--- !!data #binary\n" +
                    "00000800             d8 d7 d7 d7  d7 d7 d7 d7                 ···· ····    \n" +
                    "# position: 2060, header: 89\n" +
                    "--- !!data #binary\n" +
                    "00000810 d9 d8 d8 d8 d8 d8 d8 d8                          ········         \n" +
                    "# position: 2072, header: 90\n" +
                    "--- !!data #binary\n" +
                    "00000810                                      da d9 d9 d9              ····\n" +
                    "00000820 d9 d9 d9 d9                                      ····             \n" +
                    "# position: 2084, header: 91\n" +
                    "--- !!data #binary\n" +
                    "00000820                          db da da da da da da da          ········\n" +
                    "# position: 2096, header: 92\n" +
                    "--- !!data #binary\n" +
                    "00000830             dc db db db  db db db db                 ···· ····    \n" +
                    "# position: 2108, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000840 dd dc dc dc dc dc dc dc                          ········         \n" +
                    "# position: 2120, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000840                                      de dd dd dd              ····\n" +
                    "00000850 dd dd dd dd                                      ····             \n" +
                    "# position: 2132, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000850                          df de de de de de de de          ········\n" +
                    "# position: 2144, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000860             e0 df df df  df df df df                 ···· ····    \n" +
                    "# position: 2156, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000870 e1 e0 e0 e0 e0 e0 e0 e0                          ········         \n" +
                    "# position: 2168, header: 98\n" +
                    "--- !!data #binary\n" +
                    "00000870                                      e2 e1 e1 e1              ····\n" +
                    "00000880 e1 e1 e1 e1                                      ····             \n" +
                    "# position: 2180, header: 99\n" +
                    "--- !!data #binary\n" +
                    "00000880                          e3 e2 e2 e2 e2 e2 e2 e2          ········\n" +
                    "# position: 2192, header: 100\n" +
                    "--- !!data #binary\n" +
                    "00000890             e4 e3 e3 e3  e3 e3 e3 e3                 ···· ····    \n" +
                    "# position: 2204, header: 101\n" +
                    "--- !!data #binary\n" +
                    "000008a0 e5 e4 e4 e4 e4 e4 e4 e4                          ········         \n" +
                    "# position: 2216, header: 102\n" +
                    "--- !!data #binary\n" +
                    "000008a0                                      e6 e5 e5 e5              ····\n" +
                    "000008b0 e5 e5 e5 e5                                      ····             \n" +
                    "# position: 2228, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000008b0                          e7 e6 e6 e6 e6 e6 e6 e6          ········\n" +
                    "# position: 2240, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000008c0             e8 e7 e7 e7  e7 e7 e7 e7                 ···· ····    \n" +
                    "# position: 2252, header: 105\n" +
                    "--- !!data #binary\n" +
                    "000008d0 e9 e8 e8 e8 e8 e8 e8 e8                          ········         \n" +
                    "# position: 2264, header: 106\n" +
                    "--- !!data #binary\n" +
                    "000008d0                                      ea e9 e9 e9              ····\n" +
                    "000008e0 e9 e9 e9 e9                                      ····             \n" +
                    "# position: 2276, header: 107\n" +
                    "--- !!data #binary\n" +
                    "000008e0                          eb ea ea ea ea ea ea ea          ········\n" +
                    "# position: 2288, header: 108\n" +
                    "--- !!data #binary\n" +
                    "000008f0             ec eb eb eb  eb eb eb eb                 ···· ····    \n" +
                    "# position: 2300, header: 109\n" +
                    "--- !!data #binary\n" +
                    "00000900 ed ec ec ec ec ec ec ec                          ········         \n" +
                    "# position: 2312, header: 110\n" +
                    "--- !!data #binary\n" +
                    "00000900                                      ee ed ed ed              ····\n" +
                    "00000910 ed ed ed ed                                      ····             \n" +
                    "# position: 2324, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000910                          ef ee ee ee ee ee ee ee          ········\n" +
                    "# position: 2336, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000920             f0 ef ef ef  ef ef ef ef                 ···· ····    \n" +
                    "# position: 2348, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000930 f1 f0 f0 f0 f0 f0 f0 f0                          ········         \n" +
                    "# position: 2360, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000930                                      f2 f1 f1 f1              ····\n" +
                    "00000940 f1 f1 f1 f1                                      ····             \n" +
                    "# position: 2372, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000940                          f3 f2 f2 f2 f2 f2 f2 f2          ········\n" +
                    "# position: 2384, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000950             f4 f3 f3 f3  f3 f3 f3 f3                 ···· ····    \n" +
                    "# position: 2396, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000960 f5 f4 f4 f4 f4 f4 f4 f4                          ········         \n" +
                    "# position: 2408, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000960                                      f6 f5 f5 f5              ····\n" +
                    "00000970 f5 f5 f5 f5                                      ····             \n" +
                    "# position: 2420, header: 119\n" +
                    "--- !!data #binary\n" +
                    "00000970                          f7 f6 f6 f6 f6 f6 f6 f6          ········\n" +
                    "# position: 2432, header: 120\n" +
                    "--- !!data #binary\n" +
                    "00000980             f8 f7 f7 f7  f7 f7 f7 f7                 ···· ····    \n" +
                    "# position: 2444, header: 121\n" +
                    "--- !!data #binary\n" +
                    "00000990 f9 f8 f8 f8 f8 f8 f8 f8                          ········         \n" +
                    "# position: 2456, header: 122\n" +
                    "--- !!data #binary\n" +
                    "00000990                                      fa f9 f9 f9              ····\n" +
                    "000009a0 f9 f9 f9 f9                                      ····             \n" +
                    "# position: 2468, header: 123\n" +
                    "--- !!data #binary\n" +
                    "000009a0                          fb fa fa fa fa fa fa fa          ········\n" +
                    "# position: 2480, header: 124\n" +
                    "--- !!data #binary\n" +
                    "000009b0             fc fb fb fb  fb fb fb fb                 ···· ····    \n" +
                    "# position: 2492, header: 125\n" +
                    "--- !!data #binary\n" +
                    "000009c0 fd fc fc fc fc fc fc fc                          ········         \n" +
                    "# position: 2504, header: 126\n" +
                    "--- !!data #binary\n" +
                    "000009c0                                      fe fd fd fd              ····\n" +
                    "000009d0 fd fd fd fd                                      ····             \n" +
                    "# position: 2516, header: 127\n" +
                    "--- !!data #binary\n" +
                    "000009d0                          ff fe fe fe fe fe fe fe          ········\n" +
                    "# position: 2528, header: 128\n" +
                    "--- !!data #binary\n" +
                    "000009e0             00 00 00 00  00 00 00 00                 ···· ····    \n" +
                    "# position: 2540, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2528,\n" +
                    "  2860,\n" +
                    "  2908,\n" +
                    "  2956,\n" +
                    "  3004,\n" +
                    "  3052,\n" +
                    "  3100,\n" +
                    "  3148,\n" +
                    "  3196,\n" +
                    "  3244,\n" +
                    "  3292,\n" +
                    "  3340,\n" +
                    "  3388,\n" +
                    "  3436,\n" +
                    "  3484,\n" +
                    "  3532,\n" +
                    "  3580,\n" +
                    "  3628,\n" +
                    "  3676,\n" +
                    "  3724,\n" +
                    "  3772,\n" +
                    "  3820,\n" +
                    "  3868,\n" +
                    "  3916,\n" +
                    "  3964,\n" +
                    "  4012,\n" +
                    "  4060,\n" +
                    "  4108,\n" +
                    "  4156,\n" +
                    "  4204,\n" +
                    "  4252,\n" +
                    "  4300\n" +
                    "]\n" +
                    "# position: 2824, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000b00                                      01 01 01 01              ····\n" +
                    "00000b10 01 01 01 01                                      ····             \n" +
                    "# position: 2836, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000b10                          02 02 02 02 02 02 02 02          ········\n" +
                    "# position: 2848, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000b20             03 03 03 03  03 03 03 03                 ···· ····    \n" +
                    "# position: 2860, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000b30 04 04 04 04 04 04 04 04                          ········         \n" +
                    "# position: 2872, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000b30                                      05 05 05 05              ····\n" +
                    "00000b40 05 05 05 05                                      ····             \n" +
                    "# position: 2884, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000b40                          06 06 06 06 06 06 06 06          ········\n" +
                    "# position: 2896, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000b50             07 07 07 07  07 07 07 07                 ···· ····    \n" +
                    "# position: 2908, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000b60 08 08 08 08 08 08 08 08                          ········         \n" +
                    "# position: 2920, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000b60                                      09 09 09 09              ····\n" +
                    "00000b70 09 09 09 09                                      ····             \n" +
                    "# position: 2932, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2944, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000b80             0b 0b 0b 0b  0b 0b 0b 0b                 ···· ····    \n" +
                    "# position: 2956, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000b90 0c 0c 0c 0c 0c 0c 0c 0c                          ········         \n" +
                    "# position: 2968, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000b90                                      0d 0d 0d 0d              ····\n" +
                    "00000ba0 0d 0d 0d 0d                                      ····             \n" +
                    "# position: 2980, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000ba0                          0e 0e 0e 0e 0e 0e 0e 0e          ········\n" +
                    "# position: 2992, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000bb0             0f 0f 0f 0f  0f 0f 0f 0f                 ···· ····    \n" +
                    "# position: 3004, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000bc0 10 10 10 10 10 10 10 10                          ········         \n" +
                    "# position: 3016, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000bc0                                      11 11 11 11              ····\n" +
                    "00000bd0 11 11 11 11                                      ····             \n" +
                    "# position: 3028, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000bd0                          12 12 12 12 12 12 12 12          ········\n" +
                    "# position: 3040, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000be0             13 13 13 13  13 13 13 13                 ···· ····    \n" +
                    "# position: 3052, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000bf0 14 14 14 14 14 14 14 14                          ········         \n" +
                    "# position: 3064, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000bf0                                      15 15 15 15              ····\n" +
                    "00000c00 15 15 15 15                                      ····             \n" +
                    "# position: 3076, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000c00                          16 16 16 16 16 16 16 16          ········\n" +
                    "# position: 3088, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000c10             17 17 17 17  17 17 17 17                 ···· ····    \n" +
                    "# position: 3100, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000c20 18 18 18 18 18 18 18 18                          ········         \n" +
                    "# position: 3112, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000c20                                      19 19 19 19              ····\n" +
                    "00000c30 19 19 19 19                                      ····             \n" +
                    "# position: 3124, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000c30                          1a 1a 1a 1a 1a 1a 1a 1a          ········\n" +
                    "# position: 3136, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000c40             1b 1b 1b 1b  1b 1b 1b 1b                 ···· ····    \n" +
                    "# position: 3148, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000c50 1c 1c 1c 1c 1c 1c 1c 1c                          ········         \n" +
                    "# position: 3160, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000c50                                      1d 1d 1d 1d              ····\n" +
                    "00000c60 1d 1d 1d 1d                                      ····             \n" +
                    "# position: 3172, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000c60                          1e 1e 1e 1e 1e 1e 1e 1e          ········\n" +
                    "# position: 3184, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000c70             1f 1f 1f 1f  1f 1f 1f 1f                 ···· ····    \n" +
                    "# position: 3196, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 3208, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 3220, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3232, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3244, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3256, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3268, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3280, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3292, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3304, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3316, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3328, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3340, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3352, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3364, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3376, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3388, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3400, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3412, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3424, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3436, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3448, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3460, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3472, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3484, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3496, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3508, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3520, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3532, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3544, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3556, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3568, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3580, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3592, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3604, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3616, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3628, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3640, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3652, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3664, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3676, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3688, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3700, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3712, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3724, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3736, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3748, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3760, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3772, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3784, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3796, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3808, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3820, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3832, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3844, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3856, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3868, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3880, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3892, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3904, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3916, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3928, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3940, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3952, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3964, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 3976, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 3988, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 4000, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 4012, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 4024, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 4036, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 4048, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 4060, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 4072, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 4084, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 4096, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 4108, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 4120, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 4132, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 4144, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 4156, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 4168, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 4180, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 4192, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 4204, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 4216, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4228, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4240, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4252, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4264, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4276, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4288, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4300, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4312, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4324, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4336, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    "# 126720 bytes remaining\n", queue.dump());

        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

    public boolean postOneMessage(@NotNull ExcerptAppender appender) {
        appender.writeBytes(outgoingBytes);
        return true;
    }

    public int fetchOneMessage(@NotNull ExcerptTailer tailer, @NotNull byte[] using) {
        try (DocumentContext dc = tailer.readingDocument()) {
            return !dc.isPresent() ? -1 : dc.wire().bytes().read(using);
        }
    }

    @After
    public void checkRegisteredBytes() {
        outgoingBytes.release();
        BytesUtil.checkRegisteredBytes();
    }
} 