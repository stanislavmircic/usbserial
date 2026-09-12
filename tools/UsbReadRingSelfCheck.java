// A SELF-CHECK FOR UsbReadRing, ON A PLAIN JAVA VIRTUAL MACHINE.
//
// UsbReadRing has no Android imports on purpose, so its behaviour can be driven
// here with real threads instead of only reasoned about. This file is NOT part
// of the Android library build — it lives outside android/src so Gradle never
// sees it. Run it by hand:
//
//   javac -d /tmp/ring \
//       android/src/main/java/dev/bessems/usbserial/UsbReadRing.java \
//       tools/UsbReadRingSelfCheck.java
//   java -cp /tmp/ring dev.bessems.usbserial.UsbReadRingSelfCheck
//
// It checks, with two threads and a scripted stream of known bytes:
//
//   P3  nothing is lost: every byte in comes out, in order, at the fastest rate
//       the application supports, long enough to wrap the buffer more than twice
//   P4  the pieces handed up vary in size and average far more than one packet
//   P5  a late drain loses nothing while the buffer has room, and when it fills
//       the buffer OVERWRITES ITSELF — the oldest bytes go, the newest survive
//       in order — which is his ruling and what any circular buffer does
//   P8  every open clears head and tail, so nothing read at the wrong rate
//       survives the reset — also his ruling
//   and it prints the measurement: reads a second, hand-offs a second, and the
//       average piece size.
//
// Every failure throws, so a non-zero exit means a real fault.

package dev.bessems.usbserial;

import java.util.ArrayList;
import java.util.List;

public final class UsbReadRingSelfCheck {

    // The fastest board the application supports: 44100 samples a second,
    // 2 channels, 2 bytes a sample.
    static final int FASTEST_BYTES_PER_SECOND = 44100 * 2 * 2; // 176400

    // One USB packet as both chips report it, minus felhr's two FTDI status
    // bytes: what one read actually returns.
    static final int PACKET_PAYLOAD = 62;

    static final int DRAIN_INTERVAL_MS = 50;

    public static void main(String[] args) throws Exception {
        p3NothingIsLostAcrossMoreThanTwoWraps();
        p5LateDrainLosesNothingWhileThereIsRoom();
        p5FullBufferLosesTheOldestAndNothingElse();
        p8EveryOpenClearsHeadAndTail();
        System.out.println("ALL CHECKS PASSED");
    }

    // -----------------------------------------------------------------------
    // P3 + P4 + the measurement.
    // -----------------------------------------------------------------------

    static void p3NothingIsLostAcrossMoreThanTwoWraps() throws Exception {
        // Two and a half times the buffer, so the write index wraps twice and
        // starts a third pass.
        final long totalBytes = (long) (UsbReadRing.CAPACITY_BYTES * 2.5);
        final UsbReadRing ring = new UsbReadRing();

        // A stream of KNOWN bytes: byte i of the stream is (i * 31 + 7) & 0xFF,
        // which is a full-period sequence over 256, so any dropped, duplicated
        // or reordered byte shows up as a mismatch at a known offset.
        final long[] readCount = new long[1];
        final Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                final byte[] packet = new byte[PACKET_PAYLOAD];
                long produced = 0;
                final long startNanos = System.nanoTime();
                while (produced < totalBytes) {
                    final int n = (int) Math.min(PACKET_PAYLOAD, totalBytes - produced);
                    for (int i = 0; i < n; i++) {
                        packet[i] = streamByte(produced + i);
                    }
                    // Pace the producer at the fastest board's real byte rate,
                    // so the drain has the same amount of time it has in the
                    // application. Busy-wait on the clock: Thread.sleep cannot
                    // resolve the 0.35 ms one packet takes at this rate.
                    final long dueNanos = startNanos
                            + (produced + n) * 1_000_000_000L / FASTEST_BYTES_PER_SECOND;
                    while (System.nanoTime() < dueNanos) {
                        Thread.onSpinWait();
                    }
                    ring.write(packet, n);
                    produced += n;
                    readCount[0]++;
                }
            }
        }, "reader");

        final List<Integer> pieceSizes = new ArrayList<>();
        final long[] checked = new long[1];
        final boolean[] running = new boolean[] { true };
        final Thread drain = new Thread(new Runnable() {
            @Override
            public void run() {
                while (running[0] || ring.writtenBytes() > ring.drainedBytes()) {
                    try {
                        Thread.sleep(DRAIN_INTERVAL_MS);
                    } catch (InterruptedException e) {
                        break;
                    }
                    final byte[] piece = ring.drain();
                    if (piece == null) {
                        continue;
                    }
                    pieceSizes.add(piece.length);
                    for (int i = 0; i < piece.length; i++) {
                        final long offset = checked[0] + i;
                        if (piece[i] != streamByte(offset)) {
                            throw new AssertionError(
                                    "P3 FAILED: byte " + offset + " of the stream came back as "
                                            + (piece[i] & 0xFF) + ", expected "
                                            + (streamByte(offset) & 0xFF));
                        }
                    }
                    checked[0] += piece.length;
                }
            }
        }, "drain");

        final long wallStart = System.nanoTime();
        reader.start();
        drain.start();
        reader.join();
        running[0] = false;
        drain.join();
        final double seconds = (System.nanoTime() - wallStart) / 1e9;

        require(ring.overrunBytes() == 0,
                "P3 FAILED: " + ring.overrunBytes() + " bytes were lost to a late drain, "
                        + "but the drain kept up the whole run");
        require(checked[0] == totalBytes,
                "P3 FAILED: " + checked[0] + " of " + totalBytes + " bytes came back");
        require(totalBytes > 2L * UsbReadRing.CAPACITY_BYTES,
                "P3 FAILED: the run did not wrap the buffer twice");

        // P4: the pieces vary, and they average far more than one packet.
        int min = Integer.MAX_VALUE;
        int max = 0;
        long sum = 0;
        for (final int size : pieceSizes) {
            min = Math.min(min, size);
            max = Math.max(max, size);
            sum += size;
        }
        final double average = (double) sum / pieceSizes.size();
        require(pieceSizes.size() > 20,
                "P4 FAILED: only " + pieceSizes.size() + " pieces were handed up");
        require(min != max,
                "P4 FAILED: every piece was exactly " + min + " bytes, which is a fixed size");
        require(average > 20 * PACKET_PAYLOAD,
                "P4 FAILED: the average piece was " + average + " bytes, barely more than the "
                        + PACKET_PAYLOAD + "-byte packet one read returns");

        System.out.println("P3 nothing is lost: " + totalBytes + " bytes in, " + checked[0]
                + " bytes out in order, across "
                + String.format("%.2f", (double) totalBytes / UsbReadRing.CAPACITY_BYTES)
                + " wraps of the " + UsbReadRing.CAPACITY_BYTES + "-byte buffer, "
                + ring.overrunBytes() + " lost");
        System.out.println("P4 large variable pieces: " + pieceSizes.size() + " pieces, "
                + min + " to " + max + " bytes, average "
                + String.format("%.0f", average) + " bytes ("
                + String.format("%.1f", average / PACKET_PAYLOAD) + " packets)");
        System.out.println("MEASUREMENT at " + FASTEST_BYTES_PER_SECOND + " bytes a second over "
                + String.format("%.2f", seconds) + " s: "
                + String.format("%.0f", readCount[0] / seconds) + " reads a second, "
                + String.format("%.1f", pieceSizes.size() / seconds) + " hand-offs a second, "
                + String.format("%.0f", average) + " bytes an average piece");
    }

    // -----------------------------------------------------------------------
    // P5, first half: the drain is late but the buffer still has room.
    // -----------------------------------------------------------------------

    static void p5LateDrainLosesNothingWhileThereIsRoom() {
        final UsbReadRing ring = new UsbReadRing();
        // Two seconds of the fastest board, stored with NO drain at all. The
        // buffer holds 5.94 seconds, so there is room and nothing may be lost.
        final long stored = 2L * FASTEST_BYTES_PER_SECOND;
        final byte[] packet = new byte[PACKET_PAYLOAD];
        long produced = 0;
        while (produced < stored) {
            final int n = (int) Math.min(PACKET_PAYLOAD, stored - produced);
            for (int i = 0; i < n; i++) {
                packet[i] = streamByte(produced + i);
            }
            ring.write(packet, n);
            produced += n;
        }

        require(ring.overrunBytes() == 0,
                "P5 FAILED: the drain missed " + (stored / FASTEST_BYTES_PER_SECOND)
                        + " seconds of turns while the buffer still had room, and "
                        + ring.overrunBytes() + " bytes were lost anyway");

        final byte[] piece = ring.drain();
        require(piece != null && piece.length == produced,
                "P5 FAILED: the one late drain took "
                        + (piece == null ? "nothing" : piece.length + " bytes")
                        + " of the " + produced + " waiting");
        for (int i = 0; i < piece.length; i++) {
            require(piece[i] == streamByte(i),
                    "P5 FAILED: byte " + i + " of the late drain is wrong");
        }
        System.out.println("P5a a late drain loses nothing while there is room: no drain for "
                + (stored / FASTEST_BYTES_PER_SECOND) + " s at " + FASTEST_BYTES_PER_SECOND
                + " bytes a second, then one turn took all " + piece.length
                + " bytes in order, 0 lost");
    }

    // -----------------------------------------------------------------------
    // P5, second half: the buffer fills. The decided behaviour is that the
    // OLDEST bytes go and the newest are kept, in order.
    // -----------------------------------------------------------------------

    static void p5FullBufferLosesTheOldestAndNothingElse() {
        // His ruling: "it will do whatever any circular buffer do. it will
        // overwrite it self." So the oldest bytes go and the newest survive in
        // order - no special case, no dropping of the newest.

        final UsbReadRing ring = new UsbReadRing();
        // Ten seconds of the fastest board with no drain at all — 1.68 buffers
        // more than the buffer holds.
        final long stored = 10L * FASTEST_BYTES_PER_SECOND;
        final byte[] packet = new byte[PACKET_PAYLOAD];
        long produced = 0;
        while (produced < stored) {
            final int n = (int) Math.min(PACKET_PAYLOAD, stored - produced);
            for (int i = 0; i < n; i++) {
                packet[i] = streamByte(produced + i);
            }
            ring.write(packet, n);
            produced += n;
        }

        final byte[] piece = ring.drain();
        require(piece != null, "P5 FAILED: the drain took nothing from a full buffer");
        require(piece.length == UsbReadRing.CAPACITY_BYTES,
                "P5 FAILED: the drain took " + piece.length + " bytes from a full buffer, "
                        + "expected the whole " + UsbReadRing.CAPACITY_BYTES);
        final long expectedLost = produced - UsbReadRing.CAPACITY_BYTES;
        require(ring.overrunBytes() == expectedLost,
                "P5 FAILED: " + ring.overrunBytes() + " bytes counted as lost, expected "
                        + expectedLost);

        // What came back must be the NEWEST bytes, in order: the stream offsets
        // from (produced - capacity) to produced.
        final long firstOffset = produced - UsbReadRing.CAPACITY_BYTES;
        for (int i = 0; i < piece.length; i++) {
            require(piece[i] == streamByte(firstOffset + i),
                    "P5 FAILED: the full buffer gave back byte " + (firstOffset + i)
                            + " as " + (piece[i] & 0xFF) + ", so what survived is not the "
                            + "newest bytes in order");
        }
        System.out.println("P5b a full buffer loses the OLDEST: " + produced
                + " bytes stored with no drain, " + expectedLost
                + " oldest bytes counted as lost, the newest " + piece.length
                + " came back in order");
    }

    // -----------------------------------------------------------------------
    // P8: every open at a speed clears head and tail. His ruling, and the whole
    // answer to the wrong-rate bytes felhr's own open produces at 9600.
    // -----------------------------------------------------------------------

    static void p8EveryOpenClearsHeadAndTail() {
        final UsbReadRing ring = new UsbReadRing();
        final byte[] noise = new byte[PACKET_PAYLOAD];
        for (int i = 0; i < noise.length; i++) {
            // The mis-framed f6 f6 f6 e6 ... the tester's phone recorded at the
            // wrong rate.
            noise[i] = (byte) ((i % 6 < 3) ? 0xF6 : 0xE6);
        }
        // Twelve reads' worth of 9600-baud noise before the rate is applied.
        for (int i = 0; i < 12; i++) {
            ring.write(noise, noise.length);
        }
        require(ring.writtenBytes() == 12L * PACKET_PAYLOAD,
                "P8 FAILED: the reader did not store the wrong-rate bytes; it must store "
                        + "whatever it reads and make no decisions");

        // setPortParameters applies the real rate and clears head and tail.
        ring.reset();
        require(ring.writtenBytes() == 0 && ring.drainedBytes() == 0,
                "P8 FAILED: reset left head at " + ring.writtenBytes() + " and tail at "
                        + ring.drainedBytes() + "; both must be back at the start");
        require(ring.drain() == null,
                "P8 FAILED: a cleared buffer handed something up, so wrong-rate bytes "
                        + "survived the reset");

        // And the real stream that follows comes through untouched.
        final byte[] real = new byte[PACKET_PAYLOAD];
        for (int i = 0; i < real.length; i++) {
            real[i] = streamByte(i);
        }
        ring.write(real, real.length);
        final byte[] piece = ring.drain();
        require(piece != null && piece.length == PACKET_PAYLOAD,
                "P8 FAILED: the first real read after the reset did not come through whole");
        for (int i = 0; i < piece.length; i++) {
            require(piece[i] == streamByte(i),
                    "P8 FAILED: byte " + i + " after the reset is wrong");
        }

        // A SECOND open at another rate clears it again — the probe opens the
        // same port again and again, which is exactly the case this protects.
        for (int i = 0; i < 12; i++) {
            ring.write(noise, noise.length);
        }
        ring.reset();
        require(ring.writtenBytes() == 0 && ring.drain() == null,
                "P8 FAILED: the SECOND open did not clear the buffer. The reset must be "
                        + "applied on every open, not only the first");
        System.out.println("P8 every open clears head and tail: "
                + (12 * PACKET_PAYLOAD) + " bytes of wrong-rate noise stored, cleared by "
                + "the reset, nothing handed up, the real stream after it intact, and the "
                + "same again on the next open");
    }

    // -----------------------------------------------------------------------

    /** Byte {@code i} of the scripted stream. Full period over 256. */
    static byte streamByte(long i) {
        return (byte) ((i * 31 + 7) & 0xFF);
    }

    static void require(boolean ok, String message) {
        if (!ok) {
            throw new AssertionError(message);
        }
    }
}
