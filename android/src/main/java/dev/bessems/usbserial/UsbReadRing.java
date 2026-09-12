package dev.bessems.usbserial;

/**
 * A circular byte buffer between the USB read thread and the thread that hands
 * bytes up to Dart. One writer, one reader, and it behaves like any circular
 * buffer: when the writer catches the reader it overwrites itself.
 *
 * <p>WHY IT EXISTS. Two shapes of the Android read came before this one and
 * each was half right:
 *
 * <ul>
 *   <li>felhr's asynchronous read never lost a byte, but it posted one
 *       EventChannel message per USB packet — about 1000 a second, of about 15
 *       bytes each — onto the Flutter main thread. The main thread cannot
 *       service that and draw at the same time, so the signal lagged.</li>
 *   <li>The synchronous read that replaced it (this fork's commit 6b2b5d0)
 *       fixed the message rate by asking one read for 16384 bytes, and started
 *       losing every byte instead: a USB bulk read finishes when it has all the
 *       bytes it asked for, when a packet SHORTER than the endpoint's packet
 *       size arrives, or on its timeout — and a timed-out bulk read does not
 *       give back the bytes it already held. The kernel drops them and
 *       {@code UsbDeviceConnection.bulkTransfer} answers -1. A board that is
 *       streaming steadily never sends a short packet, so a 16 KB read on such
 *       a board could only ever end on its timeout. Every read destroyed a
 *       tenth of a second of the stream and delivered nothing.</li>
 * </ul>
 *
 * <p>This buffer keeps both halves. The reader asks for ONE packet per read, so
 * the read is always satisfied and the timeout is never reached; it stores the
 * bytes here and does nothing else. A separate drain runs on its own clock, 20
 * times a second, and hands up whatever has collected since the last time — one
 * large, variable-size piece.
 *
 * <h2>Size: one megabyte, and the arithmetic</h2>
 *
 * Stanislav's ruling, 2026-09-12: <i>"you dont need 23 seconds of buffer it can
 * be max 5 sec"</i>. Five seconds at the fastest board the application
 * supports:
 *
 * <pre>
 *   44100 samples/second x 2 channels x 2 bytes/sample = 176400 bytes/second
 *   176400 bytes/second x 5 seconds                    = 882000 bytes
 * </pre>
 *
 * 1048576 (1 MB) is the next power of two above 882000 bytes, which is 5.94
 * seconds at that rate. A power of two lets an index wrap with a bit mask
 * instead of a division. At the board that made this fault visible — the
 * Human-Human Interface, 10000 Hz x 1 channel x 2 bytes = 20000 bytes a second
 * — the same buffer holds 52 seconds.
 *
 * <h2>When the buffer fills: IT OVERWRITES ITSELF, AND THAT IS ALL</h2>
 *
 * His ruling, 2026-09-12: <i>"'if the drain is ever late and the buffer fills,'
 * it can not since it will not read fixed amount but the all that is avilable at
 * that time. But it will do whatever any circular buffer do. it will overwrite
 * it self."</i>
 *
 * <p>So there is no special case here. The drain never takes a fixed amount — it
 * takes everything available at that moment — so it cannot meaningfully fall
 * behind. If the writer does catch the reader anyway, the bytes the tail pointed
 * at no longer exist and the tail starts again at the oldest byte that does.
 * That is ordinary circular-buffer behaviour: no error, no notice, no dropping
 * of the newest, nothing clever. {@link #overrunBytes()} counts it for a log
 * line and changes nothing.
 *
 * <h2>Every open clears it</h2>
 *
 * His ruling, same day: <i>"Any time you open connection at one speed you reset
 * head and tail (you clear the circular buffer buffer)"</i> — see
 * {@link #reset()}.
 *
 * <h2>The one lock, and why neither thread can hold up the other</h2>
 *
 * Every method synchronizes on this object, so a reset is safe and a piece can
 * never be handed up torn. The lock is held only for one
 * {@code System.arraycopy} and a few integer stores, and NEVER across anything
 * that blocks: the reader is outside it while it waits in {@code bulkTransfer},
 * and the drain is outside it while it sleeps between turns. So the longest
 * either thread can wait for the other is the length of a memory copy — at most
 * one packet on the writer's side, and about 8.8 KB a turn on the reader's at
 * the fastest board.
 *
 * <p>Nothing in this class is Android-specific, so it can be compiled and
 * driven on a plain Java virtual machine. {@code tools/UsbReadRingSelfCheck.java}
 * does exactly that, with two real threads.
 */
final class UsbReadRing {

    /** Five seconds at the fastest board, rounded up to a power of two. */
    static final int CAPACITY_BYTES = 1 << 20; // 1048576

    private static final int INDEX_MASK = CAPACITY_BYTES - 1;

    private final byte[] m_bytes = new byte[CAPACITY_BYTES];

    /** Total bytes stored since the buffer was last cleared — the head. */
    private long m_written = 0;

    /** Total bytes handed up since the buffer was last cleared — the tail. */
    private long m_drained = 0;

    /** Total bytes the writer overwrote before the reader reached them. */
    private long m_overrun = 0;

    /** How many pieces {@link #drain} has handed up. */
    private long m_pieces = 0;

    /**
     * Store the first {@code length} bytes of {@code src}. READER SIDE.
     *
     * <p>Never blocks on the USB, never allocates, never refuses. If there is no
     * room the oldest bytes are written over, which is what a circular buffer
     * does.
     */
    synchronized void write(byte[] src, int length) {
        if (src == null || length <= 0) {
            return;
        }
        // A single read can never be larger than one USB packet, so this clamp
        // is a guard and not a code path, but a silent out-of-bounds copy is
        // worse than a clamp.
        final int n = Math.min(length, CAPACITY_BYTES);
        final int start = (int) (m_written & INDEX_MASK);
        final int toEnd = CAPACITY_BYTES - start;
        if (n <= toEnd) {
            System.arraycopy(src, 0, m_bytes, start, n);
        } else {
            System.arraycopy(src, 0, m_bytes, start, toEnd);
            System.arraycopy(src, toEnd, m_bytes, 0, n - toEnd);
        }
        m_written += n;
    }

    /**
     * Take everything stored since the last turn and hand it back as one piece.
     * DRAIN SIDE.
     *
     * <p>Returns {@code null} when there is nothing there — never an empty
     * array, and never a fixed size: the piece is exactly as large as whatever
     * arrived, which at 20 turns a second is about 8.8 KB at the fastest board
     * and about 1 KB at the slowest.
     */
    synchronized byte[] drain() {
        final long head = m_written;
        long tail = m_drained;
        long available = head - tail;
        if (available <= 0) {
            return null;
        }

        if (available > CAPACITY_BYTES) {
            // The writer caught the reader, so the bytes the tail pointed at do
            // not exist any more and it starts again at the oldest that does.
            // Ordinary circular-buffer behaviour, nothing else; the count is for
            // a log line and changes nothing.
            m_overrun += available - CAPACITY_BYTES;
            tail = head - CAPACITY_BYTES;
            available = CAPACITY_BYTES;
        }

        final int n = (int) available;
        final byte[] out = new byte[n];
        final int start = (int) (tail & INDEX_MASK);
        final int toEnd = CAPACITY_BYTES - start;
        if (n <= toEnd) {
            System.arraycopy(m_bytes, start, out, 0, n);
        } else {
            System.arraycopy(m_bytes, start, out, 0, toEnd);
            System.arraycopy(m_bytes, 0, out, toEnd, n - toEnd);
        }

        m_drained = tail + n;
        m_pieces++;
        return out;
    }

    /**
     * Empty the buffer: head and tail both back to the start.
     *
     * <p>His ruling, 2026-09-12: <i>"Any time you open connection at one speed
     * you reset head and tail (you clear the circular buffer buffer) (this is
     * regarding: what happens to the wrong-rate bytes the library produces at
     * 9600 inside its own open)"</i>.
     *
     * <p>That is the whole answer to the 9600-baud hazard. felhr's
     * {@code openFTDI()} ends by programming the chip to 9600
     * (FTDISerialDevice.java:474) and the adapter starts its read thread inside
     * {@code syncOpen()}, so whatever is read before the caller's rate is
     * applied is mis-clocked framing noise. Clearing the buffer where the speed
     * is set throws it away, and every open starts clean.
     *
     * <p>It is applied on EVERY open, not only the first, because the rate probe
     * opens the same port again and again at different rates — which is exactly
     * the case this protects. Nothing real is ever lost by it: the caller writes
     * its first byte to the board only after the rate has been set.
     */
    synchronized void reset() {
        m_written = 0;
        m_drained = 0;
        m_overrun = 0;
        m_pieces = 0;
    }

    /** Total bytes stored since the buffer was last cleared. */
    synchronized long writtenBytes() {
        return m_written;
    }

    /** Total bytes handed up since the buffer was last cleared. */
    synchronized long drainedBytes() {
        return m_drained;
    }

    /**
     * Total bytes the writer overwrote before the reader reached them, since the
     * buffer was last cleared. For a log line only; nothing behaves differently
     * because of it.
     */
    synchronized long overrunBytes() {
        return m_overrun;
    }

    /** How many pieces have been handed up since the buffer was last cleared. */
    synchronized long pieces() {
        return m_pieces;
    }
}
