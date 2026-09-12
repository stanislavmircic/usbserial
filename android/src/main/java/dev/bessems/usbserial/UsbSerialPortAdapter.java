package dev.bessems.usbserial;

import android.hardware.usb.UsbConstants;
import android.hardware.usb.UsbDevice;
import android.hardware.usb.UsbDeviceConnection;
import android.hardware.usb.UsbEndpoint;
import android.hardware.usb.UsbInterface;
import android.util.Log;
import android.os.Handler;
import android.os.Looper;

import com.felhr.usbserial.FTDISerialDevice;
import com.felhr.usbserial.UsbSerialDevice;
import com.felhr.usbserial.UsbSerialInterface;

import io.flutter.plugin.common.EventChannel;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import io.flutter.plugin.common.MethodChannel.MethodCallHandler;
import io.flutter.plugin.common.MethodChannel.Result;
import io.flutter.plugin.common.BinaryMessenger;

public class UsbSerialPortAdapter implements MethodCallHandler, EventChannel.StreamHandler {

    private final String TAG = UsbSerialPortAdapter.class.getSimpleName();

    private int m_InterfaceId;
    private UsbDeviceConnection m_Connection;
    private UsbSerialDevice m_SerialDevice;
    private BinaryMessenger m_Messenger;
    private String m_MethodChannelName;
    private volatile EventChannel.EventSink m_EventSink;
    private Handler m_handler;

    // ONE USB PACKET PER READ, INTO A CIRCULAR BUFFER, DRAINED ON A SLOW CLOCK.
    //
    // Three parts, and each one exists because the two shapes that came before
    // it were each half right:
    //
    //  * felhr's asynchronous read never lost a byte, but it posted one
    //    EventChannel message per USB packet — about 1000 a second, about 15
    //    bytes each — onto the Flutter main thread, which cannot service that
    //    and draw at the same time.
    //  * The synchronous read that replaced it (this fork's 6b2b5d0) fixed the
    //    message rate by asking a single read for 16384 bytes, and started
    //    losing every byte instead. A USB bulk read finishes when it has all the
    //    bytes it asked for, when a packet SHORTER than the endpoint's packet
    //    size arrives, or on its timeout — and a timed-out bulk read does NOT
    //    give back the bytes it already held: the kernel drops them and
    //    bulkTransfer answers -1. A board that streams steadily never sends a
    //    short packet (an FTDI chip sends as soon as it holds one packet's worth
    //    of data, far sooner than its 16 ms idle timer), so on such a board a
    //    16 KB read could only ever end on its timeout. At the Human-Human
    //    Interface's 20000 bytes a second, filling 16384 bytes needs about
    //    820 ms against a 100 ms timeout: every read destroyed a tenth of a
    //    second of the stream and delivered nothing, so that board reported
    //    0 bytes at 500000 — the one rate at which it answers.
    //
    // 1. THE READER ASKS FOR ONE PACKET PER READ. One packet is the only size
    //    that is always reached, so the read returns with data every time and
    //    the timeout is never the thing that ends it. The size is asked of the
    //    ENDPOINT (see bulkInPacketBytes below), not assumed.
    // 2. IT STORES THE BYTES IN A 1 MB CIRCULAR BUFFER AND DOES NOTHING ELSE —
    //    no allocation per read, no delivery, no decisions. UsbReadRing carries
    //    the size arithmetic (five seconds at the fastest board) and the
    //    decision about what happens when it fills.
    // 3. A DRAIN RUNS 20 TIMES A SECOND AND TAKES WHATEVER IS THERE. It reads
    //    the write head, copies everything between the tail and that head,
    //    advances the tail, and hands one piece up. Never a fixed size: the
    //    piece is about 8.8 KB at the fastest board and about 1 KB at the
    //    slowest, always more than one packet, and it varies with what arrived.
    //
    // The promise to Dart is the one it always had — one large, variable-size
    // piece, in order, with nothing dropped, and the tail never held back by
    // more than one drain period. The mechanism that keeps it is new; the
    // promise is not. Byte order is preserved exactly. The write path, and the
    // control calls for baud, parity, DTR and RTS, are untouched.
    private static final int READ_TIMEOUT_MS = 100;   // only reached when the line really is idle
    private static final int WRITE_TIMEOUT_MS = 200;

    // 20 times a second. Slow enough that the main thread gets 20 messages a
    // second instead of 1000, fast enough that the tail of the stream is never
    // held back longer than this — well inside the 1200 ms the application
    // allows a board to answer its identify request.
    private static final int DRAIN_INTERVAL_MS = 50;

    // What felhr's FTDI read adds on top of what it is asked for: two status
    // bytes at the head of every 62 data bytes. FTDISerialDevice.syncRead
    // allocates buffer.length + ceil(buffer.length / 62) * 2 and hands THAT to
    // bulkTransfer (felhr 6.1.0, FTDISerialDevice.java:636-689), so to put
    // exactly one packet on the wire we must ask it for (packet - 2). Every
    // other driver passes the number straight through
    // (UsbSerialDevice.java:205-217), so we ask those for the packet size
    // itself.
    private static final int FTDI_STATUS_BYTES_PER_PACKET = 2;

    // Used only when the endpoint cannot be inspected at all (a null UsbDevice,
    // or a device that declares no bulk IN endpoint). 64 bytes is the maximum a
    // full-speed USB bulk endpoint may declare, so it can never be larger than
    // the real packet, and it is what both chips this application reads — the
    // FTDI FT230X/FT231X and the native-USB (CDC) boards — actually report.
    private static final int FALLBACK_PACKET_BYTES = 64;

    // The real maximum packet size of the bulk IN endpoint, read from the
    // endpoint itself, and what a single syncRead is asked for. Both are
    // decided once, in the constructor, and never change.
    private final int m_packetBytes;
    private final int m_readRequestBytes;

    // Everything the reader stores and the drain takes.
    private final UsbReadRing m_ring = new UsbReadRing();

    // False until setPortParameters has applied the real baud rate.
    //
    // felhr's openFTDI() ends by programming the chip to 9600 baud
    // (FTDISerialDevice.java:474), and open() below starts the read thread
    // straight after syncOpen(), so the first reads of every open are made at
    // 9600 no matter what rate the caller wants. Those bytes are mis-clocked
    // framing noise. THE READER STILL READS THEM — leaving them in the USB pipe
    // would fill the chip's own small buffer and stall the board — but it does
    // not store them, so they never enter the circular buffer and no part
    // downstream has to know about them. The reader is the only writer, so
    // there is nothing to clear afterwards either.
    private volatile boolean m_rateSet = false;

    private volatile boolean m_reading = false;
    private Thread m_readThread;
    private Thread m_drainThread;

    UsbSerialPortAdapter(BinaryMessenger messenger, int interfaceId, UsbDeviceConnection connection, UsbSerialDevice serialDevice) {
        this(messenger, interfaceId, connection, serialDevice, null, -1);
    }

    UsbSerialPortAdapter(BinaryMessenger messenger, int interfaceId, UsbDeviceConnection connection, UsbSerialDevice serialDevice, UsbDevice device, int iface) {
        m_Messenger = messenger;
        m_InterfaceId = interfaceId;
        m_Connection = connection;
        m_SerialDevice = serialDevice;
        m_packetBytes = bulkInPacketBytes(device, iface);
        m_readRequestBytes = (serialDevice instanceof FTDISerialDevice)
                ? m_packetBytes - FTDI_STATUS_BYTES_PER_PACKET
                : m_packetBytes;
        Log.i(TAG, "bulk IN endpoint packet size " + m_packetBytes
                + " bytes; one read asks for " + m_readRequestBytes
                + " bytes (" + serialDevice.getClass().getSimpleName() + ")");
        m_MethodChannelName = "usb_serial/UsbSerialPortAdapter/" + String.valueOf(interfaceId);
        m_handler = new Handler(Looper.getMainLooper());
        final MethodChannel channel = new MethodChannel(m_Messenger, m_MethodChannelName);
        channel.setMethodCallHandler(this);
        final EventChannel eventChannel = new EventChannel(m_Messenger, m_MethodChannelName + "/stream");
        eventChannel.setStreamHandler(this);
    }

    // ASK THE ENDPOINT ITS PACKET SIZE — never guess it from the vendor number.
    //
    // felhr claims one interface and then takes the first bulk IN endpoint on
    // it: the interface is the one the caller named when it named one, and
    // otherwise interface 0 for an FTDI chip (FTDISerialDevice.java:117) or the
    // first interface that carries serial data for a CDC device
    // (CDCSerialDevice.java:65). We look in the same place, and when the caller
    // named no interface we take the first bulk IN endpoint in interface order —
    // which for both of those rules is the same endpoint felhr ends up reading.
    //
    // A full-speed bulk endpoint may declare 8, 16, 32 or 64 bytes and a
    // high-speed one 512; whatever it declares is the size that always satisfies
    // a read, which is the whole point. Both chips this application reads report
    // 64.
    private static int bulkInPacketBytes(UsbDevice device, int iface) {
        if (device == null) {
            return FALLBACK_PACKET_BYTES;
        }
        final int count = device.getInterfaceCount();
        final int first = (iface >= 0) ? iface : 0;
        final int last = (iface >= 0) ? iface : count - 1;
        for (int i = first; i <= last && i < count; i++) {
            final UsbInterface ui = device.getInterface(i);
            if (ui == null) {
                continue;
            }
            for (int e = 0; e < ui.getEndpointCount(); e++) {
                final UsbEndpoint endpoint = ui.getEndpoint(e);
                if (endpoint == null) {
                    continue;
                }
                if (endpoint.getType() == UsbConstants.USB_ENDPOINT_XFER_BULK
                        && endpoint.getDirection() == UsbConstants.USB_DIR_IN) {
                    final int size = endpoint.getMaxPacketSize();
                    if (size > 0) {
                        return size;
                    }
                }
            }
        }
        return FALLBACK_PACKET_BYTES;
    }

    String getMethodChannelName() {
        return m_MethodChannelName;
    }

    private void setPortParameters(int baudRate, int dataBits, int stopBits, int parity) {
        m_SerialDevice.setBaudRate(baudRate);
        m_SerialDevice.setDataBits(dataBits);
        m_SerialDevice.setStopBits(stopBits);
        m_SerialDevice.setParity(parity);
        // From here on the line runs at the rate the caller asked for, so what
        // the reader stores is real data.
        m_rateSet = true;
    }

    private void setFlowControl( int flowControl ) {
        m_SerialDevice.setFlowControl(flowControl);
    }

    // Send one drained piece up to Dart on the main thread (EventChannel
    // requires the sink to be called on the platform main thread). This is the
    // ONLY main-thread work the read road does, and it happens 20 times a
    // second.
    private void deliver(final byte[] data) {
        m_handler.post(new Runnable() {
            @Override
            public void run() {
                EventChannel.EventSink sink = m_EventSink;
                if (sink != null) {
                    sink.success(data);
                }
            }
        });
    }

    // THE READER. Read one packet, store it, repeat. Nothing else.
    private final Runnable m_readLoop = new Runnable() {
        @Override
        public void run() {
            final byte[] packet = new byte[m_readRequestBytes];
            while (m_reading) {
                int n;
                try {
                    // Ask the wire for ONE packet. It is always reached, so this
                    // read comes back with its bytes instead of timing out and
                    // having them thrown away. It returns <= 0 only when the
                    // line really was idle for the whole timeout.
                    n = m_SerialDevice.syncRead(packet, READ_TIMEOUT_MS);
                } catch (Exception e) {
                    if (!m_reading) break;
                    continue;
                }
                if (n <= 0) {
                    continue;
                }
                if (!m_rateSet) {
                    // Read at the 9600 baud felhr's open leaves behind, before
                    // setPortParameters applied the real rate. Read so the USB
                    // pipe stays clear, then thrown away: mis-clocked noise
                    // never enters the circular buffer.
                    continue;
                }
                m_ring.write(packet, n);
            }
        }
    };

    // THE DRAIN. Once every DRAIN_INTERVAL_MS, take whatever is there.
    private final Runnable m_drainLoop = new Runnable() {
        @Override
        public void run() {
            while (m_reading) {
                try {
                    Thread.sleep(DRAIN_INTERVAL_MS);
                } catch (InterruptedException e) {
                    break;
                }
                handOff();
            }
            // One last turn after the reader has stopped, so the tail of the
            // stream is not left behind.
            handOff();
        }
    };

    // Take one piece and hand it up. Nothing is taken while Dart is not
    // listening yet: the circular buffer holds the bytes instead, so the
    // board's answer to an identify request written before the listener is
    // attached is still there when it arrives.
    private void handOff() {
        if (m_EventSink == null) {
            return;
        }
        final byte[] piece = m_ring.drain();
        if (piece != null && piece.length > 0) {
            deliver(piece);
        }
    }

    private Boolean open() {
        if ( m_SerialDevice.syncOpen() ) {
            m_reading = true;
            m_readThread = new Thread(m_readLoop, "usb_serial-read-" + m_InterfaceId);
            m_readThread.start();
            m_drainThread = new Thread(m_drainLoop, "usb_serial-drain-" + m_InterfaceId);
            m_drainThread.start();
            return true;
        } else {
            return false;
        }
    }

    private Boolean close() {
        m_reading = false;
        Thread reader = m_readThread;
        m_readThread = null;
        if (reader != null) {
            try {
                reader.join(READ_TIMEOUT_MS + 100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        Thread drain = m_drainThread;
        m_drainThread = null;
        if (drain != null) {
            drain.interrupt();
            try {
                drain.join(DRAIN_INTERVAL_MS + 100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        // Whatever the reader stored after the drain's last turn.
        handOff();
        Log.i(TAG, "read road closed: " + m_ring.writtenBytes() + " bytes stored, "
                + m_ring.drainedBytes() + " handed up in " + m_ring.pieces()
                + " pieces, " + m_ring.overrunBytes() + " lost to a late drain");
        m_SerialDevice.syncClose();
        return true;
    }

    private void write( byte[] data ) {
        // Synchronous write on the calling (main) thread — matches the pinned
        // main-thread write behaviour. The identify write is a few bytes.
        m_SerialDevice.syncWrite(data, WRITE_TIMEOUT_MS);
    }

    // return true if the object is to be kept, false if it is to be destroyed.
    public void onMethodCall(MethodCall call, Result result) {

        switch (call.method) {
            case "close":
                result.success(close());
                break;
            case "open":
                result.success(open());
                break;
            case "write":
                write((byte[])call.argument("data"));
                result.success(true);
                break;

            case "setPortParameters":
                setPortParameters((int) call.argument("baudRate"), (int) call.argument("dataBits"),
                        (int) call.argument("stopBits"), (int) call.argument("parity"));
                result.success(null);
                break;

            case "setFlowControl":
                setFlowControl((int) call.argument("flowControl"));
                result.success(null);
                break;

            case "setDTR": {
                boolean v = call.argument("value");
                m_SerialDevice.setDTR(v);
                if (v == true) {
                    Log.e(TAG, "set DTR to true");
                } else {
                    Log.e(TAG, "set DTR to false");
                }
                result.success(null);
                break;
            }
            case "setRTS": {
                boolean v = call.argument("value");
                m_SerialDevice.setRTS(v);
                result.success(null);
                break;
            }

            default:
                result.notImplemented();
        }
    }

    @Override
    public void onListen(Object o, EventChannel.EventSink eventSink) {
        m_EventSink = eventSink;

    }

    @Override
    public void onCancel(Object o) {
        m_EventSink = null;

    }




}
