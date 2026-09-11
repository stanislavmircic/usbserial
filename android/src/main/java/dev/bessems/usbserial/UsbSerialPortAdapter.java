package dev.bessems.usbserial;

import android.hardware.usb.UsbDeviceConnection;
import android.util.Log;
import android.os.Handler;
import android.os.Looper;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

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

    // Large-buffer read loop (replaces felhr's per-packet async callback).
    //
    // The old async read delivered one EventChannel event per USB bulk packet
    // (~1000/second of ~15 bytes for a streaming SpikerBox) on the main thread.
    // The Flutter main thread cannot service ~1000 hand-offs/second while also
    // drawing, so the driver buffer backed up and the live signal lagged.
    //
    // Instead we open the device in synchronous mode and run one dedicated read
    // thread that asks the USB for the next packet and takes whatever is there —
    // 20 bytes or 62, however much the chip had ready. It gathers the bytes and
    // hands one large, variable-size piece up to Dart once it has a
    // real chunk (>= FLUSH_THRESHOLD_BYTES) or when the stream pauses. The read
    // loop is paced by data availability, not by a clock, so nothing depends on
    // a timer firing on time, and no fixed small packet size is imposed. Byte
    // order is preserved. Write uses the matching synchronous call on the main
    // thread (unchanged connect behaviour). The control calls (DTR/RTS/baud/
    // parity) are the same USB control messages in either mode.
    //
    // THE MOST A SINGLE ANDROID READ MAY ASK FOR: ONE USB PACKET.
    //
    // A USB bulk read of N bytes finishes when N bytes have arrived, when a packet
    // SHORTER than the endpoint's packet size arrives, or on timeout — and when it
    // times out the kernel throws away the bytes it already had and returns -1.
    // A streaming FTDI chip sends only FULL packets (it waits until it holds 62
    // bytes, far sooner than its 16 ms idle timer), so a multi-packet read on a
    // streaming board NEVER finishes early and ALWAYS times out: a 16 KB read at
    // the Human-Human Interface's 20000 bytes/second needs about 820 ms against a
    // 100 ms timeout, so every byte of that board was discarded and the log showed
    // zero at 500000 (T-305). One packet is the only size that is always reached.
    //
    // Both chips we read have a 64-byte bulk packet. felhr's FTDI read reserves two
    // status bytes for every 62 asked for, so 62 asks the wire for exactly 64; every
    // other driver passes the number through, so 64 asks for exactly 64.
    private static final int FTDI_READ_BYTES  = 62;
    private static final int OTHER_READ_BYTES = 64;
    private static final int READ_TIMEOUT_MS = 100;      // block up to this long waiting for data; on idle, flush the tail.
    private static final int FLUSH_THRESHOLD_BYTES = 512; // hand up once this much is gathered (a large, variable piece).
    private static final int WRITE_TIMEOUT_MS = 200;

    // Chosen once, from the driver felhr handed us — never guessed from the
    // vendor number.
    private final int m_readRequestBytes;

    // False until setPortParameters has applied the real baud rate. felhr's
    // openFTDI() ends by programming the chip to 9600
    // (FTDISerialDevice.java:474), and open() below starts the read thread
    // straight after syncOpen(), so the first reads of every open happen at
    // 9600. Those bytes are noise and are dropped on purpose (T-305).
    private volatile boolean m_rateSet = false;

    private volatile boolean m_reading = false;
    private Thread m_readThread;

    UsbSerialPortAdapter(BinaryMessenger messenger, int interfaceId, UsbDeviceConnection connection, UsbSerialDevice serialDevice) {
        m_Messenger = messenger;
        m_InterfaceId = interfaceId;
        m_Connection = connection;
        m_SerialDevice = serialDevice;
        m_readRequestBytes = (serialDevice instanceof com.felhr.usbserial.FTDISerialDevice)
                ? FTDI_READ_BYTES
                : OTHER_READ_BYTES;
        m_MethodChannelName = "usb_serial/UsbSerialPortAdapter/" + String.valueOf(interfaceId);
        m_handler = new Handler(Looper.getMainLooper());
        final MethodChannel channel = new MethodChannel(m_Messenger, m_MethodChannelName);
        channel.setMethodCallHandler(this);
        final EventChannel eventChannel = new EventChannel(m_Messenger, m_MethodChannelName + "/stream");
        eventChannel.setStreamHandler(this);
    }

    String getMethodChannelName() {
        return m_MethodChannelName;
    }

    private void setPortParameters(int baudRate, int dataBits, int stopBits, int parity) {
        m_SerialDevice.setBaudRate(baudRate);
        m_SerialDevice.setDataBits(dataBits);
        m_SerialDevice.setStopBits(stopBits);
        m_SerialDevice.setParity(parity);
        // From here on the line is running at the rate the caller asked for, so
        // what the read loop gathers is real data (T-305).
        m_rateSet = true;
    }

    private void setFlowControl( int flowControl ) {
        m_SerialDevice.setFlowControl(flowControl);
    }

    // Send one gathered piece up to Dart on the main thread (EventChannel
    // requires the sink to be called on the platform main thread).
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

    private final Runnable m_readLoop = new Runnable() {
        @Override
        public void run() {
            byte[] buffer = new byte[m_readRequestBytes];
            ByteArrayOutputStream gathered = new ByteArrayOutputStream();
            while (m_reading) {
                int n;
                try {
                    // Ask the wire for ONE packet; it is always reached, so this
                    // read finishes with its bytes instead of timing out and
                    // having them thrown away. Returns <= 0 only when the line
                    // really was idle for the whole timeout.
                    n = m_SerialDevice.syncRead(buffer, READ_TIMEOUT_MS);
                } catch (Exception e) {
                    if (!m_reading) break;
                    continue;
                }
                if (!m_rateSet) {
                    // Read at the 9600 baud felhr's open leaves behind, before
                    // setPortParameters applied the real rate. Mis-clocked
                    // framing noise — never hand it up.
                    gathered.reset();
                    continue;
                }
                if (n > 0) {
                    gathered.write(buffer, 0, n);
                    // Hand up a large piece once we have gathered a real chunk.
                    // Only deliver once a listener exists, otherwise keep
                    // gathering so no early bytes (e.g. the identify reply) are
                    // lost before Dart subscribes.
                    if (gathered.size() >= FLUSH_THRESHOLD_BYTES && m_EventSink != null) {
                        deliver(gathered.toByteArray());
                        gathered.reset();
                    }
                } else {
                    // Timeout with no data = a gap in the stream. Flush whatever
                    // we have so the tail is not held back.
                    if (gathered.size() > 0 && m_EventSink != null) {
                        deliver(gathered.toByteArray());
                        gathered.reset();
                    }
                }
            }
            // Final flush on stop so no gathered bytes are lost.
            if (gathered.size() > 0 && m_EventSink != null) {
                deliver(gathered.toByteArray());
            }
        }
    };

    private Boolean open() {
        if ( m_SerialDevice.syncOpen() ) {
            m_reading = true;
            m_readThread = new Thread(m_readLoop, "usb_serial-read-" + m_InterfaceId);
            m_readThread.start();
            return true;
        } else {
            return false;
        }
    }

    private Boolean close() {
        m_reading = false;
        Thread t = m_readThread;
        m_readThread = null;
        if (t != null) {
            try {
                t.join(READ_TIMEOUT_MS + 100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
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
