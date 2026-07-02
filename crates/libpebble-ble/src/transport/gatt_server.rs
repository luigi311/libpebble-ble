//! Phone-hosted PPoGATT GATT server (BlueZ peripheral via bluer).
//!
//! We register a GattApplication exporting the PPoGATT service.  The watch,
//! acting as GATT client, writes PPoGATT packets to our WRITE characteristic
//! and receives our notifications on the same characteristic.
//!
//!   service 10000000
//!     char 10000002  READ           — fixed 19-byte blob on read
//!     char 10000001  WRITE_NO_RESP + NOTIFY
//!   service badbadba-...  (Gadgetbridge also registers this; watch expects it)

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bluer::{
    gatt::{
        local::{
            characteristic_control, Application, Characteristic, CharacteristicControlEvent,
            CharacteristicNotify, CharacteristicNotifyMethod, CharacteristicRead,
            CharacteristicWrite, CharacteristicWriteMethod, Service,
        },
        CharacteristicWriter,
    },
    Adapter,
};
use futures::StreamExt;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, trace, warn};

use crate::{
    transport::ppogatt::{ppogatt_header, parse_ppogatt_header, PPoGATTSession, PPoGATTType, PPOGATT_WINDOW},
    uuids::{
        PPOGATT_BADBAD_SERVICE, PPOGATT_SERVER_READ_CHARACTERISTIC, PPOGATT_SERVER_SERVICE,
        PPOGATT_SERVER_WRITE_CHARACTERISTIC,
    },
};

// The exact 19-byte response Gadgetbridge returns for a read of 10000002.
const READ_BLOB: [u8; 19] = [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

type SendTx = mpsc::UnboundedSender<Vec<u8>>;

struct ServerState {
    mtu: usize,
    session: PPoGATTSession,
    tx_queue: VecDeque<Vec<u8>>,
}

impl ServerState {
    fn new() -> Self {
        Self { mtu: 23, session: PPoGATTSession::new(), tx_queue: VecDeque::new() }
    }

    fn max_body(&self) -> usize {
        // ATT(3) + PPoGATT header(1) overhead; floor at 20 bytes.
        (self.mtu.saturating_sub(4)).max(20)
    }
}

/// Handle held by the caller. Drop to unregister the GATT application.
pub struct PebbleGattServerHandle {
    send_tx: SendTx,
    mtu: Arc<Mutex<usize>>,
    _app_handle: bluer::gatt::local::ApplicationHandle,
}

impl PebbleGattServerHandle {
    /// Queue one whole Pebble Protocol message for transmission to the watch.
    pub fn send(&self, pebble_message: Vec<u8>) {
        if self.send_tx.send(pebble_message).is_err() {
            debug!("GATT send channel closed; message dropped");
        }
    }

    pub fn set_mtu(&self, mtu: usize) {
        *self.mtu.lock().unwrap() = mtu;
    }

    pub fn mtu(&self) -> usize {
        *self.mtu.lock().unwrap()
    }
}

/// Owns the GATT notification socket and forwards packets from `packet_rx`.
/// Updates `mtu_shared` from the negotiated ATT MTU. Calls `on_disconnect` on exit.
async fn write_task(
    writer: CharacteristicWriter,
    mut packet_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    on_disconnect: Arc<dyn Fn() + Send + Sync>,
    mtu_shared: Arc<Mutex<usize>>,
) {
    let ble_mtu = writer.mtu();
    *mtu_shared.lock().unwrap() = ble_mtu;
    debug!("BLE ATT MTU (from GATT server writer): {ble_mtu}");

    loop {
        tokio::select! {
            packet = packet_rx.recv() => {
                match packet {
                    Some(data) => {
                        if writer.send(&data).await.is_err() {
                            debug!("GATT write failed; watch disconnected");
                            break;
                        }
                    }
                    None => {
                        debug!("GATT packet channel closed; writer exiting");
                        break;
                    }
                }
            }
            _ = writer.closed() => {
                debug!("GATT writer closed by remote");
                break;
            }
        }
    }
    on_disconnect();
}

pub async fn start_gatt_server(
    adapter: &Adapter,
    on_data: Arc<dyn Fn(Vec<u8>) + Send + Sync + 'static>,
    on_disconnect: Arc<dyn Fn() + Send + Sync + 'static>,
    connected_notify: Arc<Notify>,
    session_ready_notify: Arc<Notify>,
) -> Result<PebbleGattServerHandle, bluer::Error> {
    let (send_tx, send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (ctrl, ctrl_handle) = characteristic_control();
    let mtu_shared = Arc::new(Mutex::new(23usize));

    let write_tx_clone = write_tx.clone();
    let write_char = Characteristic {
        uuid: PPOGATT_SERVER_WRITE_CHARACTERISTIC,
        write: Some(CharacteristicWrite {
            write: true,
            write_without_response: true,
            method: CharacteristicWriteMethod::Fun(Box::new(move |value, _req| {
                let tx = write_tx_clone.clone();
                Box::pin(async move {
                    let _ = tx.send(value);
                    Ok(())
                })
            })),
            ..Default::default()
        }),
        notify: Some(CharacteristicNotify {
            notify: true,
            method: CharacteristicNotifyMethod::Io,
            ..Default::default()
        }),
        control_handle: ctrl_handle,
        ..Default::default()
    };

    let read_char = Characteristic {
        uuid: PPOGATT_SERVER_READ_CHARACTERISTIC,
        read: Some(CharacteristicRead {
            read: true,
            fun: Box::new(|_req| Box::pin(async move { Ok(READ_BLOB.to_vec()) })),
            ..Default::default()
        }),
        ..Default::default()
    };

    let (badbad_ctrl, badbad_ctrl_handle) = characteristic_control();
    let badbad_char = Characteristic {
        uuid: PPOGATT_BADBAD_SERVICE,
        read: Some(CharacteristicRead {
            read: true,
            fun: Box::new(|_req| Box::pin(async move { Ok(vec![0x00]) })),
            ..Default::default()
        }),
        control_handle: badbad_ctrl_handle,
        ..Default::default()
    };

    let app = Application {
        services: vec![
            Service {
                uuid: PPOGATT_SERVER_SERVICE,
                primary: true,
                characteristics: vec![read_char, write_char],
                ..Default::default()
            },
            Service {
                uuid: PPOGATT_BADBAD_SERVICE,
                primary: true,
                characteristics: vec![badbad_char],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let app_handle = adapter.serve_gatt_application(app).await?;
    debug!("GATT server registered on adapter {}", adapter.name());

    let mtu_for_task = mtu_shared.clone();

    tokio::spawn(async move {
        tokio::spawn(async move {
            tokio::pin!(badbad_ctrl);
            while badbad_ctrl.next().await.is_some() {}
        });

        let mut state = ServerState::new();
        let mut notify_tx: Option<mpsc::UnboundedSender<Vec<u8>>> = None;
        let mut send_rx = send_rx;
        let mut write_rx = write_rx;

        tokio::pin!(ctrl);

        loop {
            state.mtu = *mtu_for_task.lock().unwrap();

            tokio::select! {
                Some(event) = ctrl.next() => {
                    if let CharacteristicControlEvent::Notify(writer) = event {
                        debug!("watch subscribed to PPoGATT server characteristic");
                        let (tx, rx) = mpsc::unbounded_channel();
                        notify_tx = Some(tx);
                        let disc = on_disconnect.clone();
                        let mtu_clone = mtu_for_task.clone();
                        tokio::spawn(write_task(writer, rx, disc, mtu_clone));
                        connected_notify.notify_waiters();
                    }
                }

                Some(packet) = write_rx.recv() => {
                    handle_ppogatt_in(
                        &packet,
                        &mut state,
                        &mut notify_tx,
                        &on_data,
                        &session_ready_notify,
                    );
                }

                Some(msg) = send_rx.recv() => {
                    let max_body = state.max_body();
                    for chunk in msg.chunks(max_body) {
                        state.tx_queue.push_back(chunk.to_vec());
                    }
                    pump_tx(&mut state, &mut notify_tx);
                }
            }
        }
    });

    Ok(PebbleGattServerHandle {
        send_tx,
        mtu: mtu_shared,
        _app_handle: app_handle,
    })
}

fn handle_ppogatt_in(
    packet: &[u8],
    state: &mut ServerState,
    notify_tx: &mut Option<mpsc::UnboundedSender<Vec<u8>>>,
    on_data: &Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    session_ready_notify: &Arc<Notify>,
) {
    if packet.is_empty() {
        trace!("PPoGATT empty packet ignored");
        return;
    }
    let (cmd_byte, serial) = parse_ppogatt_header(packet[0]);
    let body = &packet[1..];
    trace!("PPoGATT rx cmd={cmd_byte} serial={serial} len={}", body.len());

    match cmd_byte {
        c if c == PPoGATTType::ResetRequest as u8 => {
            state.session.reset();
            state.tx_queue.clear();
            let reply = if packet.len() > 1 {
                vec![
                    ppogatt_header(PPoGATTType::ResetComplete, 0),
                    PPOGATT_WINDOW,
                    PPOGATT_WINDOW,
                ]
            } else {
                vec![ppogatt_header(PPoGATTType::ResetComplete, 0)]
            };
            send_raw(reply, notify_tx);
            session_ready_notify.notify_waiters();
        }
        c if c == PPoGATTType::ResetComplete as u8 => {
            debug!("PPoGATT reset complete");
            session_ready_notify.notify_waiters();
        }
        c if c == PPoGATTType::Ack as u8 => {
            trace!("PPoGATT ack serial={serial}");
            state.session.on_ack(serial);
            pump_tx(state, notify_tx);
        }
        c if c == PPoGATTType::Data as u8 => {
            if let Some(messages) = state.session.on_data(serial, body) {
                // ACK the highest consecutive serial received (rx_seq - 1).
                // This may be higher than the incoming serial when buffered
                // ahead-of-sequence packets were also consumed.
                let ack_serial = state.session.rx_seq.wrapping_sub(1) & 0x1F;
                let ack = vec![ppogatt_header(PPoGATTType::Ack, ack_serial)];
                send_raw(ack, notify_tx);
                for msg in messages {
                    on_data(msg);
                }
            }
            // Buffered (ahead-of-sequence) or duplicate: no ACK needed.
        }
        other => {
            debug!("PPoGATT unknown command {other} ignored");
        }
    }
}

fn pump_tx(
    state: &mut ServerState,
    notify_tx: &mut Option<mpsc::UnboundedSender<Vec<u8>>>,
) {
    while !state.tx_queue.is_empty() && state.session.can_send() {
        let chunk = state.tx_queue.pop_front().unwrap();
        let seq = state.session.next_tx_seq();
        let header = ppogatt_header(PPoGATTType::Data, seq);
        trace!("PPoGATT tx DATA seq={seq} len={}", chunk.len());
        let mut packet = vec![header];
        packet.extend_from_slice(&chunk);
        send_raw(packet, notify_tx);
        if notify_tx.is_none() {
            return;
        }
    }
}

fn send_raw(
    packet: Vec<u8>,
    notify_tx: &mut Option<mpsc::UnboundedSender<Vec<u8>>>,
) {
    if let Some(tx) = notify_tx
        && tx.send(packet).is_err()
    {
        warn!("PPoGATT notify channel closed (link down)");
        *notify_tx = None;
    }
}
