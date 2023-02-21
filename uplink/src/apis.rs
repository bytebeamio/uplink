use log::info;
use rouille::input::plain_text_body;
use rouille::{start_server, try_or_400, Response};

use crate::ReloadHandle;

pub fn start(port: u16, handle: ReloadHandle) {
    let address = format!("0.0.0.0:{}", port);
    info!("Starting tracing server: {address}");

    start_server(address, move |request| {
        let data = try_or_400!(plain_text_body(request));
        info!("Reloading tracing filter = {data:?}");
        if handle.reload(&data).is_err() {
            return Response::empty_400();
        }
        return Response::text(data);
    });
}
