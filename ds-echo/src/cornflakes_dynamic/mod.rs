pub mod echo_messages_sga {
    include!(concat!(env!("OUT_DIR"), "/echo_dynamic_sga.rs"));
}

pub mod echo_messages_rcsga {
    include!(concat!(env!("OUT_DIR"), "/echo_dynamic_rcsga.rs"));
}
