use super::{
    super::datapath::connection::{InlineMode, Mlx5Connection},
    RequestShape,
};
use color_eyre::eyre::{bail, Result};
use cornflakes_libos::{
    datapath::{Datapath, PushBufType, ReceivedPkt},
    state_machine::server::ServerSM,
    MsgID, RcSga, RcSge, Sga, Sge,
};

pub struct SimpleEchoServer {
    push_mode: PushBufType,
    inline_mode: InlineMode,
    zero_copy_threshold: usize,
    request_shape: RequestShape,
}

impl SimpleEchoServer {
    pub fn new(
        push_mode: PushBufType,
        inline_mode: InlineMode,
        zero_copy_threshold: usize,
        request_shape: RequestShape,
    ) -> SimpleEchoServer {
        SimpleEchoServer {
            push_mode: push_mode,
            inline_mode: inline_mode,
            zero_copy_threshold: zero_copy_threshold,
            request_shape: request_shape,
        }
    }
}
