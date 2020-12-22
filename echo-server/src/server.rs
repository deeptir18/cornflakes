// use color_eyre::eyre::{bail, Result, WrapErr};
use cornflakes_libos::Datapath; //, ReceivedPacket, ScatterGather};

/// Server has no state other than the datapath itself.
pub struct _EchoServer<D: Datapath> {
    _datapath: D,
}

impl<D> _EchoServer<D>
where
    D: Datapath,
{
    pub fn _new(datapath: D) -> _EchoServer<D> {
        _EchoServer {
            _datapath: datapath,
        }
    }
}
