/// The general things we need to experiment with, for each application is:
/// (1) As a baseline, run *with other serialization protocols* rather than any cornflakes thing
/// (protobuf, capnproto, flatbuffers is fine): this creates a buffer in app which has to be copied
/// over (both levels of copying)
///     - The serialization ``interface'' we had from the c application was:
///         - each serialization library had and `init` function (for kv store, this didn't matter)
///         - then on the client side they produced the next sga to send
///         - on server side, it has a deserialize function which took the sga and parsed out an
///         sga and then you could produce the new sga from this
/// (2) Don't have registered memory, but still use scatter-gather at the app interface (so you
/// copy into something contiguous at the datapath layer), behind the header.
/// (3) Copy into something contiguous in the app: don't use scatter-gather at the app interface,
/// but copy into some registered buffer (maybe can reserve space behind the header, or have a way
/// to allocate extra from extra registered pages somehow). In this case, for the key-value store,
/// don't need to have a special allocator, but need to have a special pool based allocator to
/// allocate the serialized objects (which are generally going to be the same size, making this
/// easier to do).
/// (4) Have split heaps/registered memory and Scatter-gather at the app interface, so we can use
/// the offload.
///
/// And then for the final paper: might need to do some "translation" from the in-memory format
/// into something that the offload can handle - data structure could have a bunch of smaller
/// buffers, that need to be coalesced (might need to develop some sort of *algorithm* to
/// understand how to do this one.
/// So need to have more complicated workloads where the translation into the SGA is not as clear.
/// Also, we only want to write this logic of the open loop or closed loop once (ideally across
/// applications).
///
/// So given we need to build the apps these ways (to run these baselines: what's the best way to
/// build out the app related interfaces?)
///     - need to have some sort of trait related to implementing serialization (taking some in
///     memory data structure or payload, and serializing it)
///         - this could be in turn parametrized over the actual data structure that is being
///         serialized: so we can serialize the different necessary data structures
///         - for echo server - single message with bytes or string
///         - for kv store - either the get/put request, and get/put response
use color_eyre::eyre::Result; // {bail, Result, WrapErr};
use cornflakes_libos::{ClientSM, Datapath, MsgID, ScatterGather};
use std::{net::Ipv4Addr, ops::FnMut, time::Duration};
pub struct EchoClient<C, D> {
    _datapath: D,
    cornflake: C,
    server_ip: Ipv4Addr,
}

impl<C, D> EchoClient<C, D>
where
    C: ScatterGather + Clone,
    D: Datapath,
{
    pub fn _new() -> EchoClient<C, D> {
        unimplemented!();
    }
}

impl<C, D> ClientSM for EchoClient<C, D>
where
    C: ScatterGather + Clone,
    D: Datapath,
{
    type OutgoingMsg = C;
    type Datapath = D;
    fn server_ip(&self) -> Ipv4Addr {
        self.server_ip
    }

    fn send_next_msg(
        &mut self,
        mut send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        send_fn(self.cornflake.clone())
    }

    fn process_received_msg(
        &mut self,
        _sga: <<Self as ClientSM>::Datapath as Datapath>::ReceivedPkt,
        _rtt: Duration,
    ) -> Result<()> {
        Ok(())
    }

    fn msg_timeout_cb(
        &mut self,
        _id: MsgID,
        mut _send_fn: impl FnMut(Self::OutgoingMsg) -> Result<()>,
    ) -> Result<()> {
        unimplemented!();
    }
}
