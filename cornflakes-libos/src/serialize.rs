use super::datapath::Datapath;

pub trait Serializable<D>
where
    D: Datapath,
{
    const CONSTANT_HEADER_SIZE: usize;
}
