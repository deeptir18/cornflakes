use cornflakes_libos::datapath::Datapath;
use redis;

use super::ClientSerializer;
use color_eyre::eyre::Result;
use std::marker::PhantomData;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct RedisClient<D>
where
    D: Datapath,
{
    datapath: PhantomData<D>,
}

impl<D> ClientSerializer<D> for RedisClient<D>
where
    D: Datapath,
{
    fn new() -> Self
    where
        Self: Sized,
    {
        RedisClient {
            datapath: PhantomData::default(),
        }
    }

    fn deserialize_get_response(&self, _buf: &[u8]) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn deserialize_getm_response(&self, _buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn deserialize_getlist_response(&self, _buf: &[u8]) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn check_add_user_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn check_follow_unfollow_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn check_post_tweet_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn check_get_timeline_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn check_retwis_response_num_values(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_get(&self, buf: &mut [u8], key: &str, _datapath: &D) -> Result<usize> {
        let data = redis::cmd("GET").arg(key).get_packed_command();
        buf[0..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    fn serialize_put(
        &self,
        buf: &mut [u8],
        key: &str,
        value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        let data = redis::cmd("SET").arg(key).arg(value).get_packed_command();
        buf[0..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    fn serialize_getm(&self, buf: &mut [u8], keys: &Vec<String>, _datapath: &D) -> Result<usize> {
        let data = {
            let mut cmd = redis::cmd("MGET");
            let mut cmd_ref = &mut cmd;
            for key in keys {
                cmd_ref = cmd_ref.arg(key);
            }
            cmd.get_packed_command()
        };
        buf[0..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    fn serialize_putm(
        &self,
        buf: &mut [u8],
        keys: &Vec<String>,
        values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        assert_eq!(keys.len(), values.len());
        let data = {
            let mut cmd = redis::cmd("MSET");
            let mut cmd_ref = &mut cmd;
            for i in 0..keys.len() {
                cmd_ref = cmd_ref.arg(&keys[i]).arg(&values[i]);
            }
            cmd.get_packed_command()
        };
        buf[0..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    // TODO: handle get list as lrange 0 to -1?
    fn serialize_get_list(&self, _buf: &mut [u8], _key: &str, _datapath: &D) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_put_list(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_append(
        &self,
        _buf: &mut [u8],
        _key: &str,
        _value: &str,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_add_user(
        &self,
        _buf: &mut [u8],
        _keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_add_follow_unfollow(
        &self,
        _buf: &mut [u8],
        _keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_post_tweet(
        &self,
        _buf: &mut [u8],
        _keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }

    fn serialize_get_timeline(
        &self,
        _buf: &mut [u8],
        _keys: &Vec<&str>,
        _values: &Vec<String>,
        _datapath: &D,
    ) -> Result<usize> {
        unimplemented!()
    }
}
