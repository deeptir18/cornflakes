pub mod native_include;

#[cfg(feature = "mlx5")]
#[link(name = "rte_net_mlx5")]
extern "C" {
    fn rte_pmd_mlx5_get_dyn_flag_names();
}

#[inline(never)]
pub fn load_mlx5_driver() {
    if std::env::var("DONT_SET_THIS").is_ok() {
        unsafe {
            rte_pmd_mlx5_get_dyn_flag_names();
        }
    }
}
