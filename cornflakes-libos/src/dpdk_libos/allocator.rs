use super::{
    super::{dpdk_call, mem::closest_2mb_page},
    dpdk_bindings::*,
    wrapper,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolInfo {
    handle: *mut rte_mempool,
    start: usize,
    size: usize,
    object_size: usize,      // includes padding
    beginning_offset: usize, // page offset at beginning of region
    page_offset: usize,      // offset on each page
    headroom: usize,         // headroom at front of mbuf
}

impl Default for MempoolInfo {
    fn default() -> Self {
        MempoolInfo {
            handle: std::ptr::null_mut(),
            ..Default::default()
        }
    }
}

impl MempoolInfo {
    pub fn new(handle: *mut rte_mempool) -> Result<Self> {
        let (start, size, object_size, beginning_offset, page_offset, headroom) =
            get_mempool_memzone_area(handle).wrap_err(format!(
                "Not able to find memzone area for mempool: {:?}",
                handle
            ))?;

        Ok(MempoolInfo {
            handle: handle,
            start: start,
            size: size,
            object_size: object_size,
            beginning_offset: beginning_offset,
            page_offset: page_offset,
            headroom: headroom,
        })
    }

    pub fn is_within_bounds(&self, buf: &[u8]) -> bool {
        (buf.as_ptr() as usize > self.start) && (buf.as_ptr() as usize) < (self.start + self.size)
    }

    #[inline]
    fn header_size(&self) -> usize {
        unsafe { (*self.handle).header_size as usize }
    }

    #[inline]
    fn priv_size(&self) -> usize {
        unsafe { (*self.handle).private_data_size as usize }
    }

    #[inline]
    fn elt_size(&self) -> usize {
        unsafe { (*self.handle).elt_size as usize }
    }

    #[inline]
    fn get_front_padding(&self) -> usize {
        self.header_size() + self.priv_size() + self.headroom
    }

    pub fn get_mbuf_ptr(&self, buf: &[u8]) -> Result<(*mut rte_mbuf, u16)> {
        // If the mbuf is within bounds, return the mbuf this is pointing to
        let pg2mb = closest_2mb_page(buf.as_ptr());
        let base_mbuf = match pg2mb <= self.start {
            true => self.start + self.beginning_offset,
            false => pg2mb + self.page_offset,
        };
        let ptr_int = buf.as_ptr() as usize;
        let ptr_offset = (ptr_int) - base_mbuf;
        let offset_within_alloc = ptr_offset % self.object_size;

        tracing::debug!("Pointer offset within mbuf: {:?}", ptr_offset);
        ensure!(
            offset_within_alloc >= self.get_front_padding(),
            "Data pointer within header of mbuf: {:?} in {:?}",
            offset_within_alloc,
            self.get_front_padding()
        );

        let original_mbuf_ptr = (ptr_int - offset_within_alloc) as *mut rte_mbuf;
        // don't need to change the "offset" of the mbuf
        let data_off = (ptr_int - unsafe { (*original_mbuf_ptr).buf_addr as usize }) as u16;

        ensure!(
            offset_within_alloc < (self.elt_size() + self.get_front_padding()),
            "Data pointer within tail of mbuf: {:?} > {:?}",
            offset_within_alloc,
            self.get_front_padding()
        );

        tracing::debug!(
            ptr_int = ptr_int,
            original_mbuf_ptr = ?original_mbuf_ptr,
            offset_within_alloc = offset_within_alloc,
            data_off = data_off,
            "Calculated recovered mbuf"
        );
        Ok((original_mbuf_ptr, data_off))
    }

    #[inline]
    fn get_mempool(&self) -> *mut rte_mempool {
        self.handle
    }
}

impl Drop for MempoolInfo {
    fn drop(&mut self) {
        wrapper::free_mempool(self.handle);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MempoolAllocator {
    mempools: Vec<(usize, MempoolInfo)>,
}

impl MempoolAllocator {
    pub fn add_mempool(&mut self, mempool: *mut rte_mempool, obj_size: usize) -> Result<()> {
        let mempool_info = MempoolInfo::new(mempool)?;
        self.mempools.push((obj_size, mempool_info));
        Ok(())
    }

    // Right now: iterate through the mempools, and allocate from the smallest one fitting the
    // given size
    pub fn allocate(&self, alloc_size: usize, _alignment: usize) -> Result<*mut rte_mbuf> {
        let mut min_size = std::f64::INFINITY;
        let mut idx = 0;
        for (i, (size, _pool)) in self.mempools.iter().enumerate() {
            if alloc_size <= *size {
                if *size as f64 <= min_size {
                    min_size = *size as f64;
                    idx = i;
                }
            }
        }
        ensure!(
            min_size != std::f64::INFINITY,
            "Mempool to alloc from is still null"
        );
        // alloc from the mempool
        let mbuf = wrapper::alloc_mbuf(self.mempools[idx].1.get_mempool())?;
        Ok(mbuf)
    }

    pub fn recover_mbuf_ptr(&self, buf: &[u8]) -> Result<Option<(*mut rte_mbuf, u16)>> {
        for (_, mempool) in self.mempools.iter() {
            if mempool.is_within_bounds(buf) {
                let res = mempool.get_mbuf_ptr(buf)?;
                return Ok(Some(res));
            }
        }
        Ok(None)
    }
}

/// Determine three parameters about the memory in the DPDK mempool:
/// 1. (Start address, length)
/// 2. TOTAL size each mbuf takes (with any padding) (to know how to do modulus)
/// 3. Offset from each memhdr (where mbufs may not be page aligned).
fn get_mempool_memzone_area(
    mbuf_pool: *mut rte_mempool,
) -> Result<(usize, usize, usize, usize, usize, usize)> {
    let mut mbuf_addrs: Vec<usize> = vec![];
    let mut memzones: Vec<(usize, usize)> = vec![];

    // callback to iterate over the memory regions
    extern "C" fn memzone_callback(
        _mp: *mut rte_mempool,
        opaque: *mut ::std::os::raw::c_void,
        memhdr: *mut rte_mempool_memhdr,
        _idx: ::std::os::raw::c_uint,
    ) {
        let mr = unsafe { &mut *(opaque as *mut Vec<(usize, usize)>) };
        let (addr, len) = unsafe { ((*memhdr).addr, (*memhdr).len) };
        mr.push((addr as usize, len as usize));
    }

    // callback to iterate over the mbuf pool itself
    unsafe extern "C" fn mbuf_callback(
        _mp: *mut rte_mempool,
        opaque: *mut ::std::os::raw::c_void,
        m: *mut ::std::os::raw::c_void,
        _idx: u32,
    ) {
        let mbuf = m as *mut rte_mbuf;
        let mbuf_vec = &mut *(opaque as *mut Vec<usize>);
        mbuf_vec.push(mbuf as usize);
    }

    dpdk_call!(rte_mempool_obj_iter(
        mbuf_pool,
        Some(mbuf_callback),
        &mut mbuf_addrs as *mut _ as *mut ::std::os::raw::c_void
    ));

    dpdk_call!(rte_mempool_mem_iter(
        mbuf_pool,
        Some(memzone_callback),
        &mut memzones as *mut _ as *mut ::std::os::raw::c_void
    ));

    let mut space_between_mbufs = 0;
    let beginning_page_offset = mbuf_addrs[0] - memzones[0].0;
    let mut page_offset = 0;
    let mut cur_memzone = 0;
    let mut ct = 0;
    let mut first_on_page = true;
    // first, calculate space between each mbuf
    for mbuf in mbuf_addrs.iter() {
        let mut memzone_addr = memzones[cur_memzone].0;
        let mut memzone_len = memzones[cur_memzone].1;
        if *mbuf >= (memzone_addr + memzone_len) {
            first_on_page = true;
            cur_memzone += 1;
            memzone_addr = memzones[cur_memzone].0;
            memzone_len = memzones[cur_memzone].1;
            ensure!(
                *mbuf >= memzone_addr && *mbuf <= (memzone_addr + memzone_len),
                "Something is off about address calculation."
            );

            let this_page_offset = mbuf - memzone_addr;
            if page_offset != 0 && this_page_offset != page_offset {
                tracing::debug!(
                    "For mbuf # {}, this current page offset {}, not equal to previous {}",
                    ct,
                    this_page_offset,
                    page_offset
                );
            }
            page_offset = this_page_offset;
        }
        if !first_on_page {
            // check the last - first
            let last_addr = mbuf_addrs[ct];
            let space = mbuf - last_addr;

            if ct != 0 {
                if space != space_between_mbufs {
                    tracing::debug!("For mbuf # {}, space_between_mbufs is not the same as previous: space: {}, previous: {}", ct, space, space_between_mbufs);
                    bail!("Something is not right about the counts");
                }
            }
            space_between_mbufs = space;
        }
        ct += 1;
        first_on_page = false;
    }

    ensure!(space_between_mbufs != 0, "Space between mbufs is still 0");
    ensure!(page_offset != 0, "Page offset is still 0");

    tracing::debug!(
        space_btwn = space_between_mbufs,
        memhdr_off = page_offset,
        begin_memhdr_off = beginning_page_offset,
        mempool_size = unsafe { (*mbuf_pool).size } as usize,
        mempool_hdr_size = unsafe { (*mbuf_pool).header_size } as usize,
        mempool_trailer_size = unsafe { (*mbuf_pool).trailer_size } as usize,
        priv_size = unsafe { (*mbuf_pool).private_data_size } as usize,
        elt_size = unsafe { (*mbuf_pool).elt_size } as usize,
        "Calculated and mempool measurements"
    );

    let start = mbuf_addrs[0];
    let mut end = mbuf_addrs[unsafe { (*mbuf_pool).populated_size } as usize - 1];
    end += unsafe { (*mbuf_pool).size } as usize;
    let size = end - start;

    let headroom = unsafe {
        let actual_offset = ((*(start as *mut rte_mbuf)).buf_addr) as usize - start;
        actual_offset - ((*mbuf_pool).header_size + (*mbuf_pool).private_data_size) as usize
    };
    tracing::debug!(
        calc_headroom = headroom,
        buf_offset = unsafe { (*(start as *mut rte_mbuf)).data_off },
        "mbuf offsets"
    );

    Ok((
        start,
        size,
        space_between_mbufs,
        beginning_page_offset,
        page_offset,
        headroom,
    ))
}
