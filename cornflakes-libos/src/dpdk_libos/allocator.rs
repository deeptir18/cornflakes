use super::{
    super::{dpdk_call, mem::closest_2mb_page},
    dpdk_bindings::*,
    wrapper,
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use hashbrown::HashMap;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolInfo {
    handle: *mut rte_mempool,
    hugepage_size: usize, // how big are the huge pages used in this mempool
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

        let hugepage_size = unsafe { (*(*handle).mz).hugepage_sz as usize };

        Ok(MempoolInfo {
            handle: handle,
            hugepage_size: hugepage_size,
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
    mempools: HashMap<usize, Vec<MempoolInfo>>,
}

impl MempoolAllocator {
    pub fn add_mempool(&mut self, mempool: *mut rte_mempool, obj_size: usize) -> Result<()> {
        let mempool_info = MempoolInfo::new(mempool)?;
        if self.mempools.contains_key(&obj_size) {
            let info_vec = self.mempools.get_mut(&obj_size).unwrap();
            info_vec.push(mempool_info);
        } else {
            let info_vec = vec![mempool_info];
            self.mempools.insert(obj_size, info_vec);
        }
        Ok(())
    }

    // Right now: iterate through the mempools, and allocate from the smallest one fitting the
    // given size
    // There could be multiple of the same size: for now, just iterate until we find one which has
    // available space
    pub fn allocate(&self, alloc_size: usize, _alignment: usize) -> Result<*mut rte_mbuf> {
        let mut min_size = std::f64::INFINITY;
        for size in self.mempools.keys() {
            if alloc_size <= *size {
                if *size as f64 <= min_size {
                    min_size = *size as f64;
                }
            }
        }

        ensure!(
            min_size != std::f64::INFINITY,
            "No suitable size mempool to alloc from found"
        );

        let mempools = self.mempools.get(&(min_size as usize)).unwrap();
        for mempool in mempools.iter() {
            let mbuf = dpdk_call!(rte_pktmbuf_alloc(mempool.get_mempool()));
            if !mbuf.is_null() {
                tracing::debug!(
                    "Allocating object of size {:?} from mempool of size {}",
                    alloc_size,
                    min_size
                );
                return Ok(mbuf);
            }
        }

        bail!(
            "No space in mempools to allocate buffer of size {}",
            alloc_size
        );
    }

    pub fn recover_mbuf_ptr(&self, buf: &[u8]) -> Result<Option<(*mut rte_mbuf, u16)>> {
        for mempools in self.mempools.values() {
            for mempool in mempools.iter() {
                if mempool.is_within_bounds(buf) {
                    let res = mempool.get_mbuf_ptr(buf)?;
                    return Ok(Some(res));
                }
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
    // separate, per page log: (page, memzone_id, memzone_start, memzone_len)
    let mut memzones: Vec<(usize, usize, usize, usize)> = vec![];

    // callback to iterate over the memory regions
    extern "C" fn memzone_callback(
        mp: *mut rte_mempool,
        opaque: *mut ::std::os::raw::c_void,
        memhdr: *mut rte_mempool_memhdr,
        idx: ::std::os::raw::c_uint,
    ) {
        let mr = unsafe { &mut *(opaque as *mut Vec<(usize, usize, usize, usize)>) };
        let (addr, len) = unsafe { ((*memhdr).addr, (*memhdr).len) };
        let hugepage_size = unsafe { (*(*mp).mz).hugepage_sz as usize };
        let num_pages = (len as f64 / hugepage_size as f64).ceil() as usize;
        let mut page_addr = closest_2mb_page(addr as *const u8);
        for _i in 0..num_pages {
            mr.push((
                page_addr as usize,
                idx as usize,
                addr as usize,
                len as usize,
            ));
            page_addr += hugepage_size;
        }
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
    let beginning_page_offset = mbuf_addrs[0] - memzones[0].2;
    tracing::debug!("Beginning page offset: {:?}", beginning_page_offset);
    let mut page_offset = 0;
    let mut cur_memzone = 0;
    let mut ct = 0;
    let mut first_on_page = true;
    // first, calculate space between each mbuf
    let pagesize = unsafe { (*(*mbuf_pool).mz).hugepage_sz as usize };
    for mbuf in mbuf_addrs.iter() {
        let mut closest_page = memzones[cur_memzone].0;
        //let memzone_id;
        let mut memzone_addr = memzones[cur_memzone].2;
        let mut memzone_len = memzones[cur_memzone].3;
        //tracing::debug!(ct = ct, mbuf =? mbuf, closest_page = closest_page, memzone_id = memzone_id, memzone_addr = memzone_addr, memzone_len=memzone_len, cur_memzone = cur_memzone);

        if *mbuf >= (memzone_addr + memzone_len) || (*mbuf >= (closest_page + pagesize)) {
            first_on_page = true;
            cur_memzone += 1;
            closest_page = memzones[cur_memzone].0;
            //memzone_id = memzones[cur_memzone].1;
            memzone_addr = memzones[cur_memzone].2;
            memzone_len = memzones[cur_memzone].3;
            //tracing::info!(mbuf =? mbuf, memzone_addr = memzone_addr, memzone_len=memzone_len, cur_memzone = cur_memzone, "first on page true");
            ensure!(
                *mbuf >= memzone_addr && *mbuf <= (memzone_addr + memzone_len),
                "Something is off about address calculation."
            );

            let this_page_offset = mbuf - closest_page;
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
            let last_addr = mbuf_addrs[ct - 1];
            let space = mbuf - last_addr;
            //tracing::debug!(ct = ct, space = space, last =? last_addr, this =? mbuf, cur_memzone = cur_memzone, boundaries =? (memzone_addr, memzone_addr + memzone_len),"Checking space");

            if ct > 1 {
                if space != space_between_mbufs {
                    tracing::warn!("For mbuf # {}, space_between_mbufs is not the same as previous: space: {}, previous: {}", ct, space, space_between_mbufs);
                    bail!("Something is not right about the counts");
                }
            }

            if ct > 0 {
                space_between_mbufs = space;
            }
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
        cur_data_off = unsafe { (*(start as *mut rte_mbuf)).data_off as usize },
        offset_of_buffer_from_start =
            unsafe { (*(start as *mut rte_mbuf)).buf_addr as usize - start },
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
