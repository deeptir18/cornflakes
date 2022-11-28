use super::{
    super::dpdk_bindings::*,
    connection::{DpdkBuffer, DpdkConnection, RteMbufMetadata},
};
use color_eyre::eyre::{bail, ensure, Result, WrapErr};
use cornflakes_libos::{
    allocator::{DatapathMemoryPool, MempoolID},
    datapath::Datapath,
    mem::closest_2mb_page,
};

/// Determine three parameters about the memory in the DPDK mempool:
/// 1. (Start address, length)
/// 2. TOTAL size each mbuf takes (with any padding) (to know how to do modulus)
/// 3. Offset from each memhdr (where mbufs may not be page aligned).
fn get_mempool_memzone_area(
    mbuf_pool: *mut rte_mempool,
) -> Result<(usize, usize, usize, usize, usize, usize)> {
    // vector of (idx, mbuf addresses as usize)
    let mut mbuf_addrs: Vec<(usize, usize)> = vec![];

    // separate, per page log: (page, memzone_id, memzone_start, memzone_len)
    let mut memzone_pages: Vec<(usize, usize, usize, usize)> = vec![];

    // callback to iterate over the memory regions and fill in memzones
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

    // callback to iterate over the mbuf pool itself and fill in mbuf_addrs
    unsafe extern "C" fn mbuf_callback(
        _mp: *mut rte_mempool,
        opaque: *mut ::std::os::raw::c_void,
        m: *mut ::std::os::raw::c_void,
        idx: u32,
    ) {
        let mbuf = m as *mut rte_mbuf;
        let mbuf_vec = &mut *(opaque as *mut Vec<(usize, usize)>);
        mbuf_vec.push((idx as usize, mbuf as usize));
    }

    unsafe {
        rte_mempool_obj_iter(
            mbuf_pool,
            Some(mbuf_callback),
            &mut mbuf_addrs as *mut _ as *mut ::std::os::raw::c_void,
        );
    }

    unsafe {
        rte_mempool_mem_iter(
            mbuf_pool,
            Some(memzone_callback),
            &mut memzone_pages as *mut _ as *mut ::std::os::raw::c_void,
        );
    }

    // beginning page offset: first mbuf - first memzone start
    let beginning_page_offset = mbuf_addrs[0].1 - memzone_pages[0].2;
    tracing::debug!("Beginning page offset: {:?}", beginning_page_offset);

    // space between mbufs on the same page
    let mut space_between_mbufs = 0;
    // page offset from beginning of each page to first mbuf on that page
    let mut page_offset = 0;
    // current index into memzone pages array
    let mut cur_memzone_page_idx = 0;
    // current index into mbuf array
    let mut ct = 0;

    let pagesize = unsafe { (*(*mbuf_pool).mz).hugepage_sz as usize };
    let mut first_on_page = true;

    for (_idx, mbuf_ptr) in mbuf_addrs.iter() {
        let mbuf = *mbuf_ptr;
        // get current closest memzone page
        let mut closest_page = memzone_pages[cur_memzone_page_idx].0;
        let mut memzone_addr = memzone_pages[cur_memzone_page_idx].2;
        let mut memzone_len = memzone_pages[cur_memzone_page_idx].3;

        // if mbuf is past memzone or past the current closest page
        if mbuf >= (memzone_addr + memzone_len) || (mbuf >= (closest_page + pagesize)) {
            cur_memzone_page_idx += 1;
            first_on_page = true;
            closest_page = memzone_pages[cur_memzone_page_idx].0;
            memzone_addr = memzone_pages[cur_memzone_page_idx].2;
            memzone_len = memzone_pages[cur_memzone_page_idx].3;

            ensure!(
                mbuf >= memzone_addr && mbuf <= (memzone_addr + memzone_len),
                "Mbuf not within current memzone"
            );

            // this page offset = page offset from first mbuf on page and closest page
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
        // ensure mbuf is within current memzone
        ensure!(
            mbuf >= memzone_addr && mbuf <= (memzone_addr + memzone_len),
            "Mbuf not within current memzone"
        );

        // ensure mbuf is within current page
        ensure!(
            mbuf >= closest_page && mbuf < (closest_page + pagesize),
            "Mbuf not within current page"
        );

        if !first_on_page {
            // check the last - first
            let last_addr = mbuf_addrs[ct - 1].1;
            let space = mbuf - last_addr;

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

    // first mbuf address
    let start = mbuf_addrs[0].1;
    // last mbuf
    let mut end = mbuf_addrs[unsafe { (*mbuf_pool).populated_size } as usize - 1].1;
    end += unsafe { (*mbuf_pool).size } as usize;
    let size = end - start;

    let headroom = unsafe {
        // offset = buf_addr - mbuf_pointer_addr
        let actual_offset = ((*(start as *mut rte_mbuf)).buf_addr) as usize - start;
        // subtract mbuf header size and private data size
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
        start,                 // first mbuf address
        size,                  // size of region from first mbuf address
        space_between_mbufs,   // space between contiguous mbufs
        beginning_page_offset, // space from first mbuf to beginning of first memzone page
        page_offset,           // space from first mbuf to any other memzone page
        headroom, // headroom between buf address and beginning of mbuf, discounting private data and mbuf struct size
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MempoolInfo {
    handle: *mut rte_mempool,
    hugepage_size: usize,    // how big are the huge pages used in this mempool
    start: usize,            // start address of FIRST mbuf
    size: usize,             // size of mempool region
    object_size: usize,      // includes padding
    beginning_offset: usize, // page offset at beginning of region
    page_offset: usize,      // offset on each page
    headroom: usize,         // headroom at front of mbuf
}

impl DatapathMemoryPool for MempoolInfo {
    type DatapathImpl = DpdkConnection;

    type RegistrationContext = ();

    #[inline]
    fn get_2mb_pages(&self) -> Vec<usize> {
        // TODO: implement
        vec![]
    }

    #[inline]
    fn get_4k_pages(&self) -> Vec<usize> {
        // TODO: implement
        vec![]
    }

    #[inline]
    fn get_1g_pages(&self) -> Vec<usize> {
        // TODO: implement
        vec![]
    }

    fn register(&mut self, _registration_context: Self::RegistrationContext) -> Result<()> {
        Ok(())
    }

    fn unregister(&mut self) -> Result<()> {
        bail!("Cannot unregister DPDK memory pool");
    }

    fn is_registered(&self) -> bool {
        true
    }

    fn has_allocated(&self) -> bool {
        unimplemented!();
    }

    fn recover_metadata(
        &self,
        buf: <<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer,
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        // can just consume the DpdkBuffer and turn it into RteMbufMetadata
        Ok(RteMbufMetadata::from_dpdk_buf(buf))
    }

    fn recover_buffer(
        &self,
        buf: &[u8],
    ) -> Result<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathMetadata> {
        let pg2mb = closest_2mb_page(buf.as_ptr());
        let base_mbuf = match pg2mb <= self.start {
            true => self.start,                // first mbuf in region
            false => pg2mb + self.page_offset, // first mbuf on page
        };
        let ptr_int = buf.as_ptr() as usize;
        let ptr_offset = (ptr_int) - base_mbuf;
        let offset_within_alloc = ptr_offset % self.object_size;

        // ensure offset is not within the head
        ensure!(
            offset_within_alloc >= self.get_front_padding(),
            "Data pointer within header of mbuf: {:?} in {:?}",
            offset_within_alloc,
            self.get_front_padding()
        );

        // ensure offset is not within the tail
        ensure!(
            offset_within_alloc < (self.elt_size() + self.get_front_padding()),
            "Data pointer within tail of mbuf: {:?} > {:?}",
            offset_within_alloc,
            self.get_front_padding()
        );

        // original mbuf this is pointing to
        let original_mbuf_ptr = (ptr_int - offset_within_alloc) as *mut rte_mbuf;
        // space between data_off and buf addr
        let data_off = (ptr_int - unsafe { (*original_mbuf_ptr).buf_addr as usize }) as u16;

        // ensure calculated offset not larger than object size
        if data_off as usize > self.object_size {
            tracing::warn!(
                "For buffer {:?}, calculated data_off as {:?} but object size is {}, ptr_int: {}, off within alloc: {}; START of region: {:?}, base_mbuf: {}, ptr_offset: {}, off within alloc: {}",
                buf.as_ptr(),
                data_off,
                self.object_size,
                ptr_int,
                offset_within_alloc,
                self.start,
                base_mbuf as usize,
                ptr_offset,
                offset_within_alloc,
            );
        }

        let mbuf_data_off = unsafe { access!(original_mbuf_ptr, data_off, u16) };
        ensure!(
            data_off >= mbuf_data_off,
            "Data off less than mbuf's recorded data off"
        );
        tracing::debug!(
            ptr_int = ptr_int,
            original_mbuf_ptr = ?original_mbuf_ptr,
            offset_within_alloc = offset_within_alloc,
            data_off = data_off,
            mbuf_data_off = mbuf_data_off,
            "Calculated recovered mbuf"
        );
        RteMbufMetadata::new(
            original_mbuf_ptr,
            (data_off - mbuf_data_off) as _,
            Some(buf.len()),
        )
    }

    fn alloc_data_buf(
        &self,
        context: MempoolID,
    ) -> Result<Option<<<Self as DatapathMemoryPool>::DatapathImpl as Datapath>::DatapathBuffer>>
    {
        let mbuf = unsafe { rte_pktmbuf_alloc(self.handle) };
        if mbuf.is_null() {
            return Ok(None);
        }
        return Ok(Some(DpdkBuffer::new(mbuf, context)));
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

    /*#[inline]
    fn get_mempool(&self) -> *mut rte_mempool {
        self.handle
    }*/
}

impl Drop for MempoolInfo {
    fn drop(&mut self) {
        tracing::info!("Dropping mempool {:?}", self.handle);
        unsafe {
            rte_mempool_free(self.handle);
        }
    }
}
