use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic;
const BUFFER_SIZE_EXP: u8 = 16;
const BUFFER_SIZE: usize = 1 << BUFFER_SIZE_EXP;
const BUFFER_MASK: usize = BUFFER_SIZE - 1;

static THREAD_COUNT: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
thread_local! {
        static TIME_TRACE_BUFFER: RefCell<TimeTraceBuffer> = RefCell::new(TimeTraceBuffer::new(
                THREAD_COUNT.fetch_add(1, atomic::Ordering::SeqCst)
                ));
}

#[macro_export]
macro_rules! record(
    ($format_str: expr, $request_idx: expr) => {
        timetrace::tt_record($format_str, $request_idx, 0, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr) => {
        timetrace::tt_record($format_str, $request_idx, $a, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr) => {
        timetrace::tt_record($format_str, $request_idx, $a, $b, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr) => {
        timetrace::tt_record($format_str, $request_idx, $a, $b, $c, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr, $d: expr) => {
        timetrace::tt_record($format_str, $request_idx, $a, $b, $c, $d);
    };
);

pub fn tt_record(format_str: &str, request_idx: u32, a: u32, b: u32, c: u32, d: u32) {
    TIME_TRACE_BUFFER.with(|tt| {
        tt.borrow_mut().record(format_str, request_idx, a, b, c, d);
    });
}

pub fn print() {
    TIME_TRACE_BUFFER.with(|tt| {
        tt.borrow().write_to_file();
    });
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Copy)]
pub struct Event {
    pub timestamp: u64,
    pub format_str_idx: usize,
    pub request_idx: u32,
    pub arg0: u32,
    pub arg1: u32,
    pub arg2: u32,
    pub arg3: u32,
}

#[derive(Debug, Clone)]
pub struct TimeTraceBuffer {
    thread_id: usize,
    clock: quanta::Clock,
    next_index: usize,
    format_string_map: HashMap<String, usize>,
    next_format_string_idx: usize,
    events: Vec<Event>,
    frozen: bool,
    start_time: u64,
}

impl TimeTraceBuffer {
    pub fn new(thread_id: usize) -> Self {
        let clock = quanta::Clock::new();
        TimeTraceBuffer {
            thread_id,
            start_time: clock.raw(),
            clock: clock,
            next_index: 0,
            format_string_map: HashMap::default(),
            next_format_string_idx: 0,
            events: vec![Event::default(); BUFFER_SIZE as usize],
            frozen: false,
        }
    }

    pub fn get_format_str_idx(&mut self, format_str: &str) -> usize {
        match self.format_string_map.contains_key(format_str) {
            true => *self.format_string_map.get(format_str).unwrap(),
            false => {
                self.format_string_map
                    .insert(format_str.to_string(), self.next_format_string_idx);
                let old = self.next_format_string_idx;
                self.next_format_string_idx += 1;
                old
            }
        }
    }

    pub fn update_next_index(&mut self) {
        self.next_index = (self.next_index + 1) & BUFFER_MASK;
    }

    pub fn get_timestamp(&self) -> u64 {
        // raw rdtsc time
        self.clock.raw()
    }

    pub fn record(
        &mut self,
        format_str: &str,
        request_idx: u32,
        arg0: u32,
        arg1: u32,
        arg2: u32,
        arg3: u32,
    ) {
        if self.frozen {
            return;
        }
        let timestamp = self.get_timestamp();
        let format_str_idx = self.get_format_str_idx(format_str);
        let event_index = self.next_index;
        self.update_next_index();
        let event = &mut self.events[event_index];
        event.timestamp = timestamp;
        event.format_str_idx = format_str_idx;
        event.request_idx = request_idx;
        event.arg0 = arg0;
        event.arg1 = arg1;
        event.arg2 = arg2;
        event.arg3 = arg3;
    }

    pub fn write_to_file(&self) {
        for i in 0..100 {
            tracing::info!(arr_idx = i, event=? self.events[i], "Event");
        }
    }
}
