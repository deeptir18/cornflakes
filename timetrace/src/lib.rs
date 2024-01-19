use color_eyre::eyre::{ensure, Result};
use csv::Writer;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::atomic;

// TODO: currently these are defined in cornflakes-libos
// and re-defined here, which is yucky
// as we need to import this crate in cornflakes-libos
// but need these types from cornflakes-libos
type MsgID = u32;
type ConnID = usize;

const BUFFER_SIZE_EXP: u8 = 16;
const BUFFER_SIZE: usize = 1 << BUFFER_SIZE_EXP;
const BUFFER_MASK: usize = BUFFER_SIZE - 1;
const BUFFER_PRINT_LIMIT: usize = 128;

static THREAD_COUNT: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
thread_local! {
        static TIME_TRACE_BUFFER: RefCell<TimeTraceBuffer> = RefCell::new(TimeTraceBuffer::new(
                THREAD_COUNT.fetch_add(1, atomic::Ordering::SeqCst)
                ));
}

#[macro_export]
macro_rules! record_event(
    ($format_str: expr, $request_idx: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Single, 0, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Single, $a, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Single, $a, $b, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Single, $a, $b, $c, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr, $d: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Single, $a, $b, $c, $d);
    };
);

#[macro_export]
macro_rules! record_end(
    ($format_str: expr, $request_idx: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::End, 0, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::End, $a, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::End, $a, $b, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::End, $a, $b, $c, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr, $d: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::End, $a, $b, $c, $d);
    };

);

#[macro_export]
macro_rules! record_start(
    ($format_str: expr, $request_idx: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Start, 0, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Start, $a, 0, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Start, $a, $b, 0, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Start, $a, $b, $c, 0);
    };
    ($format_str: expr, $request_idx: expr, $a: expr, $b: expr, $c: expr, $d: expr) => {
        timetrace::tt_record($format_str, $request_idx, timetrace::EventType::Start, $a, $b, $c, $d);
    };
);

pub fn tt_record(
    format_str: &str,
    request_idx: Option<(ConnID, MsgID)>,
    event_type: EventType,
    a: u32,
    b: u32,
    c: u32,
    d: u32,
) {
    TIME_TRACE_BUFFER.with(|tt| {
        tt.borrow_mut()
            .record(format_str, request_idx, event_type, a, b, c, d);
    });
}

pub fn print() -> Result<()> {
    TIME_TRACE_BUFFER.with(|tt| match env::var("TIMETRACELOG") {
        Ok(path) => tt.borrow().write_to_file(path),
        Err(_) => tt.borrow().print(),
    })?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum EventType {
    Start,
    End,
    Single,
}

impl Default for EventType {
    fn default() -> Self {
        EventType::Single
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Copy)]
pub struct Event {
    pub timestamp: u64,
    pub format_str_idx: usize,
    // type of event
    pub event_type: EventType,
    // some events are associated with a specific id
    pub request_idx: Option<(ConnID, MsgID)>,
    // format string args
    pub arg0: u32,
    pub arg1: u32,
    pub arg2: u32,
    pub arg3: u32,
}

impl Event {
    pub fn is_recorded(&self) -> bool {
        // because we currently use rdtsc time, it should never be 0 if it's a true recording
        self.timestamp != 0
    }

    pub fn get_event_str(&self, event_str: &str) -> Result<String> {
        let num_parens = event_str.match_indices("{}").count();
        ensure!(
            num_parens <= 4,
            format!("Number of args must be <= 4, not: {}", num_parens)
        );
        let mut cur = event_str.to_string();
        let args = vec![self.arg0, self.arg1, self.arg2, self.arg3];
        for (i, arg) in args.iter().enumerate() {
            if (i + 1) <= num_parens {
                cur = cur.as_str().replacen("{}", &format!("{}", arg), 1);
            }
        }
        Ok(cur)
    }

    pub fn get_conn_id_str(&self) -> String {
        if let Some((conn_id, _)) = self.request_idx {
            return format!("{}", conn_id);
        } else {
            return "None".to_string();
        }
    }

    pub fn get_msg_id_str(&self) -> String {
        if let Some((_, msg_id)) = self.request_idx {
            return format!("{}", msg_id);
        } else {
            return "None".to_string();
        }
    }

    pub fn get_csv_header() -> Vec<String> {
        return vec![
            "thread_id".to_string(),
            "event_str".to_string(),
            "event_type".to_string(),
            "time_nanos".to_string(),
            "conn_id".to_string(),
            "msg_id".to_string(),
        ];
    }

    pub fn get_csv_line(
        &self,
        timestamp_converted: u64,
        format_str: &str,
        thread_id: usize,
    ) -> Result<Vec<String>> {
        return Ok(vec![
            format!("{}", thread_id),
            self.get_event_str(format_str)?,
            format!("{:?}", self.event_type),
            format!("{:?}", timestamp_converted),
            self.get_conn_id_str(),
            self.get_msg_id_str(),
        ]);
    }
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
}

impl TimeTraceBuffer {
    pub fn new(thread_id: usize) -> Self {
        TimeTraceBuffer {
            thread_id,
            clock: quanta::Clock::new(),
            next_index: 0,
            format_string_map: HashMap::default(),
            next_format_string_idx: 0,
            events: vec![Event::default(); BUFFER_SIZE as usize],
            frozen: false,
        }
    }

    pub fn generate_reverse_map(&self) -> HashMap<usize, String> {
        let rev_map = self
            .format_string_map
            .iter()
            .map(|(k, v)| (*v, k.clone()))
            .collect::<HashMap<usize, String>>();
        rev_map
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
        request_idx: Option<(ConnID, MsgID)>,
        event_type: EventType,
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
        event.event_type = event_type;
        event.request_idx = request_idx;
        event.arg0 = arg0;
        event.arg1 = arg1;
        event.arg2 = arg2;
        event.arg3 = arg3;
    }

    pub fn find_minimum_timestamp_index(&self) -> usize {
        let mut min = u64::MAX;
        let mut min_index = 0;
        for (i, event) in self.events.iter().enumerate() {
            if event.is_recorded() && event.timestamp < min {
                min_index = i;
                min = event.timestamp;
            }
        }
        return min_index;
    }

    pub fn get_csv_lines(&self) -> Result<Vec<Vec<String>>> {
        let reverse_map = self.generate_reverse_map();
        let min_index = self.find_minimum_timestamp_index();
        let start_time_raw = self.events[min_index].timestamp;
        let mut ret = vec![Event::get_csv_header()];

        let mut range = (min_index..BUFFER_SIZE).collect::<Vec<usize>>();
        range.append(&mut (0..min_index).collect::<Vec<usize>>());
        tracing::info!(
            "Min index: {}, range len: {}, start time raw: {}",
            min_index,
            range.len(),
            start_time_raw
        );
        let mut last = start_time_raw;
        for idx in range.iter() {
            let event = self.events[*idx];
            if !event.is_recorded() {
                continue;
            }
            let nanos_since_start = self.clock.delta_as_nanos(start_time_raw, event.timestamp);
            let diff = match event.timestamp == start_time_raw {
                true => 0,
                false => nanos_since_start - last,
            };
            let format_str = reverse_map.get(&event.format_str_idx).ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "Could not find format string for idx {} in map, map: {:?}",
                    event.format_str_idx,
                    self.format_string_map,
                )
            })?;
            ret.push(event.get_csv_line(diff, format_str, self.thread_id)?);
            last = nanos_since_start;
        }
        Ok(ret)
    }

    pub fn print(&self) -> Result<()> {
        let csv_lines = self.get_csv_lines()?;
        tracing::info!("Finished processing csv for timetrace: {}", csv_lines.len());
        let len = csv_lines.len();
        for csv_line in csv_lines
            .into_iter()
            .take(std::cmp::min(BUFFER_PRINT_LIMIT, len))
        {
            println!("{}", csv_line.join("\t"));
        }
        Ok(())
    }

    // TODO: this only works on a per-thread basis (which is what we are starting with anyway)
    pub fn write_to_file<P: AsRef<Path>>(&self, filename: P) -> Result<()> {
        let csv_lines = self.get_csv_lines()?;
        let mut wtr = Writer::from_path(filename)?;
        for line in csv_lines.iter() {
            wtr.write_record(line)?;
        }
        wtr.flush()?;
        Ok(())
    }
}
