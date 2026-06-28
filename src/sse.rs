use bytes::BytesMut;

use crate::types::ParsedSseEvent;

#[derive(Default)]
pub struct SseParser {
    buffer: BytesMut,
}

pub struct ParsedSseEventWithRaw {
    pub event: Option<String>,
    pub id: Option<String>,
    pub retry: Option<String>,
    pub data: Vec<String>,
    pub raw: Vec<u8>,
}

impl SseParser {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<ParsedSseEventWithRaw> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();

        while let Some((end, delimiter_len)) = find_sse_event_boundary(&self.buffer) {
            let raw = self.buffer.split_to(end + delimiter_len).to_vec();
            events.push(parse_sse_event(raw, end));
        }

        events
    }
}

fn find_sse_event_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    for index in 0..buffer.len() {
        if buffer[index..].starts_with(b"\r\n\r\n") {
            return Some((index, 4));
        }
        if buffer[index..].starts_with(b"\n\n") {
            return Some((index, 2));
        }
    }
    None
}

fn parse_sse_event(raw: Vec<u8>, event_end: usize) -> ParsedSseEventWithRaw {
    let text = String::from_utf8_lossy(&raw[..event_end]);
    let mut event = ParsedSseEvent::default();

    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "event" => event.event = Some(value.to_owned()),
            "id" => event.id = Some(value.to_owned()),
            "retry" => event.retry = Some(value.to_owned()),
            "data" => event.data.push(value.to_owned()),
            _ => {}
        }
    }

    ParsedSseEventWithRaw {
        event: event.event,
        id: event.id,
        retry: event.retry,
        data: event.data,
        raw,
    }
}
