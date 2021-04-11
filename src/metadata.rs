use pyo3::{PyResult, Python};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use rdkafka::Message;
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Headers;

#[pyclass]
pub struct Metadata {
    _headers: Vec<(String, Vec<u8>)>,
    #[pyo3(get)]
    timestamp: Option<i64>,
    #[pyo3(get)]
    topic: String,
    #[pyo3(get)]
    partition: i32,
    #[pyo3(get)]
    offset: i64,
}


impl Metadata {
    pub fn new(msg: &BorrowedMessage) -> Self {
        let mut copy_headers = Vec::new();

        if let Some(headers) = msg.headers() {
            for i in 0..headers.count() {
                let (header_name, header_value) = headers.get(i).unwrap();

                copy_headers.push((
                    header_name.to_string(),
                    header_value.to_vec()
                ));
            }
        }

        Metadata {
            _headers: copy_headers,
            timestamp: msg.timestamp().to_millis(),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
        }
    }
}

#[pymethods]
impl Metadata {
    #[getter]
    fn headers<'p>(&self, py: Python<'p>) -> PyResult<&'p PyDict> {
        let result = PyDict::new(py);

        for (key, val) in &self._headers {
            result.set_item(key, PyBytes::new(py, val.as_slice()))?;
        }

        Ok(result)
    }
}