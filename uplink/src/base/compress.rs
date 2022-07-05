use std::io::Write;

use async_compression::tokio::write::{ZlibEncoder, ZstdEncoder};
use lz4_flex::frame::FrameEncoder;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("LZ4 compression error: {0}")]
    Lz4(#[from] lz4_flex::frame::Error),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub enum CompressionAlgo {
    Lz4,
    Zlib,
    Zstd,
}

impl CompressionAlgo {
    pub async fn compress(&self, payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        match self {
            Self::Lz4 => Self::lz4_compress(payload, topic),
            Self::Zlib => Self::zlib_compress(payload, topic).await,
            Self::Zstd => Self::zstd_compress(payload, topic).await,
        }
    }

    fn lz4_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = FrameEncoder::new(vec![]);
        compressor.write_all(payload)?;
        *payload = compressor.finish()?;
        topic.push_str("/lz4");

        Ok(())
    }

    async fn zlib_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = ZlibEncoder::new(vec![]);
        compressor.write_all(payload).await?;
        compressor.shutdown().await?;
        *payload = compressor.into_inner();
        topic.push_str("/zlib");

        Ok(())
    }

    async fn zstd_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = ZstdEncoder::new(vec![]);
        compressor.write_all(payload).await?;
        compressor.shutdown().await?;
        *payload = compressor.into_inner();
        topic.push_str("/zstd");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use async_compression::tokio::bufread::{ZlibDecoder, ZstdDecoder};
    use lz4_flex::frame::FrameDecoder;
    use tokio::io::AsyncReadExt;

    use super::CompressionAlgo;

    #[tokio::test]
    async fn compress_decompress_lz4() {
        let mut payload = b"Hello World".to_vec();
        let mut topic = "hello/world".to_string();

        CompressionAlgo::Lz4.compress(&mut payload, &mut topic).await.unwrap();

        assert_eq!(topic, "hello/world/lz4");

        let mut decompressor = FrameDecoder::new(payload.as_slice());

        let mut decompressed = String::new();
        decompressor.read_to_string(&mut decompressed).unwrap();

        assert_eq!("Hello World", decompressed);
    }

    #[tokio::test]
    async fn compress_decompress_zlib() {
        let mut payload = b"Hello World".to_vec();
        let mut topic = "hello/world".to_string();

        CompressionAlgo::Zlib.compress(&mut payload, &mut topic).await.unwrap();

        assert_eq!(topic, "hello/world/zlib");

        let mut decompressor = ZlibDecoder::new(payload.as_slice());

        let mut decompressed = Vec::new();
        decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(b"Hello World".to_vec(), decompressed);
    }

    #[tokio::test]
    async fn compress_decompress_zstd() {
        let mut payload = b"Hello World".to_vec();
        let mut topic = "hello/world".to_string();

        CompressionAlgo::Zstd.compress(&mut payload, &mut topic).await.unwrap();

        assert_eq!(topic, "hello/world/zstd");

        let mut decompressor = ZstdDecoder::new(payload.as_slice());

        let mut decompressed = Vec::new();
        decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(b"Hello World".to_vec(), decompressed);
    }
}
