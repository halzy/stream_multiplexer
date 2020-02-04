#[derive(Debug)]
pub struct TcpStreamProducer {
    inner: tokio::net::TcpListener,
}
impl TcpStreamProducer {
    pub fn new(inner: tokio::net::TcpListener) -> Self {
        Self { inner }
    }
}
impl futures::Stream for TcpStreamProducer {
    type Item = Result<tokio::net::TcpStream, std::io::Error>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .poll_accept(ctx)
            .map_ok(|(s, _)| s)
            .map(Into::into)
    }
}
