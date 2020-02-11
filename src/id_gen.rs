use crate::StreamId;

/// Provided to MultiplexerSenders to override the default incrementing generator
pub trait IdGen: Default {
    /// Produces a new Id
    fn next(&mut self) -> StreamId;

    /// The current Id
    fn id(&self) -> StreamId;

    /// Useful for setting a random seed, or a starting value.
    fn seed(&mut self, _seed: usize) {}
}

/// The default IdGen for MultiplexerSenders
#[derive(Default, Copy, Clone, PartialEq, Debug)]
pub struct IncrementIdGen {
    id: StreamId,
}
impl Unpin for IncrementIdGen {}
impl IdGen for IncrementIdGen {
    /// Find the next available StreamId
    #[tracing::instrument(level = "trace", skip(self))]
    fn next(&mut self) -> StreamId {
        self.id = self.id.wrapping_add(1);
        self.id
    }
    fn id(&self) -> StreamId {
        self.id
    }
    #[tracing::instrument(level = "trace", skip(self))]
    fn seed(&mut self, seed: StreamId) {
        self.id = seed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment() {
        let mut id = IncrementIdGen::default();

        id.seed(usize::max_value());
        assert_eq!(usize::max_value(), id.id());

        let zero = id.next();
        assert_eq!(0, zero);

        let one = id.next();
        assert_eq!(1, one);
        assert_eq!(one, id.id());
    }
}
