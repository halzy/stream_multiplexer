use crate::StreamId;

pub trait IdGen: Default {
    fn next(&mut self) -> StreamId;
    fn id(&self) -> StreamId;
    fn seed(&mut self, _seed: usize) {}
}

#[derive(Default, Copy, Clone, PartialEq, Debug)]
pub struct IncrementIdGen {
    id: StreamId,
}
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
