use crate::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A custom Either enum for stream combinators.
///
/// All unsafe code is encapsulated here, as is normal for base stream APIs.
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Stream for Either<L, R>
where
    L: Stream,
    R: Stream<Item = L::Item>,
{
    type Item = L::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: This is safe because Either is never moved after being pinned,
        // and we only project to the inner stream, which is also pinned.
        unsafe {
            let this = self.get_unchecked_mut();
            match this {
                Either::Left(left) => {
                    let left_pin = Pin::new_unchecked(left);
                    left_pin.poll_next(cx)
                }
                Either::Right(right) => {
                    let right_pin = Pin::new_unchecked(right);
                    right_pin.poll_next(cx)
                }
            }
        }
    }
}

impl<L, R> Unpin for Either<L, R> where L: Unpin, R: Unpin {} 