use {
    core::{cmp::max, pin::Pin},
    futures::{
        ready,
        stream::Stream,
        task::{Context, Poll},
    },
    pin_project::pin_project,
};

#[derive(Debug)]
enum State {
    Initial,
    InSt1,
    InSt2,
}

#[pin_project]
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct OrStream<St1, St2> {
    #[pin]
    stream1: St1,
    #[pin]
    stream2: St2,
    state: State,
}

use State::{InSt1, InSt2, Initial};

impl<St1, St2> OrStream<St1, St2>
where
    St1: Stream,
    St2: Stream<Item = St1::Item>,
{
    pub fn new(stream1: St1, stream2: St2) -> Self {
        Self {
            stream1,
            stream2,
            state: Initial,
        }
    }
}

impl<St1, St2> Stream for OrStream<St1, St2>
where
    St1: Stream,
    St2: Stream<Item = St1::Item>,
{
    type Item = St1::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.state {
            Initial => match ready!(this.stream1.poll_next(cx)) {
                item @ Some(_) => {
                    *this.state = InSt1;

                    Poll::Ready(item)
                }
                None => {
                    *this.state = InSt2;

                    this.stream2.poll_next(cx)
                }
            },
            InSt1 => this.stream1.poll_next(cx),
            InSt2 => this.stream2.poll_next(cx),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.state {
            Initial => match self.stream1.size_hint() {
                (_, Some(0)) => self.stream2.size_hint(),
                (0, i1_high) => {
                    let (i2_low, i2_high) = self.stream2.size_hint();
                    let low = if i2_low > 0 { 1 } else { 0 };

                    let high = i1_high.and_then(|h1| i2_high.map(|h2| max(h1, h2)));
                    (low, high)
                }
                i1_hint => i1_hint,
            },
            InSt1 => self.stream1.size_hint(),
            InSt2 => self.stream2.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OrStream;
    use futures::executor::block_on;
    use futures::stream::{empty, once, StreamExt};

    #[test]
    fn basic() {
        block_on(async move {
            let s1 = once(async { 1 });
            let s2 = once(async { 3 });
            let v = OrStream::new(s1, s2).collect::<Vec<i32>>().await;
            assert_eq!(vec![1], v);

            let s1 = empty();
            let s2 = once(async { 3 });
            let v = OrStream::new(s1, s2).collect::<Vec<i32>>().await;
            assert_eq!(vec![3], v);

            let s1 = once(async { 3 });
            let s2 = empty();
            let v = OrStream::new(s1, s2).collect::<Vec<i32>>().await;
            assert_eq!(vec![3], v);
        });
    }
}
