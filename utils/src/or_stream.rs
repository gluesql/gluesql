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
    St1,
    St2,
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

use State::{Initial, St1, St2};

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
                    *this.state = St1;

                    Poll::Ready(item)
                }
                None => {
                    *this.state = St2;

                    this.stream2.poll_next(cx)
                }
            },
            St1 => this.stream1.poll_next(cx),
            St2 => this.stream2.poll_next(cx),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.state {
            Initial => match self.stream1.size_hint() {
                (_, Some(0)) => self.stream2.size_hint(),
                (0, i1_high) => {
                    let (i2_low, i2_high) = self.stream2.size_hint();
                    let low = usize::from(i2_low > 0);

                    let high = i1_high.and_then(|h1| i2_high.map(|h2| max(h1, h2)));
                    (low, high)
                }
                i1_hint => i1_hint,
            },
            St1 => self.stream1.size_hint(),
            St2 => self.stream2.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::OrStream,
        futures::{
            Stream,
            executor::block_on,
            future::ready,
            stream::{StreamExt, empty, once},
        },
    };

    #[test]
    fn basic() {
        block_on(async move {
            let s1 = once(ready(1));
            let s2 = once(ready(3));
            let v = OrStream::new(s1, s2).collect::<Vec<i32>>().await;
            assert_eq!(vec![1], v);

            let s1 = empty();
            let s2 = once(ready(3));
            let v = OrStream::new(s1, s2).collect::<Vec<i32>>().await;
            assert_eq!(vec![3], v);

            let s1 = once(ready(3));
            let s2 = empty();
            let v = OrStream::new(s1, s2).collect::<Vec<i32>>().await;
            assert_eq!(vec![3], v);
        });
    }

    #[test]
    fn size_hint_states() {
        block_on(async move {
            // Initial state - `stream1` has an item so `size_hint` should come from it
            let s1 = once(ready(1));
            let s2 = once(ready(3));
            let mut or_stream = OrStream::new(s1, s2);
            assert_eq!((1, Some(1)), or_stream.size_hint());

            // After the first item `state` becomes `St1` and hints follow `stream1`
            assert_eq!(Some(1), or_stream.next().await);
            assert_eq!((0, Some(0)), or_stream.size_hint());

            // `stream1` is empty from the start so `size_hint` falls back to `stream2`
            let s1 = empty();
            let s2 = once(ready(2));
            let mut or_stream = OrStream::new(s1, s2);
            assert_eq!((1, Some(1)), or_stream.size_hint());

            // Polling once switches to `St2` which reports `stream2`'s size hint
            assert_eq!(Some(2), or_stream.next().await);
            assert_eq!((0, Some(0)), or_stream.size_hint());
        });
    }
}
