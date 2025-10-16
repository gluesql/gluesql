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
            Initial => {
                let (s1_low, s1_high) = self.stream1.size_hint();
                let (s2_low, s2_high) = self.stream2.size_hint();

                if s1_high == Some(0) {
                    (s2_low, s2_high)
                } else if s1_low > 0 {
                    (s1_low, s1_high)
                } else {
                    let low = usize::from(s2_low > 0);
                    let high = match (s1_high, s2_high) {
                        (Some(h1), Some(h2)) => Some(max(h1, h2)),
                        _ => None,
                    };
                    (low, high)
                }
            }
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
            pin_mut,
            stream::{StreamExt, empty, iter, once, poll_fn},
        },
        std::task::Poll,
    };

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

    #[test]
    fn size_hint_initial_branches() {
        // stream1 high is Some(0)
        let s1 = empty();
        let s2 = once(async { 1 });
        let or = OrStream::new(s1, s2);
        assert_eq!(or.size_hint(), (1, Some(1)));

        // stream1 low > 0
        let s1 = once(async { 1 });
        let s2 = empty();
        let or = OrStream::new(s1, s2);
        assert_eq!(or.size_hint(), (1, Some(1)));

        // else branch with s2_low > 0
        let s1 = poll_fn(|_| Poll::<Option<i32>>::Pending);
        let s2 = once(async { 1 });
        let or = OrStream::new(s1, s2);
        assert_eq!(or.size_hint(), (1, None));

        // else branch with s2_low == 0
        let s1 = poll_fn(|_| Poll::<Option<i32>>::Pending);
        let s2 = empty();
        let or = OrStream::new(s1, s2);
        assert_eq!(or.size_hint(), (0, None));

        // both highs defined triggers max branch
        let s1 = iter([1, 2, 3]).filter(|_| async { true });
        let s2 = iter([1, 2]);
        let or = OrStream::new(s1, s2);
        assert_eq!(or.size_hint(), (1, Some(3)));
    }

    #[test]
    fn size_hint_state_changes() {
        block_on(async {
            // move to St1 after first item from stream1
            let or = OrStream::new(once(async { 1 }), once(async { 2 }));
            pin_mut!(or);
            assert_eq!(or.next().await, Some(1));
            assert_eq!(or.size_hint(), (0, Some(0)));

            // move to St2 when stream1 is empty
            let or = OrStream::new(empty(), once(async { 2 }));
            pin_mut!(or);
            assert_eq!(or.next().await, Some(2));
            assert_eq!(or.size_hint(), (0, Some(0)));
        });
    }
}
