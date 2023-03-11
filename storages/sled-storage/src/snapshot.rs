use {
    serde::{Deserialize, Serialize},
    std::fmt::Debug,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotItem<T> {
    data: T,
    created_by: u64,
    deleted_by: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot<T>(Vec<SnapshotItem<T>>);

impl<T: Clone> Snapshot<T> {
    pub fn new(txid: u64, data: T) -> Self {
        Self(vec![SnapshotItem {
            data,
            created_by: txid,
            deleted_by: None,
        }])
    }

    pub fn update(mut self, txid: u64, data: T) -> (Self, Option<T>) {
        let old_data = (!self.0.is_empty()).then(|| {
            if self.0[0].deleted_by.is_none() {
                self.0[0].deleted_by = Some(txid);
            }

            self.0[0].data.clone()
        });

        let new_item = SnapshotItem {
            data,
            created_by: txid,
            deleted_by: None,
        };

        self.0.insert(0, new_item);

        (self, old_data)
    }

    pub fn delete(mut self, txid: u64) -> (Self, Option<T>) {
        if !self.0.is_empty() {
            self.0[0].deleted_by = Some(txid);

            let data = self.0[0].data.clone();

            (self, Some(data))
        } else {
            (self, None)
        }
    }

    pub fn rollback(self, txid: u64) -> Option<Self> {
        let items = self
            .0
            .into_iter()
            .filter_map(|mut item| {
                if item.created_by == txid {
                    None
                } else if item.deleted_by == Some(txid) {
                    item.deleted_by = None;

                    Some(item)
                } else {
                    Some(item)
                }
            })
            .collect::<Vec<_>>();

        (!items.is_empty()).then_some(Snapshot(items))
    }

    pub fn extract(self, txid: u64, lock_txid: Option<u64>) -> Option<T> {
        let lock_txid = if Some(txid) == lock_txid {
            None
        } else {
            lock_txid
        };

        for item in self.0 {
            if Some(item.created_by) == lock_txid {
                continue;
            }

            let deleted = item.deleted_by.is_some()
                && item.deleted_by != lock_txid
                && Some(txid) >= item.deleted_by;

            if txid >= item.created_by && !deleted {
                return Some(item.data);
            }
        }

        None
    }

    pub fn get(&self, txid: u64, lock_txid: Option<u64>) -> Option<T> {
        let lock_txid = if Some(txid) == lock_txid {
            None
        } else {
            lock_txid
        };

        for item in self.0.iter() {
            if Some(item.created_by) == lock_txid {
                continue;
            }

            let deleted = item.deleted_by.is_some()
                && item.deleted_by != lock_txid
                && Some(txid) >= item.deleted_by;

            if txid >= item.created_by && !deleted {
                return Some(item.data.clone());
            }
        }

        None
    }

    pub fn gc(self, txid: u64) -> Option<Self> {
        let items = self
            .0
            .into_iter()
            .skip_while(|SnapshotItem { deleted_by, .. }| match deleted_by {
                Some(d_txid) => d_txid <= &txid,
                None => false,
            })
            .collect::<Vec<_>>();

        (!items.is_empty()).then_some(Self(items))
    }
}
