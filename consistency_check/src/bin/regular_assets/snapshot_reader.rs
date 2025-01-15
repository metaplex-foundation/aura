#![allow(dead_code)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::unnecessary_cast)]

// Copyright 2022 Solana Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains code vendored from https://github.com/solana-labs/solana

use bincode::Options;
use memmap2::{Mmap, MmapMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use solana_accounts_db::account_storage::meta::StoredMetaWriteVersion;
use solana_accounts_db::accounts_db::BankHashStats;
use solana_accounts_db::ancestors::AncestorsForSerialization;
use solana_accounts_db::blockhash_queue::BlockhashQueue;
use solana_frozen_abi_macro::AbiExample;
use solana_runtime::epoch_stakes::EpochStakes;
use solana_runtime::stakes::Stakes;
use solana_sdk::account::{Account, AccountSharedData, ReadableAccount};
use solana_sdk::clock::{Epoch, UnixTimestamp};
use solana_sdk::deserialize_utils::default_on_eof;
use solana_sdk::epoch_schedule::EpochSchedule;
use solana_sdk::fee_calculator::{FeeCalculator, FeeRateGovernor};
use solana_sdk::hard_forks::HardForks;
use solana_sdk::hash::Hash;
use solana_sdk::inflation::Inflation;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;
use solana_sdk::stake::state::Delegation;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::str::FromStr;
use std::{io, mem};
use thiserror::Error;
use tracing::info;

use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::{BufReader, Read};
use std::path::{Component, Path};
use std::pin::Pin;
use std::time::Instant;
use tar::{Archive, Entries, Entry};

#[derive(Deserialize)]
pub struct RentCollector {
    pub epoch: Epoch,
    pub epoch_schedule: EpochSchedule,
    pub slots_per_year: f64,
    pub rent: solana_sdk::sysvar::rent::Rent,
}

pub struct ArchiveSnapshotExtractor<Source>
where
    Source: Read + Unpin + 'static,
{
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
    _archive: Pin<Box<Archive<zstd::Decoder<'static, BufReader<Source>>>>>,
    entries: Option<Entries<'static, zstd::Decoder<'static, BufReader<Source>>>>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct AccountsDbFields<T>(
    pub HashMap<Slot, Vec<T>>,
    pub StoredMetaWriteVersion,
    pub Slot,
    pub BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<(Slot, Hash)>,
);

pub type SerializedAppendVecId = usize;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct SerializableAccountStorageEntry {
    pub id: SerializedAppendVecId,
    pub accounts_current_len: usize,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, AbiExample)]
pub struct BankHashInfo {
    pub hash: Hash,
    pub snapshot_hash: Hash,
    pub stats: BankHashStats,
}

#[derive(Default, PartialEq, Eq, Debug, Deserialize)]
struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct DeserializableVersionedBank {
    pub blockhash_queue: BlockhashQueue,
    pub ancestors: AncestorsForSerialization,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub parent_slot: Slot,
    pub hard_forks: HardForks,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub signature_count: u64,
    pub capitalization: u64,
    pub max_tick_height: u64,
    pub hashes_per_tick: Option<u64>,
    pub ticks_per_slot: u64,
    pub ns_per_slot: u128,
    pub genesis_creation_time: UnixTimestamp,
    pub slots_per_year: f64,
    pub accounts_data_len: u64,
    pub slot: Slot,
    pub epoch: Epoch,
    pub block_height: u64,
    pub collector_id: Pubkey,
    pub collector_fees: u64,
    pub fee_calculator: FeeCalculator,
    pub fee_rate_governor: FeeRateGovernor,
    pub collected_rent: u64,
    pub rent_collector: RentCollector,
    pub epoch_schedule: EpochSchedule,
    pub inflation: Inflation,
    pub stakes: Stakes<Delegation>,
    #[allow(dead_code)]
    unused_accounts: UnusedAccounts,
    pub epoch_stakes: HashMap<Epoch, EpochStakes>,
    pub is_delta: bool,
}

const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;

pub fn deserialize_from<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from::<R, T>(reader)
}

fn parse_append_vec_name(name: &OsStr) -> Option<(u64, u64)> {
    let name = name.to_str()?;
    let mut parts = name.splitn(2, '.');
    let slot = u64::from_str(parts.next().unwrap_or(""));
    let id = u64::from_str(parts.next().unwrap_or(""));
    match (slot, id) {
        (Ok(slot), Ok(version)) => Some((slot, version)),
        _ => None,
    }
}

impl<Source> ArchiveSnapshotExtractor<Source>
where
    Source: Read + Unpin + 'static,
{
    pub fn open(source: Source) -> Result<Self> {
        // TODO: make it before call this function
        // let source = File::open(file_path).unwrap();
        let tar_stream = zstd::stream::read::Decoder::new(source)?;
        let mut archive = Box::pin(Archive::new(tar_stream));

        // This is safe as long as we guarantee that entries never gets accessed past drop.
        let archive_static = unsafe { &mut *((&mut *archive) as *mut Archive<_>) };
        let mut entries = archive_static.entries()?;

        // Search for snapshot manifest.
        let mut snapshot_file: Option<Entry<_>> = None;
        for entry in entries.by_ref() {
            let entry = entry?;
            let path = entry.path()?;
            if Self::is_snapshot_manifest_file(&path) {
                snapshot_file = Some(entry);
                break;
            } else if Self::is_appendvec_file(&path) {
                // TODO Support archives where AppendVecs precede snapshot manifests
                return Err(SnapshotError::UnexpectedAppendVec);
            }
        }
        let snapshot_file = snapshot_file.ok_or(SnapshotError::NoSnapshotManifest)?;
        //let snapshot_file_len = snapshot_file.size();
        let snapshot_file_path = snapshot_file.path()?.as_ref().to_path_buf();

        println!("Opening snapshot manifest: {:?}", &snapshot_file_path);
        let mut snapshot_file = BufReader::new(snapshot_file);

        let pre_unpack = Instant::now();
        let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            deserialize_from(&mut snapshot_file)?;
        let accounts_db_fields_post_time = Instant::now();
        drop(snapshot_file);

        println!("Read bank fields in {:?}", versioned_bank_post_time - pre_unpack);
        println!(
            "Read accounts DB fields in {:?}",
            accounts_db_fields_post_time - versioned_bank_post_time
        );

        Ok(ArchiveSnapshotExtractor {
            _archive: archive,
            accounts_db_fields,
            entries: Some(entries),
        })
    }

    fn is_snapshot_manifest_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("snapshots".as_ref())) {
            return false;
        }
        let slot_number_str_1 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        // Check if slot number file is valid u64.
        if slot_number_str_1.to_str().and_then(|s| s.parse::<u64>().ok()).is_none() {
            return false;
        }
        let slot_number_str_2 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        components.next().is_none() && slot_number_str_1 == slot_number_str_2
    }

    fn is_appendvec_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("accounts".as_ref())) {
            return false;
        }
        let name = match components.next() {
            Some(Component::Normal(c)) => c,
            _ => return false,
        };
        components.next().is_none() && parse_append_vec_name(name).is_some()
    }

    fn unboxed_iter(&mut self) -> impl Iterator<Item = Result<AppendVec>> + '_ {
        self.entries.take().into_iter().flatten().filter_map(|entry| {
            let mut entry = match entry {
                Ok(x) => x,
                Err(e) => return Some(Err(e.into())),
            };
            let path = match entry.path() {
                Ok(x) => x,
                Err(e) => return Some(Err(e.into())),
            };
            let (slot, id) = path.file_name().and_then(parse_append_vec_name)?;
            Some(self.process_entry(&mut entry, slot, id))
        })
    }

    fn process_entry(
        &self,
        entry: &mut Entry<'static, zstd::Decoder<'static, BufReader<Source>>>,
        slot: u64,
        id: u64,
    ) -> Result<AppendVec> {
        let known_vecs = self.accounts_db_fields.0.get(&slot).map(|v| &v[..]).unwrap_or(&[]);
        let known_vec = known_vecs.iter().find(|entry| entry.id == (id as usize));
        let known_vec = match known_vec {
            None => return Err(SnapshotError::UnexpectedAppendVec),
            Some(v) => v,
        };
        Ok(AppendVec::new_from_reader(entry, known_vec.accounts_current_len, slot)?)
    }

    pub fn iter(&mut self) -> AppendVecIterator<'_> {
        Box::new(self.unboxed_iter())
    }
}

// Data placement should be aligned at the next boundary. Without alignment accessing the memory may
// crash on some architectures.
pub const ALIGN_BOUNDARY_OFFSET: usize = mem::size_of::<u64>();
macro_rules! u64_align {
    ($addr: expr) => {
        ($addr + (ALIGN_BOUNDARY_OFFSET - 1)) & !(ALIGN_BOUNDARY_OFFSET - 1)
    };
}

pub const MAXIMUM_APPEND_VEC_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024; // 16 GiB

/// Meta contains enough context to recover the index from storage itself
/// This struct will be backed by mmaped and snapshotted data files.
/// So the data layout must be stable and consistent across the entire cluster!
#[repr(C)]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StoredMeta {
    pub write_version: StoredMetaWriteVersion,
    pub data_len: u64,
    pub pubkey: Pubkey,
}

/// This struct will be backed by mmaped and snapshotted data files.
/// So the data layout must be stable and consistent across the entire cluster!
#[repr(C)]
#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct AccountMeta {
    /// lamports in the account
    pub lamports: u64,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
}

impl<'a, T: ReadableAccount> From<&'a T> for AccountMeta {
    fn from(account: &'a T) -> Self {
        Self {
            lamports: account.lamports(),
            owner: *account.owner(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
        }
    }
}

impl<'a, T: ReadableAccount> From<Option<&'a T>> for AccountMeta {
    fn from(account: Option<&'a T>) -> Self {
        match account {
            Some(account) => AccountMeta::from(account),
            None => AccountMeta::default(),
        }
    }
}

/// References to account data stored elsewhere. Getting an `Account` requires cloning
/// (see `StoredAccountMeta::clone_account()`).
#[derive(PartialEq, Eq, Debug)]
pub struct StoredAccountMeta<'a> {
    pub meta: &'a StoredMeta,
    /// account data
    pub account_meta: &'a AccountMeta,
    pub data: &'a [u8],
    pub offset: usize,
    pub stored_size: usize,
    pub hash: &'a Hash,
}

impl<'a> StoredAccountMeta<'a> {
    /// Return a new Account by copying all the data referenced by the `StoredAccountMeta`.
    pub fn clone_account(&self) -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports: self.account_meta.lamports,
            owner: self.account_meta.owner,
            executable: self.account_meta.executable,
            rent_epoch: self.account_meta.rent_epoch,
            data: self.data.to_vec(),
        })
    }
}

/// A thread-safe, file-backed block of memory used to store `Account` instances. Append operations
/// are serialized such that only one thread updates the internal `append_lock` at a time. No
/// restrictions are placed on reading. That is, one may read items from one thread while another
/// is appending new items.
pub struct AppendVec {
    /// A file-backed block of memory that is used to store the data for each appended item.
    map: Mmap,

    /// The number of bytes used to store items, not the number of items.
    current_len: usize,

    /// The number of bytes available for storing items.
    file_size: u64,

    slot: u64,
}

pub fn append_vec_iter(append_vec: Rc<AppendVec>) -> impl Iterator<Item = StoredAccountMetaHandle> {
    let mut offsets = Vec::<usize>::new();
    let mut offset = 0usize;
    loop {
        match append_vec.get_account(offset) {
            None => break,
            Some((_, next_offset)) => {
                offsets.push(offset);
                offset = next_offset;
            },
        }
    }
    let append_vec = Rc::clone(&append_vec);
    offsets
        .into_iter()
        .map(move |offset| StoredAccountMetaHandle::new(Rc::clone(&append_vec), offset))
}

pub struct StoredAccountMetaHandle {
    append_vec: Rc<AppendVec>,
    offset: usize,
}

impl StoredAccountMetaHandle {
    pub fn new(append_vec: Rc<AppendVec>, offset: usize) -> StoredAccountMetaHandle {
        Self { append_vec, offset }
    }

    pub fn access(&self) -> Option<StoredAccountMeta<'_>> {
        Some(self.append_vec.get_account(self.offset)?.0)
    }
}

impl AppendVec {
    fn sanitize_len_and_size(current_len: usize, file_size: usize) -> io::Result<()> {
        if file_size == 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("too small file size {} for AppendVec", file_size),
            ))
        } else if usize::try_from(MAXIMUM_APPEND_VEC_FILE_SIZE)
            .map(|max| file_size > max)
            .unwrap_or(true)
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("too large file size {} for AppendVec", file_size),
            ))
        } else if current_len > file_size {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("current_len is larger than file size ({})", file_size),
            ))
        } else {
            Ok(())
        }
    }

    /// how many more bytes can be stored in this append vec
    pub fn remaining_bytes(&self) -> u64 {
        (self.capacity()).saturating_sub(self.len() as u64)
    }

    pub fn len(&self) -> usize {
        self.current_len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> u64 {
        self.file_size
    }

    pub fn new_from_file<P: AsRef<Path>>(
        path: P,
        current_len: usize,
        slot: u64,
    ) -> io::Result<Self> {
        let data = OpenOptions::new().read(true).write(false).create(false).open(&path)?;

        let file_size = std::fs::metadata(&path)?.len();
        AppendVec::sanitize_len_and_size(current_len, file_size as usize)?;

        let map = unsafe {
            let result = Mmap::map(&data);
            if result.is_err() {
                // for vm.max_map_count, error is: {code: 12, kind: Other, message: "Cannot allocate memory"}
                info!("memory map error: {:?}. This may be because vm.max_map_count is not set correctly.", result);
            }
            result?
        };

        let new = AppendVec { map, current_len, file_size, slot };

        Ok(new)
    }

    pub fn new_from_reader<R: Read>(
        reader: &mut R,
        current_len: usize,
        slot: u64,
    ) -> io::Result<Self> {
        let mut map = MmapMut::map_anon(current_len)?;
        io::copy(&mut reader.take(current_len as u64), &mut map.as_mut())?;
        Ok(AppendVec {
            map: map.make_read_only()?,
            current_len,
            file_size: current_len as u64,
            slot,
        })
    }

    /// Get a reference to the data at `offset` of `size` bytes if that slice
    /// doesn't overrun the internal buffer. Otherwise return None.
    /// Also return the offset of the first byte after the requested data that
    /// falls on a 64-byte boundary.
    fn get_slice(&self, offset: usize, size: usize) -> Option<(&[u8], usize)> {
        let (next, overflow) = offset.overflowing_add(size);
        if overflow || next > self.len() {
            return None;
        }
        let data = &self.map[offset..next];
        let next = u64_align!(next);

        Some((
            //UNSAFE: This unsafe creates a slice that represents a chunk of self.map memory
            //The lifetime of this slice is tied to &self, since it points to self.map memory
            unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, size) },
            next,
        ))
    }

    /// Return a reference to the type at `offset` if its data doesn't overrun the internal buffer.
    /// Otherwise return None. Also return the offset of the first byte after the requested data
    /// that falls on a 64-byte boundary.
    fn get_type<'a, T>(&self, offset: usize) -> Option<(&'a T, usize)> {
        let (data, next) = self.get_slice(offset, mem::size_of::<T>())?;
        let ptr: *const T = data.as_ptr() as *const T;
        //UNSAFE: The cast is safe because the slice is aligned and fits into the memory
        //and the lifetime of the &T is tied to self, which holds the underlying memory map
        Some((unsafe { &*ptr }, next))
    }

    /// Return account metadata for the account at `offset` if its data doesn't overrun
    /// the internal buffer. Otherwise return None. Also return the offset of the first byte
    /// after the requested data that falls on a 64-byte boundary.
    pub fn get_account<'a>(&'a self, offset: usize) -> Option<(StoredAccountMeta<'a>, usize)> {
        let (meta, next): (&'a StoredMeta, _) = self.get_type(offset)?;
        let (account_meta, next): (&'a AccountMeta, _) = self.get_type(next)?;
        let (hash, next): (&'a Hash, _) = self.get_type(next)?;
        let (data, next) = self.get_slice(next, meta.data_len as usize)?;
        let stored_size = next - offset;
        Some((StoredAccountMeta { meta, account_meta, data, offset, stored_size, hash }, next))
    }

    pub fn get_slot(&self) -> u64 {
        self.slot
    }
}

pub type AppendVecIterator<'a> = Box<dyn Iterator<Item = Result<AppendVec>> + 'a>;

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("Failed to deserialize: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("Missing status cache")]
    NoStatusCache,
    #[error("No snapshot manifest file found")]
    NoSnapshotManifest,
    #[error("Unexpected AppendVec")]
    UnexpectedAppendVec,
}

pub type Result<T> = std::result::Result<T, SnapshotError>;
