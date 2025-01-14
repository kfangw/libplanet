using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using Libplanet.Blocks;
using Libplanet.Store;
using Libplanet.Tx;
using LruCacheNet;
using NetMQ;
using RocksDbSharp;
using Serilog;

namespace Libplanet.RocksDBStore
{
    /// <summary>
    /// The <a href="https://rocksdb.org/">RocksDB</a> <see cref="IStore"/> implementation,
    /// which is more production-ready than <see cref="DefaultStore"/>.
    /// This stores data in the RocksDB with multiple partitions under the hood.
    /// </summary>
    /// <seealso cref="IStore"/>
    public class RocksDBStore : BaseStore
    {
        private const string BlockDbRootPathName = "block";
        private const string BlockIndexDbName = "blockindex";
        private const string BlockPerceptionDbName = "blockpercept";
        private const string TxDbRootPathName = "tx";
        private const string TxIndexDbName = "txindex";
        private const string StagedTxDbName = "stagedtx";
        private const string ChainDbName = "chain";
        private const int ForkWriteBatchSize = 100000;

        private static readonly byte[] IndexKeyPrefix = { (byte)'I' };
        private static readonly byte[] BlockKeyPrefix = { (byte)'B' };
        private static readonly byte[] TxKeyPrefix = { (byte)'T' };
        private static readonly byte[] TxNonceKeyPrefix = { (byte)'N' };
        private static readonly byte[] StagedTxKeyPrefix = { (byte)'t' };
        private static readonly byte[] IndexCountKey = { (byte)'c' };
        private static readonly byte[] CanonicalChainIdIdKey = { (byte)'C' };

        private static readonly byte[] EmptyBytes = new byte[0];

        private readonly ILogger _logger;

        private readonly LruCache<TxId, object> _txCache;
        private readonly LruCache<BlockHash, BlockDigest> _blockCache;

        private readonly DbOptions _options;
        private readonly string _path;
        private readonly int _txEpochUnitSeconds;
        private readonly int _blockEpochUnitSeconds;

        private readonly RocksDb _blockIndexDb;
        private readonly RocksDb _blockPerceptionDb;
        private readonly LruCache<string, RocksDb> _blockDbCache;
        private readonly RocksDb _txIndexDb;
        private readonly LruCache<string, RocksDb> _txDbCache;
        private readonly RocksDb _stagedTxDb;
        private readonly RocksDb _chainDb;

        private readonly ReaderWriterLockSlim _rwTxLock;
        private readonly ReaderWriterLockSlim _rwBlockLock;
        private bool _disposed = false;

        /// <summary>
        /// Creates a new <seealso cref="RocksDBStore"/>.
        /// </summary>
        /// <param name="path">The path of the directory where the storage files will be saved.
        /// </param>
        /// <param name="blockCacheSize">The capacity of the block cache.</param>
        /// <param name="txCacheSize">The capacity of the transaction cache.</param>
        /// <param name="statesCacheSize">The capacity of the states cache.</param>
        /// <param name="maxTotalWalSize">The number to configure <c>max_total_wal_size</c> RocksDB
        /// option.</param>
        /// <param name="keepLogFileNum">The number to configure <c>keep_log_file_num</c> RocksDB
        /// option.</param>
        /// <param name="maxLogFileSize">The number to configure <c>max_log_file_size</c>
        /// RocksDB option.</param>
        /// <param name="txEpochUnitSeconds">The interval between epochs of DB partions containing
        /// transactions.  86,400 seconds by default.</param>
        /// <param name="blockEpochUnitSeconds">The interval between epochs of DB partions
        /// containing blocks.  86,400 seconds by default.</param>
        /// <param name="dbConnectionCacheSize">The capacity of the block and transaction
        /// RocksDB connection cache. 100 by default.</param>
        public RocksDBStore(
            string path,
            int blockCacheSize = 512,
            int txCacheSize = 1024,
            ulong? maxTotalWalSize = null,
            ulong? keepLogFileNum = null,
            ulong? maxLogFileSize = null,
            int txEpochUnitSeconds = 86400,
            int blockEpochUnitSeconds = 86400,
            int dbConnectionCacheSize = 100
        )
        {
            _logger = Log.ForContext<RocksDBStore>();

            if (path is null)
            {
                throw new ArgumentNullException(nameof(path));
            }

            path = Path.GetFullPath(path);

            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            _txCache = new LruCache<TxId, object>(capacity: txCacheSize);
            _blockCache = new LruCache<BlockHash, BlockDigest>(capacity: blockCacheSize);

            _path = path;
            _txEpochUnitSeconds = txEpochUnitSeconds > 0
                ? txEpochUnitSeconds
                : throw new ArgumentException(
                    "It must be greater than 0.",
                    nameof(txEpochUnitSeconds));
            _blockEpochUnitSeconds = blockEpochUnitSeconds > 0
                ? blockEpochUnitSeconds
                : throw new ArgumentException(
                    "It must be greater than 0.",
                    nameof(blockEpochUnitSeconds));
            _options = new DbOptions()
                .SetCreateIfMissing();

            if (maxTotalWalSize is ulong maxTotalWalSizeValue)
            {
                _options = _options.SetMaxTotalWalSize(maxTotalWalSizeValue);
            }

            if (keepLogFileNum is ulong keepLogFileNumValue)
            {
                _options = _options.SetKeepLogFileNum(keepLogFileNumValue);
            }

            if (maxLogFileSize is ulong maxLogFileSizeValue)
            {
                _options = _options.SetMaxLogFileSize(maxLogFileSizeValue);
            }

            _blockIndexDb = RocksDBUtils.OpenRocksDb(_options, BlockDbPath(BlockIndexDbName));
            _blockPerceptionDb =
                RocksDBUtils.OpenRocksDb(_options, RocksDbPath(BlockPerceptionDbName));
            _txIndexDb = RocksDBUtils.OpenRocksDb(_options, TxDbPath(TxIndexDbName));
            _stagedTxDb = RocksDBUtils.OpenRocksDb(_options, RocksDbPath(StagedTxDbName));

            // When opening a DB in a read-write mode, you need to specify all Column Families that
            // currently exist in a DB. https://github.com/facebook/rocksdb/wiki/Column-Families
            var chainDbColumnFamilies = GetColumnFamilies(_options, ChainDbName);
            _chainDb = RocksDBUtils.OpenRocksDb(
                _options, RocksDbPath(ChainDbName), chainDbColumnFamilies);

            _rwTxLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _rwBlockLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

            _blockDbCache = new LruCache<string, RocksDb>(dbConnectionCacheSize);
            _blockDbCache.SetPreRemoveDataMethod(db =>
            {
                db.Dispose();
                return true;
            });
            _txDbCache = new LruCache<string, RocksDb>(dbConnectionCacheSize);
            _txDbCache.SetPreRemoveDataMethod(db =>
            {
                db.Dispose();
                return true;
            });
        }

        /// <inheritdoc/>
        public override IEnumerable<Guid> ListChainIds()
        {
            string path = Path.Combine(_path, ChainDbName);

            foreach (string name in RocksDb.ListColumnFamilies(_options, path))
            {
                Guid guid;

                try
                {
                    guid = Guid.Parse(name);
                }
                catch (FormatException)
                {
                    continue;
                }

                yield return guid;
            }
        }

        /// <inheritdoc/>
        public override void DeleteChainId(Guid chainId)
        {
            _logger.Debug($"Deleting chainID: {chainId}.");

            var cfName = chainId.ToString();
            try
            {
                _chainDb.DropColumnFamily(cfName);
            }
            catch (KeyNotFoundException)
            {
                // Do nothing according to the specification: DeleteChainId() should be idempotent.
                _logger.Debug($"No such chain ID in _chainDb: {cfName}.", cfName);
            }
        }

        /// <inheritdoc />
        public override Guid? GetCanonicalChainId()
        {
            byte[] bytes = _chainDb.Get(CanonicalChainIdIdKey);

            return bytes is null
                ? (Guid?)null
                : new Guid(bytes);
        }

        /// <inheritdoc />
        public override void SetCanonicalChainId(Guid chainId)
        {
            byte[] bytes = chainId.ToByteArray();
            _chainDb.Put(CanonicalChainIdIdKey, bytes);
        }

        /// <inheritdoc/>
        public override long CountIndex(Guid chainId)
        {
            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
            byte[] bytes = _chainDb.Get(IndexCountKey, cf);
            return bytes is null
                ? 0
                : RocksDBStoreBitConverter.ToInt64(bytes);
        }

        /// <inheritdoc cref="BaseStore.IterateIndexes(Guid, int, int?)"/>
        public override IEnumerable<BlockHash> IterateIndexes(Guid chainId, int offset, int? limit)
        {
            int count = 0;
            byte[] prefix = IndexKeyPrefix;

            foreach (Iterator it in IterateDb(_chainDb, prefix, chainId).Skip(offset))
            {
                if (count >= limit)
                {
                    break;
                }

                byte[] value = it.Value();
                yield return new BlockHash(value);

                count += 1;
            }
        }

        /// <inheritdoc cref="BaseStore.IndexBlockHash(Guid, long)"/>
        public override BlockHash? IndexBlockHash(Guid chainId, long index)
        {
            if (index < 0)
            {
                index += CountIndex(chainId);

                if (index < 0)
                {
                    return null;
                }
            }

            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);

            byte[] indexBytes = RocksDBStoreBitConverter.GetBytes(index);

            byte[] key = IndexKeyPrefix.Concat(indexBytes).ToArray();
            byte[] bytes = _chainDb.Get(key, cf);
            return bytes is null ? (BlockHash?)null : new BlockHash(bytes);
        }

        /// <inheritdoc cref="BaseStore.AppendIndex(Guid, BlockHash)"/>
        public override long AppendIndex(Guid chainId, BlockHash hash)
        {
            long index = CountIndex(chainId);

            byte[] indexBytes = RocksDBStoreBitConverter.GetBytes(index);

            byte[] key = IndexKeyPrefix.Concat(indexBytes).ToArray();
            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);

            using var writeBatch = new WriteBatch();

            writeBatch.Put(key, hash.ToByteArray(), cf);
            writeBatch.Put(IndexCountKey, RocksDBStoreBitConverter.GetBytes(index + 1), cf);

            _chainDb.Write(writeBatch);

            return index;
        }

        /// <inheritdoc cref="BaseStore.ForkBlockIndexes(Guid, Guid, BlockHash)"/>
        public override void ForkBlockIndexes(
            Guid sourceChainId,
            Guid destinationChainId,
            BlockHash branchpoint
        )
        {
            BlockHash? genesisHash = IterateIndexes(sourceChainId, 0, 1).FirstOrDefault();

            if (genesisHash is null || branchpoint.Equals(genesisHash))
            {
                return;
            }

            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, destinationChainId);
            var writeBatch = new WriteBatch();
            long index = 0;
            try
            {
                foreach (Iterator it in IterateDb(_chainDb, IndexKeyPrefix, sourceChainId))
                {
                    byte[] hashBytes = it.Value();
                    writeBatch.Put(it.Key(), hashBytes, cf);
                    index += 1;

                    if (writeBatch.Count() >= ForkWriteBatchSize)
                    {
                        _chainDb.Write(writeBatch);
                        writeBatch.Dispose();
                        writeBatch = new WriteBatch();
                    }

                    if (branchpoint.ByteArray.SequenceEqual(hashBytes))
                    {
                        break;
                    }
                }
            }
            finally
            {
                _chainDb.Write(writeBatch);
                writeBatch.Dispose();
            }

            _chainDb.Put(
                IndexCountKey,
                RocksDBStoreBitConverter.GetBytes(index),
                cf
            );
        }

        /// <inheritdoc/>
        public override void StageTransactionIds(IImmutableSet<TxId> txids)
        {
            foreach (TxId txId in txids)
            {
                byte[] key = StagedTxKey(txId);
                _stagedTxDb.Put(key, EmptyBytes);
            }
        }

        /// <inheritdoc/>
        public override void UnstageTransactionIds(ISet<TxId> txids)
        {
            foreach (TxId txId in txids)
            {
                byte[] key = StagedTxKey(txId);
                _stagedTxDb.Remove(key);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<TxId> IterateStagedTransactionIds()
        {
            byte[] prefix = StagedTxKeyPrefix;
            foreach (var it in IterateDb(_stagedTxDb, prefix))
            {
                byte[] key = it.Key();
                byte[] txIdBytes = key.Skip(prefix.Length).ToArray();
                yield return new TxId(txIdBytes);
            }
        }

        /// <inheritdoc/>
        public override IEnumerable<TxId> IterateTransactionIds()
        {
            byte[] prefix = TxKeyPrefix;

            foreach (Iterator it in IterateDb(_txIndexDb, prefix) )
            {
                byte[] key = it.Key();
                byte[] txIdBytes = key.Skip(prefix.Length).ToArray();

                var txId = new TxId(txIdBytes);
                yield return txId;
            }
        }

        /// <inheritdoc/>
        public override Transaction<T> GetTransaction<T>(TxId txid)
        {
            if (_txCache.TryGetValue(txid, out object cachedTx))
            {
                return (Transaction<T>)cachedTx;
            }

            byte[] key = TxKey(txid);
            if (!(_txIndexDb.Get(key) is byte[] txDbNameBytes))
            {
                return null;
            }

            string txDbName = RocksDBStoreBitConverter.GetString(txDbNameBytes);
            _rwTxLock.EnterReadLock();
            try
            {
                if (!_txDbCache.TryGetValue(txDbName, out RocksDb txDb))
                {
                    txDb = RocksDBUtils.OpenRocksDb(_options, TxDbPath(txDbName));
                    _txDbCache.AddOrUpdate(txDbName, txDb);
                }

                byte[] txBytes = txDb.Get(key);

                Transaction<T> tx = Transaction<T>.Deserialize(txBytes, false);
                _txCache.AddOrUpdate(txid, tx);
                return tx;
            }
            finally
            {
                _rwTxLock.ExitReadLock();
            }
        }

        /// <inheritdoc/>
        public override void PutTransaction<T>(Transaction<T> tx)
        {
            if (_txCache.ContainsKey(tx.Id))
            {
                return;
            }

            byte[] key = TxKey(tx.Id);
            if (!(_txIndexDb.Get(key) is null))
            {
                return;
            }

            long timestamp = tx.Timestamp.ToUnixTimeSeconds();
            string txDbName = $"epoch{(int)timestamp / _txEpochUnitSeconds}";
            _rwTxLock.EnterWriteLock();
            try
            {
                if (!_txDbCache.TryGetValue(txDbName, out RocksDb txDb))
                {
                    txDb = RocksDBUtils.OpenRocksDb(_options, TxDbPath(txDbName));
                    _txDbCache.AddOrUpdate(txDbName, txDb);
                }

                txDb.Put(key, tx.Serialize(true));
                _txIndexDb.Put(key, RocksDBStoreBitConverter.GetBytes(txDbName));
                _txCache.AddOrUpdate(tx.Id, tx);
            }
            finally
            {
                _rwTxLock.ExitWriteLock();
            }
        }

        /// <inheritdoc/>
        public override bool DeleteTransaction(TxId txid)
        {
            byte[] key = TxKey(txid);

            if (!(_txIndexDb.Get(key) is byte[] txDbNameBytes))
            {
                return false;
            }

            _rwTxLock.EnterWriteLock();
            try
            {
                string txDbName = RocksDBStoreBitConverter.GetString(txDbNameBytes);
                if (!_txDbCache.TryGetValue(txDbName, out RocksDb txDb))
                {
                    txDb = RocksDBUtils.OpenRocksDb(_options, TxDbPath(txDbName));
                    _txDbCache.AddOrUpdate(txDbName, txDb);
                }

                _txCache.Remove(txid);
                _txIndexDb.Remove(key);
                txDb.Remove(key);

                return true;
            }
            finally
            {
                _rwTxLock.ExitWriteLock();
            }
        }

        /// <inheritdoc/>
        public override bool ContainsTransaction(TxId txId)
        {
            if (_txCache.ContainsKey(txId))
            {
                return true;
            }

            byte[] key = TxKey(txId);

            return !(_txIndexDb.Get(key) is null);
        }

        /// <inheritdoc cref="BaseStore.IterateBlockHashes()"/>
        public override IEnumerable<BlockHash> IterateBlockHashes()
        {
            byte[] prefix = BlockKeyPrefix;

            foreach (Iterator it in IterateDb(_blockIndexDb, prefix))
            {
                byte[] key = it.Key();
                byte[] hashBytes = key.Skip(prefix.Length).ToArray();
                yield return new BlockHash(hashBytes);
            }
        }

        /// <inheritdoc cref="BaseStore.GetBlockDigest(BlockHash)"/>
        public override BlockDigest? GetBlockDigest(BlockHash blockHash)
        {
            if (_blockCache.TryGetValue(blockHash, out BlockDigest cachedDigest))
            {
                return cachedDigest;
            }

            byte[] key = BlockKey(blockHash);
            if (!(_blockIndexDb.Get(key) is byte[] blockDbNameBytes))
            {
                return null;
            }

            _rwBlockLock.EnterReadLock();
            try
            {
                string blockDbName = RocksDBStoreBitConverter.GetString(blockDbNameBytes);
                if (!_blockDbCache.TryGetValue(blockDbName, out RocksDb blockDb))
                {
                    blockDb = RocksDBUtils.OpenRocksDb(_options, BlockDbPath(blockDbName));
                    _blockDbCache.AddOrUpdate(blockDbName, blockDb);
                }

                byte[] blockBytes = blockDb.Get(key);

                BlockDigest blockDigest = BlockDigest.Deserialize(blockBytes);

                _blockCache.AddOrUpdate(blockHash, blockDigest);
                return blockDigest;
            }
            finally
            {
                _rwBlockLock.ExitReadLock();
            }
        }

        /// <inheritdoc/>
        public override void PutBlock<T>(Block<T> block)
        {
            if (_blockCache.ContainsKey(block.Hash))
            {
                return;
            }

            byte[] key = BlockKey(block.Hash);

            if (!(_blockIndexDb.Get(key) is null))
            {
                return;
            }

            long timestamp = block.Timestamp.ToUnixTimeSeconds();

            foreach (Transaction<T> tx in block.Transactions)
            {
                PutTransaction(tx);
            }

            _rwBlockLock.EnterWriteLock();
            try
            {
                string blockDbName = $"epoch{timestamp / _blockEpochUnitSeconds}";
                if (!_blockDbCache.TryGetValue(blockDbName, out RocksDb blockDb))
                {
                    blockDb = RocksDBUtils.OpenRocksDb(_options, BlockDbPath(blockDbName));
                    _blockDbCache.AddOrUpdate(blockDbName, blockDb);
                }

                byte[] value = block.ToBlockDigest().Serialize();
                blockDb.Put(key, value);
                _blockIndexDb.Put(key, RocksDBStoreBitConverter.GetBytes(blockDbName));
                _blockCache.AddOrUpdate(block.Hash, block.ToBlockDigest());
            }
            finally
            {
                _rwBlockLock.ExitWriteLock();
            }
        }

        /// <inheritdoc cref="BaseStore.DeleteBlock(BlockHash)"/>
        public override bool DeleteBlock(BlockHash blockHash)
        {
            byte[] key = BlockKey(blockHash);

            if (!(_blockIndexDb.Get(key) is byte[] blockDbNameByte))
            {
                return false;
            }

            _rwBlockLock.EnterWriteLock();
            try
            {
                string blockDbName = RocksDBStoreBitConverter.GetString(blockDbNameByte);
                if (!_blockDbCache.TryGetValue(blockDbName, out RocksDb blockDb))
                {
                    blockDb = RocksDBUtils.OpenRocksDb(_options, BlockDbPath(blockDbName));
                    _blockDbCache.AddOrUpdate(blockDbName, blockDb);
                }

                _blockCache.Remove(blockHash);
                _blockIndexDb.Remove(key);
                blockDb.Remove(key);
                return true;
            }
            finally
            {
                _rwBlockLock.ExitWriteLock();
            }
        }

        /// <inheritdoc cref="BaseStore.ContainsBlock(BlockHash)"/>
        public override bool ContainsBlock(BlockHash blockHash)
        {
            if (_blockCache.ContainsKey(blockHash))
            {
                return true;
            }

            byte[] key = BlockKey(blockHash);

            return !(_blockIndexDb.Get(key) is null);
        }

        /// <inheritdoc cref="BaseStore.SetBlockPerceivedTime(BlockHash, DateTimeOffset)"/>
        public override void SetBlockPerceivedTime(
            BlockHash blockHash,
            DateTimeOffset perceivedTime
        )
        {
            byte[] key = BlockKey(blockHash);
            _blockPerceptionDb.Put(
                key,
                NetworkOrderBitsConverter.GetBytes(perceivedTime.ToUnixTimeMilliseconds())
            );
        }

        /// <inheritdoc cref="BaseStore.GetBlockPerceivedTime(BlockHash)"/>
        public override DateTimeOffset? GetBlockPerceivedTime(BlockHash blockHash)
        {
            byte[] key = BlockKey(blockHash);
            if (_blockPerceptionDb.Get(key) is { } bytes)
            {
                long unixTimeMs = NetworkOrderBitsConverter.ToInt64(bytes);
                return DateTimeOffset.FromUnixTimeMilliseconds(unixTimeMs);
            }

            return null;
        }

        /// <inheritdoc/>
        public override IEnumerable<KeyValuePair<Address, long>> ListTxNonces(Guid chainId)
        {
            byte[] prefix = TxNonceKeyPrefix;

            foreach (Iterator it in IterateDb(_chainDb, prefix, chainId))
            {
                byte[] addressBytes = it.Key()
                    .Skip(prefix.Length)
                    .ToArray();
                var address = new Address(addressBytes);
                long nonce = RocksDBStoreBitConverter.ToInt64(it.Value());
                yield return new KeyValuePair<Address, long>(address, nonce);
            }
        }

        /// <inheritdoc/>
        public override long GetTxNonce(Guid chainId, Address address)
        {
            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
            byte[] key = TxNonceKey(address);
            byte[] bytes = _chainDb.Get(key, cf);

            return bytes is null
                ? 0
                : RocksDBStoreBitConverter.ToInt64(bytes);
        }

        /// <inheritdoc/>
        public override void IncreaseTxNonce(Guid chainId, Address signer, long delta = 1)
        {
            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
            long nextNonce = GetTxNonce(chainId, signer) + delta;

            byte[] key = TxNonceKey(signer);
            byte[] bytes = RocksDBStoreBitConverter.GetBytes(nextNonce);

            _chainDb.Put(key, bytes, cf);
        }

        /// <inheritdoc/>
        public override long CountTransactions()
        {
            return IterateTransactionIds().LongCount();
        }

        /// <inheritdoc/>
        public override long CountBlocks()
        {
            return IterateBlockHashes().LongCount();
        }

        public override void Dispose()
        {
            if (!_disposed)
            {
                _chainDb?.Dispose();
                _txIndexDb?.Dispose();
                _blockIndexDb?.Dispose();
                _blockPerceptionDb?.Dispose();
                _stagedTxDb?.Dispose();
                foreach (var db in _txDbCache.Values)
                {
                    db.Dispose();
                }

                _txDbCache.Clear();

                foreach (var db in _blockDbCache.Values)
                {
                    db.Dispose();
                }

                _blockDbCache.Clear();
                _disposed = true;
            }
        }

        /// <inheritdoc/>
        public override void ForkTxNonces(Guid sourceChainId, Guid destinationChainId)
        {
            byte[] prefix = TxNonceKeyPrefix;
            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, destinationChainId);
            var writeBatch = new WriteBatch();
            try
            {
                foreach (Iterator it in IterateDb(_chainDb, prefix, sourceChainId))
                {
                    writeBatch.Put(it.Key(), it.Value(), cf);
                    if (writeBatch.Count() >= ForkWriteBatchSize)
                    {
                        _chainDb.Write(writeBatch);
                        writeBatch.Dispose();
                        writeBatch = new WriteBatch();
                    }
                }
            }
            finally
            {
                _chainDb.Write(writeBatch);
                writeBatch.Dispose();
            }
        }

        private byte[] BlockKey(in BlockHash blockHash) =>
            BlockKeyPrefix.Concat(blockHash.ByteArray).ToArray();

        private byte[] TxKey(in TxId txId) =>
            TxKeyPrefix.Concat(txId.ByteArray).ToArray();

        private byte[] TxNonceKey(in Address address) =>
            TxNonceKeyPrefix.Concat(address.ByteArray).ToArray();

        private byte[] StagedTxKey(in TxId txId)
        {
            return StagedTxKeyPrefix.Concat(txId.ToByteArray()).ToArray();
        }

        private IEnumerable<Iterator> IterateDb(RocksDb db, byte[] prefix, Guid? chainId = null)
        {
            ColumnFamilyHandle cf = GetColumnFamily(db, chainId);
            using Iterator it = db.NewIterator(cf);
            for (it.Seek(prefix); it.Valid() && it.Key().StartsWith(prefix); it.Next())
            {
                yield return it;
            }
        }

        private ColumnFamilyHandle GetColumnFamily(RocksDb db, Guid? chainId = null)
        {
            if (chainId is null)
            {
                return null;
            }

            var cfName = chainId.ToString();

            ColumnFamilyHandle cf;
            try
            {
                cf = db.GetColumnFamily(cfName);
            }
            catch (KeyNotFoundException)
            {
                cf = db.CreateColumnFamily(_options, cfName);
            }

            return cf;
        }

        private ColumnFamilies GetColumnFamilies(DbOptions options, string dbName)
        {
            var dbPath = Path.Combine(_path, dbName);
            var columnFamilies = new ColumnFamilies();
            List<string> listColumnFamilies;

            try
            {
                listColumnFamilies = RocksDb.ListColumnFamilies(options, dbPath).ToList();
            }
            catch (RocksDbException)
            {
                listColumnFamilies = new List<string>();
            }

            foreach (string name in listColumnFamilies)
            {
                columnFamilies.Add(name, _options);
            }

            return columnFamilies;
        }

        private string TxDbPath(string dbName) =>
            Path.Combine(RocksDbPath(TxDbRootPathName), dbName);

        private string BlockDbPath(string dbName) =>
            Path.Combine(RocksDbPath(BlockDbRootPathName), dbName);

        private string RocksDbPath(string dbName) => Path.Combine(_path, dbName);
    }
}
