using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Bencodex;
using Bencodex.Types;
using Libplanet;
using Libplanet.Action;
using Libplanet.Assets;
using Libplanet.Blocks;
using Libplanet.DataBase;
using Libplanet.Tx;
using LruCacheNet;
using NetMQ;
using Serilog;
using FAV = Libplanet.Assets.FungibleAssetValue;

namespace Libplanet.Store
{
    /// <summary>
    /// Common code for several <see cref="IStore"/> implementations.
    /// </summary>
    public class Store : IStore, IDisposable
    {
        private const int ForkWriteBatchSize = 100000;

        private static readonly byte[] IndexKeyPrefix = { 0x01 };
        private static readonly byte[] BlockKeyPrefix = { 0x02 };
        private static readonly byte[] TxKeyPrefix = { 0x03 };
        private static readonly byte[] TxNonceKeyPrefix = { 0x04 };
        private static readonly byte[] StagedTxKeyPrefix = { 0x05 };
        private static readonly byte[] TxExecutionKeyPrefix = { 0x06 };
        private static readonly byte[] IndexCountKey = { 0x07 };
        private static readonly byte[] CanonicalChainIdIdKey = { 0x08 };
        private static readonly byte[] PreviousChainIdKey = { 0x09 };
        private static readonly byte[] PreviousChainIndexKey = { 0x0a };
        private static readonly byte[] ForkedChainsKeyPrefix = { 0x0b };
        private static readonly byte[] DeletedKey = { 0x0c };

        private static readonly byte[] EmptyBytes = new byte[0];

        private static readonly Codec Codec = new Codec();

        private readonly ILogger _logger;

        private readonly LruCache<TxId, object> _txCache;
        private readonly LruCache<BlockHash, BlockDigest> _blockCache;

        private readonly IDataBase _db;

        private readonly ReaderWriterLockSlim _rwTxLock;
        private readonly ReaderWriterLockSlim _rwBlockLock;
        private bool _disposed = false;

        public Store(
            IDataBase db,
            int blockCacheSize = 512,
            int txCacheSize = 1024
        )
        {
            _logger = Log.ForContext<Store>();

            _db = db;

            _txCache = new LruCache<TxId, object>(capacity: txCacheSize);
            _blockCache = new LruCache<BlockHash, BlockDigest>(capacity: blockCacheSize);

            _rwTxLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _rwBlockLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        /// <summary>
        /// Get <c>long</c> representation of the <paramref name="value"/>.
        /// </summary>
        /// <param name="value">The Big-endian byte-array value to convert to <c>long</c>.</param>
        /// <returns>The <c>long</c> representation of the <paramref name="value"/>.</returns>
        public static long ToInt64(byte[] value)
        {
            byte[] bytes = new byte[sizeof(long)];
            value.CopyTo(bytes, 0);

            // Use Big-endian to order index lexicographically.
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt64(bytes, 0);
        }

        /// <summary>
        /// Get <c>string</c> representation of the <paramref name="value"/>.
        /// </summary>
        /// <param name="value">The byte-array value to convert to <c>string</c>.</param>
        /// <returns>The <c>string</c> representation of the <paramref name="value"/>.</returns>
        public static string GetString(byte[] value)
        {
            return Encoding.UTF8.GetString(value);
        }

        /// <summary>
        /// Get Big-endian byte-array representation of the <paramref name="value"/>.
        /// </summary>
        /// <param name="value">The <c>long</c> value to convert to byte-array.</param>
        /// <returns>The Big-endian byte-array representation of the <paramref name="value"/>.
        /// </returns>
        public static byte[] GetBytes(long value)
        {
            byte[] bytes = BitConverter.GetBytes(value);

            // Use Big-endian to order index lexicographically.
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }

        /// <summary>
        /// Get encoded byte-array representation of the <paramref name="value"/>.
        /// </summary>
        /// <param name="value">The <c>string</c> to convert to byte-array.</param>
        /// <returns>The encoded representation of the <paramref name="value"/>.</returns>
        public static byte[] GetBytes(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        /// <inheritdoc />
        public Guid? GetCanonicalChainId()
        {
            try
            {
                var bytes = _db.Get(CanonicalChainIdIdKey);

                return bytes is null
                    ? (Guid?)null
                    : new Guid(bytes);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(GetCanonicalChainId), e);
            }

            return null;
        }

        /// <inheritdoc />
        public void SetCanonicalChainId(Guid chainId)
        {
            try
            {
                var bytes = chainId.ToByteArray();
                _db.Put(CanonicalChainIdIdKey, bytes);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(SetCanonicalChainId), e);
            }
        }

        /// <inheritdoc/>
        public IEnumerable<Guid> ListChainIds()
        {
            //TODO: how to list the chain id?
            yield break;
        }

        /// <inheritdoc/>
        public void DeleteChainId(Guid chainId)
        {
            //TODO: do we need this?
        }

        /// <inheritdoc cref="BaseStore.IndexBlockHash(Guid, long)"/>
        public override BlockHash? IndexBlockHash(Guid chainId, long index)
            => IndexBlockHash(chainId, index, false);

        /// <inheritdoc cref="BaseStore.AppendIndex(Guid, BlockHash)"/>
        public long AppendIndex(Guid chainId, BlockHash hash)
        {
            var index = CountIndex(chainId);
            try
            {
                var indexBytes = GetBytes(index);

                var key = IndexKeyPrefix.Concat(indexBytes).ToArray();
                ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);

                using var writeBatch = new WriteBatch();

                writeBatch.Put(key, hash.ToByteArray(), cf);
                writeBatch.Put(IndexCountKey, RocksDBStoreBitConverter.GetBytes(index + 1), cf);

                _chainDb.Write(writeBatch);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(AppendIndex), e);
            }

            return index;
        }

        /// <inheritdoc/>
        public long CountIndex(Guid chainId)
        {
            try
            {
                ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
                byte[] bytes = _chainDb.Get(IndexCountKey, cf);
                return bytes is null
                    ? 0
                    : RocksDBStoreBitConverter.ToInt64(bytes);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(CountIndex), e);
            }

            return 0;
        }

        /// <inheritdoc cref="BaseStore.IterateIndexes(Guid, int, int?)"/>
        public override IEnumerable<BlockHash> IterateIndexes(Guid chainId, int offset, int? limit)
            => IterateIndexes(chainId, offset, limit, false);

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

            ColumnFamilyHandle srcCf = GetColumnFamily(_chainDb, sourceChainId);
            ColumnFamilyHandle destCf = GetColumnFamily(_chainDb, destinationChainId);
            foreach (Iterator k in IterateDb(_chainDb, IndexKeyPrefix, destinationChainId))
            {
                _chainDb.Remove(k.Key(), destCf);
            }

            long bpIndex = GetBlockIndex(branchpoint).Value;

            if (GetPreviousChainInfo(srcCf) is { } chainInfo &&
                chainInfo.Item2 == bpIndex)
            {
                ForkBlockIndexes(chainInfo.Item1, destinationChainId, branchpoint);
                return;
            }

            _chainDb.Put(PreviousChainIdKey, sourceChainId.ToByteArray(), destCf);
            _chainDb.Put(
                PreviousChainIndexKey,
                RocksDBStoreBitConverter.GetBytes(bpIndex),
                destCf
            );
            _chainDb.Put(
                IndexCountKey,
                RocksDBStoreBitConverter.GetBytes(bpIndex + 1),
                destCf
            );
            AddFork(srcCf, destinationChainId);
        }

        /// <inheritdoc/>
        public override void StageTransactionIds(IImmutableSet<TxId> txids)
        {
            try
            {
                foreach (TxId txId in txids)
                {
                    byte[] key = StagedTxKey(txId);
                    _stagedTxDb.Put(key, EmptyBytes);
                }
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(StageTransactionIds), e);
            }
        }

        /// <inheritdoc/>
        public override void UnstageTransactionIds(ISet<TxId> txids)
        {
            try
            {
                foreach (TxId txId in txids)
                {
                    byte[] key = StagedTxKey(txId);
                    _stagedTxDb.Remove(key);
                }
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(UnstageTransactionIds), e);
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

        Transaction<T> IStore.GetTransaction<T>(TxId txid)
        {
            throw new NotImplementedException();
        }

        void IStore.PutTransaction<T>(Transaction<T> tx)
        {
            PutTransaction(tx);
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(GetTransaction), e);
                return null;
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(PutTransaction), e);
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(DeleteTransaction), e);
            }
            finally
            {
                _rwTxLock.ExitWriteLock();
            }

            return false;
        }

        /// <inheritdoc/>
        public override bool ContainsTransaction(TxId txId)
        {
            try
            {
                if (_txCache.ContainsKey(txId))
                {
                    return true;
                }

                byte[] key = TxKey(txId);

                return !(_txIndexDb.Get(key) is null);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(ContainsTransaction), e);
            }

            return false;
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

        public Block<T> GetBlock<T>(BlockHash blockHash) where T : IAction, new()
        {
            throw new NotImplementedException();
        }

        public long? GetBlockIndex(BlockHash blockHash)
        {
            throw new NotImplementedException();
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(GetBlockDigest), e);
            }
            finally
            {
                _rwBlockLock.ExitReadLock();
            }

            return null;
        }

        void IStore.PutBlock<T>(Block<T> block)
        {
            PutBlock(block);
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(PutBlock), e);
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(DeleteBlock), e);
            }
            finally
            {
                _rwBlockLock.ExitWriteLock();
            }

            return false;
        }

        /// <inheritdoc cref="BaseStore.ContainsBlock(BlockHash)"/>
        public override bool ContainsBlock(BlockHash blockHash)
        {
            try
            {
                if (_blockCache.ContainsKey(blockHash))
                {
                    return true;
                }

                byte[] key = BlockKey(blockHash);

                return !(_blockIndexDb.Get(key) is null);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(ContainsBlock), e);
            }

            return false;
        }

        /// <inheritdoc cref="BaseStore.PutTxExecution(Libplanet.Tx.TxSuccess)"/>
        public override void PutTxExecution(TxSuccess txSuccess) =>
            _txExecutionDb.Put(
                TxExecutionKey(txSuccess),
                Codec.Encode(SerializeTxExecution(txSuccess))
            );

        /// <inheritdoc cref="BaseStore.PutTxExecution(Libplanet.Tx.TxFailure)"/>
        public override void PutTxExecution(TxFailure txFailure) =>
            _txExecutionDb.Put(
                TxExecutionKey(txFailure),
                Codec.Encode(SerializeTxExecution(txFailure))
            );

        /// <inheritdoc cref="BaseStore.GetTxExecution(BlockHash, TxId)"/>
        public override TxExecution GetTxExecution(BlockHash blockHash, TxId txid)
        {
            byte[] key = TxExecutionKey(blockHash, txid);
            if (_txExecutionDb.Get(key) is { } bytes)
            {
                return DeserializeTxExecution(blockHash, txid, Codec.Decode(bytes), _logger);
            }

            return null;
        }

        /// <inheritdoc cref="BaseStore.SetBlockPerceivedTime(BlockHash, DateTimeOffset)"/>
        public override void SetBlockPerceivedTime(
            BlockHash blockHash,
            DateTimeOffset perceivedTime
        )
        {
            try
            {
                byte[] key = BlockKey(blockHash);
                _blockPerceptionDb.Put(
                    key,
                    NetworkOrderBitsConverter.GetBytes(perceivedTime.ToUnixTimeMilliseconds())
                );
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(SetBlockPerceivedTime), e);
            }
        }

        /// <inheritdoc cref="BaseStore.GetBlockPerceivedTime(BlockHash)"/>
        public override DateTimeOffset? GetBlockPerceivedTime(BlockHash blockHash)
        {
            try
            {
                byte[] key = BlockKey(blockHash);
                if (_blockPerceptionDb.Get(key) is { } bytes)
                {
                    long unixTimeMs = NetworkOrderBitsConverter.ToInt64(bytes);
                    return DateTimeOffset.FromUnixTimeMilliseconds(unixTimeMs);
                }
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(GetBlockPerceivedTime), e);
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
            try
            {
                ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
                byte[] key = TxNonceKey(address);
                byte[] bytes = _chainDb.Get(key, cf);
                return bytes is null
                    ? 0
                    : RocksDBStoreBitConverter.ToInt64(bytes);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(GetTxNonce), e);
            }

            return 0;
        }

        /// <inheritdoc/>
        public override void IncreaseTxNonce(Guid chainId, Address signer, long delta = 1)
        {
            try
            {
                ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
                long nextNonce = GetTxNonce(chainId, signer) + delta;

                byte[] key = TxNonceKey(signer);
                byte[] bytes = RocksDBStoreBitConverter.GetBytes(nextNonce);

                _chainDb.Put(key, bytes, cf);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(IncreaseTxNonce), e);
            }
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
            try
            {
                if (!_disposed)
                {
                    _chainDb?.Dispose();
                    _txIndexDb?.Dispose();
                    _blockIndexDb?.Dispose();
                    _blockPerceptionDb?.Dispose();
                    _stagedTxDb?.Dispose();
                    _txExecutionDb?.Dispose();
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(Dispose), e);
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
            catch (Exception e)
            {
                LogUnexpectedException(nameof(ForkTxNonces), e);
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

        private byte[] StagedTxKey(in TxId txId) =>
            StagedTxKeyPrefix.Concat(txId.ToByteArray()).ToArray();

        private byte[] TxExecutionKey(in BlockHash blockHash, in TxId txId) =>

            // As BlockHash is not fixed size, place TxId first.
            TxExecutionKeyPrefix.Concat(txId.ByteArray).Concat(blockHash.ByteArray).ToArray();

        private byte[] TxExecutionKey(TxExecution txExecution) =>
            TxExecutionKey(txExecution.BlockHash, txExecution.TxId);

        private byte[] ForkedChainsKey(Guid guid) =>
            ForkedChainsKeyPrefix.Concat(guid.ToByteArray()).ToArray();

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

        private void LogUnexpectedException(string methodName, Exception e)
        {
            string msg = $"An unexpected exception occurred on {methodName}: {{Message}}";
            _logger.Error(e, msg, e.Message);
        }

        private (Guid, long)? GetPreviousChainInfo(ColumnFamilyHandle cf)
        {
            if (_chainDb.Get(PreviousChainIdKey, cf) is { } prevChainId &&
                _chainDb.Get(PreviousChainIndexKey, cf) is { } prevChainIndex)
            {
                return (new Guid(prevChainId), RocksDBStoreBitConverter.ToInt64(prevChainIndex));
            }

            return null;
        }

        private IEnumerable<BlockHash> IterateIndexes(
            Guid chainId,
            int offset,
            int? limit,
            bool includeDeleted
        )
        {
            ColumnFamilyHandle cf = GetColumnFamily(_chainDb, chainId);
            if (!includeDeleted && IsDeletionMarked(cf))
            {
                yield break;
            }

            long count = 0;

            if (GetPreviousChainInfo(cf) is { } chainInfo)
            {
                Guid prevId = chainInfo.Item1;
                long pi = chainInfo.Item2;

                int expectedCount = (int)(pi - offset + 1);
                if (limit is { } limitNotNull && limitNotNull < expectedCount)
                {
                    expectedCount = limitNotNull;
                }

                foreach (BlockHash hash in IterateIndexes(prevId, offset, expectedCount, true))
                {
                    if (count >= limit)
                    {
                        yield break;
                    }

                    yield return hash;
                    count += 1;
                }

                offset = (int)Math.Max(0, offset - pi - 1);
            }

            byte[] prefix = IndexKeyPrefix;

            foreach (Iterator it in IterateDb(_chainDb, prefix, chainId).Skip(offset))
            {
                if (count >= limit)
                {
                    yield break;
                }

                byte[] value = it.Value();
                yield return new BlockHash(value);

                count += 1;
            }
        }

        private BlockHash? IndexBlockHash(Guid chainId, long index, bool includeDeleted)
        {
            try
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

                if (!includeDeleted && IsDeletionMarked(cf))
                {
                    return null;
                }

                if (GetPreviousChainInfo(cf) is { } chainInfo &&
                    chainInfo.Item2 >= index)
                {
                    return IndexBlockHash(chainInfo.Item1, index, true);
                }

                byte[] indexBytes = RocksDBStoreBitConverter.GetBytes(index);

                byte[] key = IndexKeyPrefix.Concat(indexBytes).ToArray();
                byte[] bytes = _chainDb.Get(key, cf);
                return bytes is null ? (BlockHash?)null : new BlockHash(bytes);
            }
            catch (Exception e)
            {
                LogUnexpectedException(nameof(IndexBlockHash), e);
            }

            return null;
        }

        private void AddFork(ColumnFamilyHandle cf, Guid chainId)
        {
            _chainDb.Put(ForkedChainsKey(chainId), new byte[0], cf);
        }

        private void RemoveFork(ColumnFamilyHandle cf, Guid chainId)
        {
            _chainDb.Remove(ForkedChainsKey(chainId), cf);
        }

        private bool HasFork(Guid chainId)
        {
            return IterateDb(_chainDb, ForkedChainsKeyPrefix, chainId).Any();
        }

        private bool IsDeletionMarked(ColumnFamilyHandle cf)
            => _chainDb.Get(DeletedKey, cf) is { };
    }

        /// <inheritdoc cref="IStore.GetBlock{T}(BlockHash)"/>
        public Block<T> GetBlock<T>(BlockHash blockHash)
            where T : IAction, new()
        {
            if (GetBlockDigest(blockHash) is BlockDigest blockDigest)
            {
                BlockHash? prevHash = blockDigest.Header.PreviousHash.Any()
                    ? new BlockHash(blockDigest.Header.PreviousHash)
                    : (BlockHash?)null;
                BlockHash? preEvaluationHash = blockDigest.Header.PreEvaluationHash.Any()
                    ? new BlockHash(blockDigest.Header.PreEvaluationHash)
                    : (BlockHash?)null;
                HashDigest<SHA256>? stateRootHash = blockDigest.Header.StateRootHash.Any()
                    ? new HashDigest<SHA256>(blockDigest.Header.StateRootHash)
                    : (HashDigest<SHA256>?)null;

                return new Block<T>(
                    index: blockDigest.Header.Index,
                    difficulty: blockDigest.Header.Difficulty,
                    totalDifficulty: blockDigest.Header.TotalDifficulty,
                    nonce: new Nonce(blockDigest.Header.Nonce.ToArray()),
                    miner: new Address(blockDigest.Header.Miner),
                    previousHash: prevHash,
                    timestamp: DateTimeOffset.ParseExact(
                        blockDigest.Header.Timestamp,
                        BlockHeader.TimestampFormat,
                        CultureInfo.InvariantCulture
                    ).ToUniversalTime(),
                    transactions: blockDigest.TxIds
                        .Select(bytes => GetTransaction<T>(new TxId(bytes.ToArray())))
                        .ToImmutableArray(),
                    preEvaluationHash: preEvaluationHash,
                    stateRootHash: stateRootHash,
                    protocolVersion: blockDigest.Header.ProtocolVersion
                );
            }

            return null;
        }

        /// <inheritdoc cref="IStore.GetBlockIndex(BlockHash)"/>
        public long? GetBlockIndex(BlockHash blockHash)
        {
            return GetBlockDigest(blockHash)?.Header.Index;
        }


        public virtual long CountTransactions()
        {
            return IterateTransactionIds().LongCount();
        }

        public virtual long CountBlocks()
        {
            return IterateBlockHashes().LongCount();
        }

        protected static IValue SerializeTxExecution(TxSuccess txSuccess)
        {
            var sDelta = new Dictionary(
                txSuccess.UpdatedStates.Select(kv =>
                    new KeyValuePair<IKey, IValue>(
                        new Binary(kv.Key.ByteArray),
                        kv.Value is { } v ? List.Empty.Add(v) : List.Empty
                    )
                )
            );
            var favDelta = SerializeGroupedFAVs(txSuccess.FungibleAssetsDelta);
            var updatedFAVs = SerializeGroupedFAVs(txSuccess.UpdatedFungibleAssets);
            return (Dictionary)Dictionary.Empty
                .Add("fail", false)
                .Add("sDelta", sDelta)
                .Add((IKey)(Text)"favDelta", new Dictionary(favDelta))
                .Add((IKey)(Text)"updatedFAVs", new Dictionary(updatedFAVs));
        }

        protected static IValue SerializeTxExecution(TxFailure txFailure)
        {
            Dictionary d = Dictionary.Empty
                .Add("fail", true)
                .Add("exc", txFailure.ExceptionName);
            return txFailure.ExceptionMetadata is { } v ? d.Add("excMeta", v) : d;
        }

        protected static TxExecution DeserializeTxExecution(
            BlockHash blockHash,
            TxId txid,
            IValue decoded,
            ILogger logger
        )
        {
            if (!(decoded is Bencodex.Types.Dictionary d))
            {
                const string msg = nameof(TxExecution) +
                    " must be serialized as a Bencodex dictionary, not {ActualValue}.";
                logger?.Error(msg, decoded.Inspection);
                return null;
            }

            try
            {
                bool fail = d.GetValue<Bencodex.Types.Boolean>("fail");
                if (fail)
                {
                    string excName = d.GetValue<Text>("exc");
                    IValue excMetadata = d.ContainsKey("excMeta") ? d["excMeta"] : null;
                    return new TxFailure(blockHash, txid, excName, excMetadata);
                }

                ImmutableDictionary<Address, IValue> sDelta = d.GetValue<Dictionary>("sDelta")
                    .ToImmutableDictionary(
                        kv => new Address((Binary)kv.Key),
                        kv => kv.Value is List l && l.Any() ? l[0] : null
                    );
                IImmutableDictionary<Address, IImmutableDictionary<Currency, FungibleAssetValue>>
                    favDelta = DeserializeGroupedFAVs(d.GetValue<Dictionary>("favDelta"));
                IImmutableDictionary<Address, IImmutableDictionary<Currency, FungibleAssetValue>>
                    updatedFAVs = DeserializeGroupedFAVs(d.GetValue<Dictionary>("updatedFAVs"));
                return new TxSuccess(
                    blockHash,
                    txid,
                    sDelta,
                    favDelta,
                    updatedFAVs
                );
            }
            catch (Exception e)
            {
                const string msg =
                    "Uncaught exception during deserializing a " + nameof(TxExecution) +
                    ": {Exception}";
                logger?.Error(e, msg, e);
                return null;
            }
        }

        private static Bencodex.Types.Dictionary SerializeGroupedFAVs(
            IImmutableDictionary<Address, IImmutableDictionary<Currency, FAV>> balanceDelta
        ) =>
            new Dictionary(
                balanceDelta.Select(pair =>
                    new KeyValuePair<IKey, IValue>(
                        new Binary(pair.Key.ByteArray),
                        SerializeFAVs(pair.Value)
                    )
                )
            );

        private static IImmutableDictionary<Address, IImmutableDictionary<Currency, FAV>>
        DeserializeGroupedFAVs(Bencodex.Types.Dictionary serialized) =>
            serialized.ToImmutableDictionary(
                kv => new Address((Binary)kv.Key),
                kv => DeserializeFAVs((List)kv.Value)
            );

        private static Bencodex.Types.List SerializeFAVs(
            IImmutableDictionary<Currency, FungibleAssetValue> favs
        ) =>
            new List(
                favs.Select(
                    kv => (IValue)List.Empty.Add(kv.Key.Serialize()).Add(kv.Value.RawValue)
                )
            );

        private static IImmutableDictionary<Currency, FungibleAssetValue> DeserializeFAVs(
            List serialized
        ) =>
            serialized.Select(pList =>
            {
                var pair = (List)pList;
                var currency = new Currency(pair[0]);
                BigInteger rawValue = (Bencodex.Types.Integer)pair[1];
                return new KeyValuePair<Currency, FAV>(
                    currency,
                    FAV.FromRawValue(currency, rawValue)
                );
            }).ToImmutableDictionary();
    }

}
