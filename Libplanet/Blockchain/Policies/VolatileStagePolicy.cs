#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Libplanet.Action;
using Libplanet.Tx;

namespace Libplanet.Blockchain.Policies
{
    /// <summary>
    /// In-memory staged transactions.
    /// </summary>
    public class VolatileStagePolicy : IStagePolicy
    {
        private readonly ConcurrentDictionary<TxId, Transaction?> _set;
        private readonly List<TxId> _queue;
        private readonly ReaderWriterLockSlim _lock;

        /// <summary>
        /// Creates a new <see cref="VolatileStagePolicy"/> instance.
        /// <para><see cref="Lifetime"/> is configured to 3 hours.</para>
        /// </summary>
        public VolatileStagePolicy()
            : this(TimeSpan.FromHours(3))
        {
        }

        /// <summary>
        /// Creates a new <see cref="VolatileStagePolicy"/> instance.
        /// </summary>
        /// <param name="lifetime">Volatilizes staged transactions older than this <paramref
        /// name="lifetime"/>.  See also the <see cref="Lifetime"/> property.</param>
        public VolatileStagePolicy(TimeSpan lifetime)
        {
            Lifetime = lifetime;
            _set = new ConcurrentDictionary<TxId, Transaction?>();
            _queue = new List<TxId>();
            _lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        }

        /// <summary>
        /// Volatilizes staged transactions older than this <see cref="Lifetime"/>.
        /// <para>Note that transactions older than the lifetime never cannot be staged.</para>
        /// </summary>
        public TimeSpan Lifetime { get; }

        /// <inheritdoc cref="IStagePolicy.Stage(BlockChain, Transaction)"/>
        public void Stage(BlockChain blockChain, Transaction transaction)
        {
            if (DateTimeOffset.UtcNow - Lifetime > transaction.Timestamp)
            {
                // The transaction is already expired; don't stage it at all.
                return;
            }

            try
            {
                if (!_set.TryAdd(transaction.Id, transaction))
                {
                    _lock.EnterUpgradeableReadLock();
                    if (_queue.Contains(transaction.Id))
                    {
                        return;
                    }
                }

                _lock.EnterWriteLock();
                try
                {
                    _queue.Add(transaction.Id);
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }
            finally
            {
                if (_lock.IsUpgradeableReadLockHeld)
                {
                    _lock.ExitUpgradeableReadLock();
                }
            }
        }

        /// <inheritdoc cref="IStagePolicy.Unstage(BlockChain, TxId)"/>
        public void Unstage(BlockChain blockChain, TxId id)
        {
            _lock.EnterWriteLock();
            _queue.Remove(id);
            _lock.ExitWriteLock();
        }

        /// <inheritdoc cref="IStagePolicy.Ignore(BlockChain, TxId)"/>
        public void Ignore(BlockChain blockChain, TxId id) =>
            _set.TryAdd(id, null);

        /// <inheritdoc cref="IStagePolicy.Ignores(BlockChain, TxId)"/>
        public bool Ignores(BlockChain blockChain, TxId id) =>
            (_set.TryGetValue(id, out Transaction? tx) && tx is null)
            || Get(blockChain, id, includeUnstaged: true) is { };

        /// <inheritdoc cref="IStagePolicy.Get(BlockChain, TxId, bool)"/>
        public Transaction? Get(BlockChain blockChain, TxId id, bool includeUnstaged)
        {
            if (!_set.TryGetValue(id, out Transaction? tx) || tx is null)
            {
                return null;
            }
            else if (tx.Timestamp >= DateTimeOffset.UtcNow - Lifetime)
            {
                _lock.EnterReadLock();
                try
                {
                    return includeUnstaged || _queue.Contains(id) ? tx : null;
                }
                finally
                {
                    _lock.ExitReadLock();
                }
            }

            _lock.EnterWriteLock();
            try
            {
                _queue.Remove(id);
                _set.TryRemove(id, out _);
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            return null;
        }

        /// <inheritdoc cref="IStagePolicy.Iterate(BlockChain)"/>
        public IEnumerable<Transaction> Iterate(BlockChain blockChain)
        {
            _lock.EnterReadLock();
            TxId[] queue;
            try
            {
                queue = _queue.ToArray();
            }
            finally
            {
                _lock.ExitReadLock();
            }

            DateTimeOffset exp = DateTimeOffset.UtcNow - Lifetime;
            var expired = new List<TxId>();
            foreach (TxId txid in queue)
            {
                if (_set.TryGetValue(txid, out Transaction? tx) && !(tx is null))
                {
                    if (tx.Timestamp > exp)
                    {
                        yield return tx;
                    }
                    else
                    {
                        expired.Add(tx.Id);
                    }
                }
            }

            if (!expired.Any())
            {
                yield break;
            }

            // Clean up expired transactions (if any exist).
            _lock.EnterWriteLock();
            try
            {
                foreach (TxId txid in expired)
                {
                    _queue.Remove(txid);
                    _set.TryRemove(txid, out _);
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
    }
}
