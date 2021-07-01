using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Bencodex;
using Bencodex.Types;
using Libplanet.Action;
using Libplanet.Assets;
using Libplanet.Blockchain.Policies;
using Libplanet.Blockchain.Renderers;
using Libplanet.Blocks;
using Libplanet.Crypto;
using Libplanet.Store;
using Libplanet.Store.Trie;
using Libplanet.Tx;
using Serilog;
using static Libplanet.Blockchain.KeyConverters;

namespace Libplanet.Blockchain
{
    /// <summary>
    /// A class have <see cref="Block"/>s, <see cref="Transaction"/>s, and the chain
    /// information.
    /// <para>In order to watch its state changes, implement <see cref="IRenderer"/>
    /// interface and pass it to the <see cref=
    /// "BlockChain(IBlockPolicy, IStagePolicy, IStore, IStateStore, Block, IEnumerable{IRenderer})"
    /// /> constructor.</para>
    /// </summary>
    /// <remarks>This object is guaranteed that it has at least one block, since it takes a genesis
    /// block when it's instantiated.</remarks>
    public partial class BlockChain
    {
        // FIXME: The _rwlock field should be private.
        [SuppressMessage(
            "StyleCop.CSharp.OrderingRules",
            "SA1401:FieldsMustBePrivate",
            Justification = "Temporary visibility.")]
        internal readonly ReaderWriterLockSlim _rwlock;
        private readonly object _txLock;
        private readonly ILogger _logger;

        /// <summary>
        /// All <see cref="Block"/>s in the <see cref="BlockChain"/>
        /// storage, including orphan <see cref="Block"/>s.
        /// Keys are <see cref="Block.Hash"/>es and values are
        /// their corresponding <see cref="Block"/>s.
        /// </summary>
        private IDictionary<BlockHash, Block> _blocks;

        /// <summary>
        /// Cached genesis block.
        /// </summary>
        private Block _genesis;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockChain"/> class.
        /// </summary>
        /// <param name="policy"><see cref="IBlockPolicy"/> to use in the
        /// <see cref="BlockChain"/>.</param>
        /// <param name="stagePolicy">The staging policy to follow.</param>
        /// <param name="store"><see cref="IStore"/> to store <see cref="Block"/>s,
        /// <see cref="Transaction"/>s, and <see cref="BlockChain"/> information.</param>
        /// <param name="genesisBlock">The genesis <see cref="Block"/> of
        /// the <see cref="BlockChain"/>, which is a part of the consensus.
        /// If the given <paramref name="store"/> already contains the genesis block
        /// it checks if the existing genesis block and this argument is the same.
        /// If the <paramref name="store"/> has no genesis block yet this argument will
        /// be used for that.</param>
        /// <param name="renderers">Listens state changes on the created chain.  Listens nothing
        /// by default or if it is <c>null</c>.  Note that action renderers receive events made
        /// by unsuccessful transactions too; see also <see cref="AtomicActionRenderer"/> for
        /// workaround.</param>
        /// <param name="stateStore"><see cref="IStateStore"/> to store states.</param>
        /// <exception cref="InvalidGenesisBlockException">Thrown when the <paramref name="store"/>
        /// has a genesis block and it does not match to what the network expects
        /// (i.e., <paramref name="genesisBlock"/>).</exception>
        public BlockChain(
            IBlockPolicy policy,
            IStagePolicy stagePolicy,
            IStore store,
            IStateStore stateStore,
            Block genesisBlock,
            IEnumerable<IRenderer> renderers = null
        )
            : this(
                policy,
                stagePolicy,
                store,
                stateStore,
                store.GetCanonicalChainId() ?? Guid.NewGuid(),
                genesisBlock,
                renderers
            )
        {
        }

        internal BlockChain(
            IBlockPolicy policy,
            IStagePolicy stagePolicy,
            IStore store,
            IStateStore stateStore,
            Guid id,
            Block genesisBlock,
            IEnumerable<IRenderer> renderers
        )
            : this(
                policy,
                stagePolicy,
                store,
                stateStore,
                id,
                genesisBlock,
                false,
                renderers
            )
        {
        }

        private BlockChain(
            IBlockPolicy policy,
            IStagePolicy stagePolicy,
            IStore store,
            IStateStore stateStore,
            Guid id,
            Block genesisBlock,
            bool inFork,
            IEnumerable<IRenderer> renderers
        )
        {
            Id = id;
            Policy = policy;
            StagePolicy = stagePolicy;
            Store = store;

            // It expects store is DefaultStore or RocksDBStore.
            StateStore = stateStore ?? store as IStateStore;
            if (StateStore is null)
            {
                throw new ArgumentNullException(nameof(stateStore));
            }

            _blocks = new BlockSet(store);
            Renderers = renderers is IEnumerable<IRenderer> r
                ? r.ToImmutableArray()
                : ImmutableArray<IRenderer>.Empty;
            ActionRenderers = Renderers.OfType<IActionRenderer>().ToImmutableArray();
            _rwlock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            _txLock = new object();

            if (Store.GetCanonicalChainId() is null)
            {
                Store.SetCanonicalChainId(Id);
            }

            _logger = Log.ForContext<BlockChain>()
                .ForContext("CanonicalChainId", Id);
            Func<BlockHash, ITrie> trieGetter = StateStore is TrieStateStore trieStateStore
                ? h => trieStateStore.GetTrie(h)
                : (Func<BlockHash, ITrie>)null;
            ActionEvaluator = new ActionEvaluator(
                policy.BlockAction,
                GetState,
                GetBalance,
                trieGetter);

            if (Count == 0)
            {
                if (inFork && StateStore is TrieStateStore)
                {
                    // If the store is BlockStateStore, have to fork state reference too so
                    // should use Append().
                    Store.AppendIndex(Id, genesisBlock.Hash);
                }
                else
                {
                    Append(
                        genesisBlock,
                        currentTime: genesisBlock.Timestamp,
                        renderBlocks: !inFork,
                        renderActions: !inFork,
                        evaluateActions: !inFork
                    );
                }
            }
            else if (!Genesis.Equals(genesisBlock))
            {
                string msg =
                    $"The genesis block that the given {nameof(IStore)} contains does not match " +
                    "to the genesis block that the network expects.  You might pass the wrong " +
                    "store which is incompatible with this chain.  Or your network might " +
                    "restarted the chain with a new genesis block so that it is incompatible " +
                    "with your existing chain in the local store.";
                throw new InvalidGenesisBlockException(
                    networkExpected: genesisBlock.Hash,
                    stored: Genesis.Hash,
                    message: msg
                );
            }
        }

        ~BlockChain()
        {
            _rwlock?.Dispose();
        }

        /// <summary>
        /// An event which is invoked when <see cref="Tip"/> is changed.
        /// </summary>
        private event EventHandler<(Block OldTip, Block NewTip)> TipChanged;

        /// <summary>
        /// The list of registered renderers listening the state changes.
        /// </summary>
        /// <remarks>
        /// Since this value is immutable, renderers cannot be registered after once a <see
        /// cref="BlockChain"/> object is instantiated; use <c>renderers</c> option of <see cref=
        /// "BlockChain(IBlockPolicy, IStagePolicy, IStore, IStateStore, Block, IEnumerable{IRenderer})"
        /// />
        /// constructor instead.
        /// </remarks>
        public IImmutableList<IRenderer> Renderers { get; }

        /// <summary>
        /// A filtered list, from <see cref="Renderers"/>, which contains only <see
        /// cref="IActionRenderer"/> instances.
        /// </summary>
        public IImmutableList<IActionRenderer> ActionRenderers { get; }

        /// <summary>
        /// The block and blockchain policy.
        /// </summary>
        public IBlockPolicy Policy { get; }

        /// <summary>
        /// The staging policy.
        /// </summary>
        public IStagePolicy StagePolicy { get; set; }

        /// <summary>
        /// The topmost <see cref="Block"/> of the current blockchain.
        /// Can be <c>null</c> if the blockchain is empty.
        /// </summary>
        public Block Tip
        {
            get
            {
                try
                {
                    return this[-1];
                }
                catch (ArgumentOutOfRangeException)
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// The first <see cref="Block"/> in the <see cref="BlockChain"/>.
        /// </summary>
        public Block Genesis => _genesis ??= this[0];

        public Guid Id { get; private set; }

        /// <summary>
        /// All <see cref="Block.Hash"/>es in the current index.  The genesis block's hash goes
        /// first, and the tip goes last.
        /// Returns a <see cref="long"/> integer that represents the number of elements in the
        /// <see cref="BlockChain"/>.
        /// </summary>
        public IEnumerable<BlockHash> BlockHashes => IterateBlockHashes();

        /// <summary>
        /// Returns a <see cref="long"/> integer that represents the number of elements in the
        /// <see cref="BlockChain"/>.
        /// </summary>
        /// <returns>A number that represents how many elements in the <see cref="BlockChain"/>.
        /// </returns>
        public long Count => Store.CountIndex(Id);

        internal IStore Store { get; }

        internal IStateStore StateStore { get; }

        internal ActionEvaluator ActionEvaluator { get; }

        /// <summary>
        /// Gets the block corresponding to the <paramref name="index"/>.
        /// </summary>
        /// <param name="index">A number of index of <see cref="Block"/>.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the given index of
        /// <see cref="Block"/> does not exist.</exception>
        public Block this[int index] => this[(long)index];

        /// <summary>
        /// Gets the block corresponding to the <paramref name="index"/>.
        /// </summary>
        /// <param name="index">A number of index of <see cref="Block"/>.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the given index of
        /// <see cref="Block"/> does not exist.</exception>
        public Block this[long index]
        {
            get
            {
                _rwlock.EnterReadLock();
                try
                {
                    BlockHash? blockHash = Store.IndexBlockHash(Id, index);
                    return blockHash is { } bh
                        ? _blocks[bh]
                        : throw new ArgumentOutOfRangeException();
                }
                finally
                {
                    _rwlock.ExitReadLock();
                }
            }
        }

        /// <summary>
        /// Gets the block corresponding to the <paramref name="blockHash"/>.
        /// </summary>
        /// <param name="blockHash">A <see cref="Block.Hash"/> of the <see cref="Block"/> to
        /// get. </param>
        /// <exception cref="KeyNotFoundException">Thrown when there is no <see cref="Block"/>
        /// with a given <paramref name="blockHash"/>.</exception>
        public Block this[in BlockHash blockHash]
        {
            get
            {
                if (!ContainsBlock(blockHash))
                {
                    throw new KeyNotFoundException(
                        $"The given hash[{blockHash}] was not found in this chain."
                    );
                }

                _rwlock.EnterReadLock();
                try
                {
                    return _blocks[blockHash];
                }
                finally
                {
                    _rwlock.ExitReadLock();
                }
            }
        }

        /// <summary>
        /// Mine the genesis block of the blockchain.
        /// </summary>
        /// <param name="actions">List of actions will be included in the genesis block.
        /// If it's null, it will be replaced with <see cref="ImmutableArray{IAction}.Empty"/>
        /// as default.</param>
        /// <param name="privateKey">A private key to sign the transaction in the genesis block.
        /// If it's null, it will use new private key as default.</param>
        /// <param name="timestamp">The timestamp of the genesis block. If it's null, it will
        /// use <see cref="DateTimeOffset.UtcNow"/> as default.</param>
        /// <param name="blockAction">A block action to execute and be rendered for every block.
        /// It must match to <see cref="BlockPolicy.BlockAction"/> of <see cref="Policy"/>.
        /// </param>
        /// <returns>The genesis block mined with parameters.</returns>
        public static Block MakeGenesisBlock(
            IEnumerable<IAction> actions = null,
            PrivateKey privateKey = null,
            DateTimeOffset? timestamp = null,
            IAction blockAction = null)
        {
            privateKey = privateKey ?? new PrivateKey();
            actions = actions ?? ImmutableArray<IAction>.Empty;
            IEnumerable<Transaction> transactions = new[]
            {
                Transaction.Create(0, privateKey, null, actions, timestamp: timestamp),
            };

            Block block = Block.Mine(
                0,
                0,
                0,
                privateKey.ToAddress(),
                null,
                timestamp ?? DateTimeOffset.UtcNow,
                transactions);

            var actionEvaluator = new ActionEvaluator(
                blockAction,
                (address, digest, stateCompleter) => null,
                (address, currency, hash, fungibleAssetStateCompleter)
                    => new FungibleAssetValue(currency),
                null);
            var actionEvaluationResult = actionEvaluator
                .Evaluate(block, StateCompleterSet.Reject)
                .GetTotalDelta(ToStateKey, ToFungibleAssetKey);
            ITrie trie = new MerkleTrie(new DefaultKeyValueStore(null));
            trie = trie.Set(actionEvaluationResult);
            var stateRootHash = trie.Commit(rehearsal: true).Hash;

            return new Block(
                block,
                stateRootHash);
        }

        /// <summary>
        /// Determines whether the <see cref="BlockChain"/> contains <see cref="Block"/>
        /// the specified <paramref name="blockHash"/>.
        /// </summary>
        /// <param name="blockHash">The <see cref="HashDigest{T}"/> of the <see cref="Block"/> to
        /// check if it is in the <see cref="BlockChain"/>.</param>
        /// <returns>
        /// <c>true</c> if the <see cref="BlockChain"/> contains <see cref="Block"/> with
        /// the specified <paramref name="blockHash"/>; otherwise, <c>false</c>.
        /// </returns>
        public bool ContainsBlock(BlockHash blockHash)
        {
            _rwlock.EnterReadLock();
            try
            {
                return
                    _blocks.ContainsKey(blockHash) &&
                    Store.GetBlockIndex(blockHash) is { } branchPointIndex &&
                    branchPointIndex <= Tip.Index &&
                    Store.IndexBlockHash(Id, branchPointIndex).Equals(blockHash);
            }
            finally
            {
                _rwlock.ExitReadLock();
            }
        }

        /// <summary>
        /// Gets the transaction corresponding to the <paramref name="txId"/>.
        /// </summary>
        /// <param name="txId">A <see cref="TxId"/> of the <see cref="Transaction"/> to get.
        /// </param>
        /// <returns><see cref="Transaction"/> with <paramref name="txId"/>.</returns>
        /// <exception cref="KeyNotFoundException">Thrown when there is no
        /// <see cref="Transaction"/> with a given <paramref name="txId"/>.</exception>
        public Transaction GetTransaction(TxId txId)
        {
            if (StagePolicy.Get(this, txId, includeUnstaged: true) is { } tx)
            {
                return tx;
            }

            _rwlock.EnterReadLock();
            try
            {
                if (Store.GetTransaction(txId) is { } transaction)
                {
                    transaction.Validate();
                    return transaction;
                }

                throw new KeyNotFoundException($"No such transaction: {txId}");
            }
            finally
            {
                _rwlock.ExitReadLock();
            }
        }

        /// <summary>
        /// Gets the state of the given <paramref name="address"/> in the
        /// <see cref="BlockChain"/> from <paramref name="offset"/>.
        /// </summary>
        /// <param name="address">An <see cref="Address"/> to get the states of.</param>
        /// <param name="offset">The <see cref="HashDigest{T}"/> of the block to start finding
        /// the state.  It will be The tip of the <see cref="BlockChain"/> if it is <c>null</c>.
        /// </param>
        /// <param name="stateCompleter">When the <see cref="BlockChain"/> instance does not
        /// contain states dirty of the block which lastly updated states of a requested address,
        /// this delegate is called and its return value is used instead.
        /// <para><see cref="StateCompleters.Recalculate"/> makes the incomplete states
        /// recalculated and filled on the fly.</para>
        /// <para><see cref="StateCompleters.Reject"/> (which is default) makes the incomplete
        /// states (if needed) to cause <see cref="IncompleteBlockStatesException"/> instead.</para>
        /// </param>
        /// <returns>The current state of given <paramref name="address"/>.  This can be <c>null</c>
        /// if <paramref name="address"/> has no value.</returns>
        public IValue GetState(
            Address address,
            BlockHash? offset = null,
            StateCompleter stateCompleter = null
        ) =>
            GetRawState(
                ToStateKey(address),
                offset,
                StateCompleters.ToRawStateCompleter(
                    stateCompleter ?? StateCompleters.Reject,
                    address
                )
            );

        /// <summary>
        /// Queries <paramref name="address"/>'s balance of the <paramref name="currency"/> in the
        /// <see cref="BlockChain"/> from <paramref name="offset"/>.
        /// </summary>
        /// <param name="address">The owner <see cref="Address"/> to query.</param>
        /// <param name="currency">The currency type to query.</param>
        /// <param name="offset">The <see cref="HashDigest{T}"/> of the block to
        /// start finding the state. It will be the tip of the
        /// <see cref="BlockChain"/> if it is <c>null</c>.</param>
        /// <param name="stateCompleter">When the <see cref="BlockChain"/> instance does not
        /// contain states dirty of the block which lastly updated states of a requested address,
        /// this delegate is called and its return value is used instead.
        /// <para><see cref="FungibleAssetStateCompleters.Recalculate"/> makes the incomplete
        /// states recalculated and filled on the fly.</para>
        /// <para><see cref="FungibleAssetStateCompleters.Reject"/> (which is default) makes
        /// the incomplete states (if needed) to cause <see cref="IncompleteBlockStatesException"/>
        /// instead.</para></param>
        /// <returns>The <paramref name="address"/>'s current balance (or balance as of the given
        /// <paramref name="offset"/>) of the <paramref name="currency"/>.
        /// </returns>
        public FungibleAssetValue GetBalance(
            Address address,
            Currency currency,
            BlockHash? offset = null,
            FungibleAssetStateCompleter stateCompleter = null
        )
        {
            stateCompleter ??= FungibleAssetStateCompleters.Reject;
            IValue v = GetRawState(
                ToFungibleAssetKey(address, currency),
                offset,
                FungibleAssetStateCompleters.ToRawStateCompleter(
                    stateCompleter,
                    address,
                    currency
                )
            );
            return FungibleAssetValue.FromRawValue(
                currency,
                v is Bencodex.Types.Integer i ? i.Value : 0
            );
        }

        /// <summary>
        /// Queries the recorded <see cref="TxExecution"/> for a successful or failed
        /// <see cref="Transaction"/> within a <see cref="Block"/>.
        /// </summary>
        /// <param name="blockHash">The <see cref="Block.Hash"/> of the <see cref="Block"/>
        /// that the <see cref="Transaction"/> is executed within.</param>
        /// <param name="txid">The executed <see cref="Transaction"/>'s
        /// <see cref="Transaction.Id"/>.</param>
        /// <returns>The recorded <see cref="TxExecution"/>.  If the transaction has never been
        /// executed within the block, it returns <c>null</c> instead.</returns>
        public TxExecution GetTxExecution(BlockHash blockHash, TxId txid) =>
            Store.GetTxExecution(blockHash, txid);

        /// <summary>
        /// Adds a <paramref name="block"/> to the end of this chain.
        /// <para><see cref="Block.Transactions"/> in the <paramref name="block"/> updates
        /// states and balances in the blockchain, and <see cref="TxExecution"/>s for
        /// transactions are recorded.</para>
        /// <para>Note that <see cref="Renderers"/> receive events right after the <paramref
        /// name="block"/> is confirmed (and thus all states reflect changes in the <paramref
        /// name="block"/>).</para>
        /// </summary>
        /// <param name="block">A next <see cref="Block"/>, which is mined,
        /// to add.</param>
        /// <param name="stateCompleters">The strategy to complement incomplete block states which
        /// are required for action execution and rendering.
        /// <see cref="StateCompleterSet.Recalculate"/> by default.
        /// </param>
        /// <exception cref="InvalidBlockBytesLengthException">Thrown when the given <paramref
        /// name="block"/> is too long in bytes (according to <see
        /// cref="IBlockPolicy.GetMaxBlockBytes(long)"/>).</exception>
        /// <exception cref="BlockExceedingTransactionsException">Thrown when the given <paramref
        /// name="block"/> has too many transactions (according to <see
        /// cref="IBlockPolicy.MaxTransactionsPerBlock"/>).</exception>
        /// <exception cref="InvalidBlockException">Thrown when the given <paramref name="block"/>
        /// is invalid, in itself or according to the <see cref="Policy"/>.</exception>
        /// <exception cref="InvalidTxNonceException">Thrown when the
        /// <see cref="Transaction.Nonce"/> is different from
        /// <see cref="GetNextTxNonce"/> result of the
        /// <see cref="Transaction.Signer"/>.</exception>
        public void Append(Block block, StateCompleterSet? stateCompleters = null) =>
            Append(block, DateTimeOffset.UtcNow, stateCompleters);

        /// <summary>
        /// Adds a <paramref name="block"/> to the end of this chain.
        /// <para><see cref="Block.Transactions"/> in the <paramref name="block"/> updates
        /// states and balances in the blockchain, and <see cref="TxExecution"/>s for
        /// transactions are recorded.</para>
        /// <para>Note that <see cref="Renderers"/> receive events right after the <paramref
        /// name="block"/> is confirmed (and thus all states reflect changes in the <paramref
        /// name="block"/>).</para>
        /// </summary>
        /// <param name="block">A next <see cref="Block"/>, which is mined,
        /// to add.</param>
        /// <param name="currentTime">The current time.</param>
        /// <param name="stateCompleters">The strategy to complement incomplete block states which
        /// are required for action execution and rendering.
        /// <see cref="StateCompleterSet.Recalculate"/> by default.
        /// </param>
        /// <exception cref="InvalidBlockBytesLengthException">Thrown when the given <paramref
        /// name="block"/> is too long in bytes (according to <see
        /// cref="IBlockPolicy.GetMaxBlockBytes(long)"/>).</exception>
        /// <exception cref="BlockExceedingTransactionsException">Thrown when the given <paramref
        /// name="block"/> has too many transactions (according to <see
        /// cref="IBlockPolicy.MaxTransactionsPerBlock"/>).</exception>
        /// <exception cref="InvalidBlockException">Thrown when the given <paramref name="block"/>
        /// is invalid, in itself or according to the <see cref="Policy"/>.</exception>
        /// <exception cref="InvalidTxNonceException">Thrown when the
        /// <see cref="Transaction.Nonce"/> is different from
        /// <see cref="GetNextTxNonce"/> result of the
        /// <see cref="Transaction.Signer"/>.</exception>
        public void Append(
            Block block,
            DateTimeOffset currentTime,
            StateCompleterSet? stateCompleters = null
        ) =>
            Append(
                block,
                currentTime,
                evaluateActions: true,
                renderBlocks: true,
                renderActions: true,
                stateCompleters: stateCompleters
            );

        /// <summary>
        /// Adds a <paramref name="transaction"/> to the pending list so that
        /// a next <see cref="Block"/> to be mined contains the given
        /// <paramref name="transaction"/>.
        /// </summary>
        /// <param name="transaction"><see cref="Transaction"/> to add to the pending list.
        /// </param>
        /// <exception cref="InvalidTxException">Thrown when the given
        /// <paramref name="transaction"/> is invalid.</exception>
        public void StageTransaction(Transaction transaction)
        {
            if (!transaction.GenesisHash.Equals(Genesis.Hash))
            {
                var msg = "GenesisHash of the transaction is not compatible " +
                          "with the BlockChain.Genesis.Hash.";
                throw new InvalidTxGenesisHashException(
                    transaction.Id,
                    Genesis.Hash,
                    transaction.GenesisHash,
                    msg);
            }

            if (StagePolicy.Ignores(this, transaction.Id))
            {
                return;
            }

            if (transaction.Nonce >= Store.GetTxNonce(Id, transaction.Signer))
            {
                StagePolicy.Stage(this, transaction);
            }
            else
            {
                StagePolicy.Ignore(this, transaction.Id);
            }
        }

        /// <summary>
        /// Removes a <paramref name="transaction"/> from the pending list.
        /// </summary>
        /// <param name="transaction">A <see cref="Transaction"/>
        /// to remove from the pending list.</param>
        /// <seealso cref="StageTransaction"/>
        public void UnstageTransaction(Transaction transaction) =>
            StagePolicy.Unstage(this, transaction.Id);

        /// <summary>
        /// Gets next <see cref="Transaction.Nonce"/> of the address.
        /// </summary>
        /// <param name="address">The <see cref="Address"/> from which to obtain the
        /// <see cref="Transaction.Nonce"/> value.</param>
        /// <returns>The next <see cref="Transaction.Nonce"/> value of the
        /// <paramref name="address"/>.</returns>
        public long GetNextTxNonce(Address address)
        {
            long nonce = Store.GetTxNonce(Id, address);
            long prevNonce = nonce - 1;
            IOrderedEnumerable<long> stagedTxNonces = StagePolicy.Iterate(this)
                .Where(tx => tx.Signer.Equals(address) && tx.Nonce > prevNonce)
                .Select(tx => tx.Nonce)
                .OrderBy(n => n);

            foreach (long n in stagedTxNonces)
            {
                if (n < nonce)
                {
                    continue;
                }

                if (n != nonce)
                {
                    break;
                }

                nonce++;
            }

            return nonce;
        }

        /// <summary>
        /// Records and queries the <paramref name="perceivedTime"/> of the given
        /// <paramref name="blockExcerpt"/>.
        /// <para>Although blocks have their own <see cref="Block.Timestamp"/>, but these values
        /// are untrustworthy as they are arbitrarily determined by their miners.</para>
        /// <para>On the other hand, this method returns the subjective time according to the local
        /// node's perception.</para>
        /// <para>If the local node has never perceived the <paramref name="blockExcerpt"/> yet,
        /// it is perceived at that moment and the current time is returned instead. (However, you
        /// can replace the current time with the <paramref name="perceivedTime"/> option.)
        /// In other words, this method is idempotent.</para>
        /// </summary>
        /// <param name="blockExcerpt">The perceived block.</param>
        /// <param name="perceivedTime">The time the local node perceived the given <paramref
        /// name="blockExcerpt"/>.  The current time by default.</param>
        /// <returns>A pair of a block and the time it was perceived.</returns>
        public BlockPerception PerceiveBlock(
            IBlockExcerpt blockExcerpt,
            DateTimeOffset? perceivedTime = null
        )
        {
            if (!(Store.GetBlockPerceivedTime(blockExcerpt.Hash) is { } time))
            {
                time = perceivedTime ?? DateTimeOffset.UtcNow;
                Store.SetBlockPerceivedTime(blockExcerpt.Hash, time);
            }

            return new BlockPerception(blockExcerpt, time);
        }

#pragma warning disable MEN003
        /// <summary>
        /// Mines a next <see cref="Block"/> using staged <see cref="Transaction"/>s,
        /// and then <see cref="Append(Block, StateCompleterSet?)"/> it to the chain
        /// (unless the <paramref name="append"/> option is turned off).
        /// </summary>
        /// <param name="miner">The <see cref="Address"/> of miner that mined the block.</param>
        /// <param name="currentTime">The <see cref="DateTimeOffset"/> when mining started.</param>
        /// <param name="append">Whether to <see cref="Append(Block, StateCompleterSet?)"/>
        /// the mined block.  Turned on by default.</param>
        /// <param name="maxTransactions">The maximum number of transactions that a block can
        /// accept.  This value must be greater than 0, and less than or equal to
        /// <see cref="Policy"/>.<see cref="IBlockPolicy.MaxTransactionsPerBlock"/>.
        /// Zero and negative values are treated as 1. If it is omitted or more than
        /// <see cref="Policy"/>.<see cref="IBlockPolicy.MaxTransactionsPerBlock"/>, it will be
        /// treated as <see cref="Policy"/>.<see cref="IBlockPolicy.MaxTransactionsPerBlock"/>.
        /// </param>
        /// <param name="cancellationToken">
        /// A cancellation token used to propagate notification that this
        /// operation should be canceled.
        /// </param>
        /// <returns>An awaitable task with a <see cref="Block"/> that is mined.</returns>
        /// <exception cref="OperationCanceledException">Thrown when
        /// <see cref="BlockChain.Tip"/> is changed while mining.</exception>
        public async Task<Block> MineBlock(
            Address miner,
            DateTimeOffset currentTime,
            bool append = true,
            int? maxTransactions = null,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            using var cts = new CancellationTokenSource();
            using CancellationTokenSource cancellationTokenSource =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token);
            void WatchTip(object target, (Block OldTip, Block NewTip) tip) => cts.Cancel();
            TipChanged += WatchTip;

            maxTransactions = Math.Max(
                Math.Min(
                    maxTransactions ?? Policy.MaxTransactionsPerBlock,
                    Policy.MaxTransactionsPerBlock
                ),
                1
            );

            long index = Store.CountIndex(Id);
            long difficulty = Policy.GetNextBlockDifficulty(this);
            BlockHash? prevHash = Store.IndexBlockHash(Id, index - 1);

            int sessionId = new System.Random().Next();
            int procId = Process.GetCurrentProcess().Id;
            _logger.Debug(
                "Start to mine a block #{Index} [difficulty: {Difficulty}; prev: {prevHash}; " +
                "session: {SessionId}; proc: {ProcessId}]... ",
                index,
                difficulty,
                prevHash,
                sessionId,
                procId
            );

            ImmutableArray<Transaction> stagedTransactions = ListStagedTransactions();
            _logger.Debug(
                "There are {Transactions} staged transactions.",
                stagedTransactions.Length
            );

            var transactionsToMine = new List<Transaction>();
            int i = 0;

            // FIXME: The tx collection timeout should be configurable.
            DateTimeOffset timeout = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(4);

            // Makes an empty block to estimate the length of bytes without transactions.
            var estimatedBytes = new Block(
                index: index,
                difficulty: difficulty,
                totalDifficulty: Tip.TotalDifficulty,
                nonce: default,
                miner: miner,
                previousHash: prevHash,
                timestamp: currentTime,
                transactions: new Transaction[0]
            ).BytesLength;
            int maxBlockBytes = Math.Max(Policy.GetMaxBlockBytes(index), 1);
            var skippedSigners = new HashSet<Address>();
            var storedNonces = new Dictionary<Address, long>();
            var nextNonces = new Dictionary<Address, long>();

            foreach (Transaction tx in stagedTransactions)
            {
                // We don't care about nonce ordering here because `.ListStagedTransactions()`
                // returns already ordered transactions by its nonce.
                if (!storedNonces.ContainsKey(tx.Signer))
                {
                    storedNonces[tx.Signer] = Store.GetTxNonce(Id, tx.Signer);
                }

                if (nextNonces.TryGetValue(tx.Signer, out long prevNonce))
                {
                    nextNonces[tx.Signer] = prevNonce + 1;
                }
                else
                {
                    nextNonces[tx.Signer] = storedNonces[tx.Signer] + 1;
                }

                _logger.Verbose(
                    "Preparing mining a block #{Index}; validating a tx {Index}/{Total} " +
                    "{Transaction}...",
                    index,
                    ++i,
                    stagedTransactions.Length,
                    tx.Id
                );

                if (transactionsToMine.Count >= maxTransactions)
                {
                    _logger.Information(
                        "Not all staged transactions will be included in a block #{Index} to " +
                        "be mined by {Miner}, because it reaches the maximum number of " +
                        "acceptable transactions: {MaxTransactions}",
                        index,
                        miner,
                        maxTransactions
                    );
                    break;
                }

                if (!Policy.DoesTransactionFollowsPolicy(tx, this))
                {
                    _logger.Debug(
                        "Unstage the tx {Index}/{Total} {Transaction} as it doesn't follow policy.",
                        i,
                        stagedTransactions.Length,
                        tx.Id
                    );
                    UnstageTransaction(tx);
                    continue;
                }

                if (storedNonces[tx.Signer] <= tx.Nonce && tx.Nonce < nextNonces[tx.Signer])
                {
                    if (estimatedBytes + tx.BytesLength > maxBlockBytes)
                    {
                        // Once someone's tx is excluded from a block, their later txs are also all
                        // excluded in the block, because later nonces become invalid.
                        skippedSigners.Add(tx.Signer);
                        _logger.Information(
                            "The {Signer}'s transactions after the nonce #{Nonce} will be " +
                            "excluded in a block #{Index} to be mined by {Miner}, because it " +
                            "takes too long bytes.",
                            tx.Signer,
                            tx.Nonce,
                            index,
                            miner
                        );
                        continue;
                    }

                    transactionsToMine.Add(tx);
                    estimatedBytes += tx.BytesLength;
                }
                else if (tx.Nonce < storedNonces[tx.Signer])
                {
                    _logger.Debug(
                        "Tx {Index}/{Total} {Transaction} has a lower nonce than expected: " +
                        "{Nonce} ({Signer})." +
                        "it will be discarded.",
                        i,
                        stagedTransactions.Length,
                        tx.Id,
                        tx.Nonce,
                        tx.Signer
                    );
                    UnstageTransaction(tx);
                }
                else
                {
                    _logger.Debug(
                        "Tx {Index}/{Total} {Transaction} has a higher nonce than expected: " +
                        "{Nonce} ({Signer}).  " +
                        "It will be included by a block mined later.",
                        i,
                        stagedTransactions.Length,
                        tx.Id,
                        tx.Nonce,
                        tx.Signer
                    );
                }

                if (timeout < DateTimeOffset.UtcNow)
                {
                    _logger.Debug(
                        "Reached the time limit to collect staged transactions; other staged " +
                        "transactions will be mined later."
                    );
                    break;
                }
            }

            _logger.Verbose(
                "A block #{Index} to be mined by {Miner} will include {Transactions} " +
                "transactions out of {StagedTransactions} staged transactions.",
                index,
                miner,
                transactionsToMine.Count,
                stagedTransactions.Length
            );

            Block block;
            try
            {
                block = await Task.Run(
                    () => Block.Mine(
                        index: index,
                        difficulty: difficulty,
                        previousTotalDifficulty: Tip.TotalDifficulty,
                        miner: miner,
                        previousHash: prevHash,
                        timestamp: currentTime,
                        transactions: transactionsToMine,
                        cancellationToken: cancellationTokenSource.Token),
                    cancellationTokenSource.Token
                );
            }
            catch (OperationCanceledException)
            {
                if (cts.IsCancellationRequested)
                {
                    throw new OperationCanceledException(
                        "Mining canceled due to change of tip index.");
                }

                throw new OperationCanceledException(cancellationToken);
            }
            finally
            {
                TipChanged -= WatchTip;
            }

            IReadOnlyList<ActionEvaluation> actionEvaluations = ActionEvaluator.Evaluate(
                block, StateCompleterSet.Recalculate);

            if (StateStore is TrieStateStore trieStateStore)
            {
                _rwlock.EnterWriteLock();
                try
                {
                    SetStates(block, actionEvaluations);
                    block = new Block(block, trieStateStore.GetRootHash(block.Hash));

                    // it's needed because `block.Hash` was updated with the state root hash.
                    // FIXME: we need a method for calculating the state root hash without
                    // `.SetStates()`.
                    SetStates(block, actionEvaluations);
                }
                finally
                {
                    _rwlock.ExitWriteLock();
                }
            }
            else
            {
                // We need to re-execute it.
                actionEvaluations = null;
            }

            _logger.Debug(
                "Mined a block #{Index} [difficulty: {Difficulty}; prev: {prevHash}; " +
                "session: {SessionId}; proc: {ProcessId}].",
                index,
                difficulty,
                prevHash,
                sessionId,
                procId
            );

            if (append)
            {
                Append(
                    block,
                    currentTime,
                    evaluateActions: true,
                    renderBlocks: true,
                    renderActions: true,
                    actionEvaluations: actionEvaluations
                );

                _logger.Debug(
                    "Appended a block #{Index} [difficulty: {Difficulty}; prev: {prevHash}; " +
                    "session: {SessionId}; proc: {ProcessId}].",
                    index,
                    difficulty,
                    prevHash,
                    sessionId,
                    procId
                );
            }

            return block;
        }
#pragma warning restore MEN003

        /// <summary>
        /// Mines a next <see cref="Block"/> using staged <see cref="Transaction"/>s,
        /// and then <see cref="Append(Block, StateCompleterSet?)"/> it to the chain
        /// (unless the <paramref name="append"/> option is turned off).
        /// </summary>
        /// <param name="miner">The <see cref="Address"/> of miner that mined the block.</param>
        /// <param name="append">Whether to <see cref="Append(Block, StateCompleterSet?)"/>
        /// the mined block.  Turned on by default.</param>
        /// <param name="maxTransactions">The maximum number of transactions that a block can
        /// accept.  This value must be greater than 0, and less than or equal to
        /// <see cref="Policy"/>.<see cref="IBlockPolicy.MaxTransactionsPerBlock"/>.
        /// Zero and negative values are treated as 1. If it is omitted or more than
        /// <see cref="Policy"/>.<see cref="IBlockPolicy.MaxTransactionsPerBlock"/>, it will be
        /// treated as <see cref="Policy"/>.<see cref="IBlockPolicy.MaxTransactionsPerBlock"/>.
        /// </param>
        /// <param name="cancellationToken">
        /// A cancellation token used to propagate notification that this
        /// operation should be canceled.
        /// </param>
        /// <returns>An awaitable task with a <see cref="Block"/> that is mined.</returns>
        /// <exception cref="OperationCanceledException">Thrown when
        /// <see cref="BlockChain.Tip"/> is changed while mining.</exception>
        public Task<Block> MineBlock(
            Address miner,
            bool append = true,
            int? maxTransactions = null,
            CancellationToken cancellationToken = default
        ) =>
            MineBlock(miner, DateTimeOffset.UtcNow, append, maxTransactions, cancellationToken);

        /// <summary>
        /// Creates a new <see cref="Transaction"/> and stage the transaction.
        /// Cannot create new transaction if the genesis block does not exist.
        /// </summary>
        /// <param name="privateKey">A <see cref="PrivateKey"/> of the account who creates and
        /// signs a new transaction.</param>
        /// <param name="actions">A list of <see cref="IAction"/>s to include to a new transaction.
        /// </param>
        /// <param name="updatedAddresses"><see cref="Address"/>es whose states affected by
        /// <paramref name="actions"/>.</param>
        /// <param name="timestamp">The time this <see cref="Transaction"/> is created and
        /// signed.</param>
        /// <returns>A created new <see cref="Transaction"/> signed by the given
        /// <paramref name="privateKey"/>.</returns>
        /// <seealso cref="Transaction.Create" />
        public Transaction MakeTransaction(
            PrivateKey privateKey,
            IEnumerable<IAction> actions,
            IImmutableSet<Address> updatedAddresses = null,
            DateTimeOffset? timestamp = null)
        {
            timestamp = timestamp ?? DateTimeOffset.UtcNow;
            lock (_txLock)
            {
                // FIXME: Exception should be documented when the genesis block does not exist.
                Transaction tx = Transaction.Create(
                    GetNextTxNonce(privateKey.ToAddress()),
                    privateKey,
                    Genesis.Hash,
                    actions,
                    updatedAddresses,
                    timestamp);
                StageTransaction(tx);

                return tx;
            }
        }

        /// <summary>
        /// Lists all staged <see cref="TxId"/>s.
        /// </summary>
        /// <returns><see cref="IImmutableSet{TxId}"/> of staged transactions.</returns>
        public IImmutableSet<TxId> GetStagedTransactionIds()
        {
            // FIXME: How about turning this method to the StagedTransactions property?
            return StagePolicy.Iterate(this).Select(tx => tx.Id).ToImmutableHashSet();
        }

#pragma warning disable MEN003
        internal void Append(
            Block block,
            DateTimeOffset currentTime,
            bool evaluateActions,
            bool renderBlocks,
            bool renderActions,
            IReadOnlyList<ActionEvaluation> actionEvaluations = null,
            StateCompleterSet? stateCompleters = null
        )
        {
            if (!evaluateActions && renderActions)
            {
                throw new ArgumentException(
                    $"{nameof(renderActions)} option requires {nameof(evaluateActions)} " +
                    "to be turned on.",
                    nameof(renderActions)
                );
            }

            renderActions = renderActions && renderBlocks && ActionRenderers.Any();

            // Since rendering process requires every step's states, if required block states
            // are incomplete they are complemented anyway:
            stateCompleters ??= StateCompleterSet.Recalculate;

            _logger.Debug("Trying to append block {blockIndex}: {block}", block?.Index, block);

            if (block.Transactions.Count() is { } txCount &&
                txCount > Policy.MaxTransactionsPerBlock)
            {
                throw new BlockExceedingTransactionsException(
                    txCount,
                    Policy.MaxTransactionsPerBlock,
                    "The block to append has too many transactions."
                );
            }

            if (block.BytesLength > Policy.GetMaxBlockBytes(block.Index))
            {
                throw new InvalidBlockBytesLengthException(
                    block.BytesLength,
                    Policy.GetMaxBlockBytes(block.Index),
                    "The block to append is too long in bytes."
                );
            }

            _rwlock.EnterUpgradeableReadLock();
            Block prevTip = Tip;
            try
            {
                InvalidBlockException e = ValidateNextBlock(block);

                if (!(e is null))
                {
                    _logger.Error(e, "Append failed. The block is invalid.");
                    throw e;
                }

                var nonceDeltas = new Dictionary<Address, long>();

                // block.Transactions have already been sorted by
                // the tx nounce order when the block was created
                foreach (Transaction tx1 in block.Transactions)
                {
                    if (!Policy.DoesTransactionFollowsPolicy(tx1, this))
                    {
                        throw new TxViolatingBlockPolicyException(
                            tx1.Id,
                            "According to BlockPolicy, this transaction is not valid.");
                    }

                    Address txSigner = tx1.Signer;
                    nonceDeltas.TryGetValue(txSigner, out var nonceDelta);

                    long expectedNonce = nonceDelta + Store.GetTxNonce(Id, txSigner);

                    if (!expectedNonce.Equals(tx1.Nonce))
                    {
                        _logger.Debug("Append failed. The tx `{transaction}` is invalid.", tx1);
                        throw new InvalidTxNonceException(
                            tx1.Id,
                            expectedNonce,
                            tx1.Nonce,
                            "Transaction nonce is invalid."
                        );
                    }

                    nonceDeltas[txSigner] = nonceDelta + 1;
                }

                ImmutableDictionary<Address, long> maxNonces;
                _rwlock.EnterWriteLock();
                try
                {
                    if (evaluateActions && actionEvaluations is null)
                    {
                        _logger.Debug(
                            "Executing actions in the block #{BlockIndex} {BlockHash}...",
                            block.Index,
                            block.Hash
                        );
                        actionEvaluations = ExecuteActions(block);
                        _logger.Debug(
                            "Executed actions in the block #{BlockIndex} {BlockHash}.",
                            block.Index,
                            block.Hash
                        );
                    }

                    if (actionEvaluations is { } evals)
                    {
                        IEnumerable<TxExecution> txExecutions = MakeTxExecutions(block, evals);
                        UpdateTxExecutions(txExecutions);
                    }

                    _blocks[block.Hash] = block;
                    foreach (KeyValuePair<Address, long> pair in nonceDeltas)
                    {
                        Store.IncreaseTxNonce(Id, pair.Key, pair.Value);
                    }

                    Store.AppendIndex(Id, block.Hash);

                    const string unstageStartMsg =
                        "Unstaging {Txs} transaction(s) which belong to the block " +
                        "#{BlockIndex} {BlockHash}...";
                    _logger.Debug(
                        unstageStartMsg,
                        block.Transactions.Count(),
                        block.Index,
                        block.Hash
                    );

                    maxNonces = block.Transactions
                        .GroupBy(
                            t => t.Signer,
                            t => t.Nonce,
                            (signer, nonces) => new
                            {
                                signer = signer,
                                maxNonce = nonces.Max(),
                            }
                        )
                        .ToImmutableDictionary(t => t.signer, t => t.maxNonce);
                }
                finally
                {
                    _rwlock.ExitWriteLock();
                }

                ISet<TxId> txIds = StagePolicy.Iterate(this)
                    .Where(tx => maxNonces.TryGetValue(tx.Signer, out long nonce) &&
                        tx.Nonce <= nonce)
                    .Select(tx => tx.Id)
                    .ToImmutableHashSet();
                foreach (TxId txId in txIds)
                {
                    StagePolicy.Unstage(this, txId);
                }

                const string unstageEndMsg =
                    "Unstaged {Txs} transaction(s), which belong to the block " +
                    "#{BlockIndex} {BlockHash}...";
                _logger.Debug(unstageEndMsg, txIds.Count, block.Index, block.Hash);

                TipChanged?.Invoke(this, (prevTip, block));
                _logger.Debug(
                    "Appended the block #{BlockIndex} {BlockHash}.",
                    block.Index,
                    block.Hash
                );

                if (renderBlocks)
                {
                    const string startMsg =
                        "Invoking renderers for #{BlockIndex} {BlockHash}... " +
                        "({Renderers} renderer(s), {ActionRenderers} action renderer(s))";
                    _logger.Debug(
                        startMsg,
                        block.Index,
                        block.Hash,
                        Renderers.Count,
                        ActionRenderers.Count
                    );
                    foreach (IRenderer renderer in Renderers)
                    {
                        renderer.RenderBlock(oldTip: prevTip ?? Genesis, newTip: block);
                    }

                    if (ActionRenderers.Any())
                    {
                        foreach (IActionRenderer renderer in ActionRenderers)
                        {
                            if (renderActions)
                            {
                                RenderActions(actionEvaluations, block, renderer, stateCompleters);
                            }

                            renderer.RenderBlockEnd(oldTip: prevTip ?? Genesis, newTip: block);
                        }
                    }

                    const string endMsg =
                        "Invoked renderers for #{BlockIndex} {BlockHash}... " +
                        "({Renderers} renderer(s), {ActionRenderers} action renderer(s))";
                    _logger.Debug(
                        endMsg,
                        block.Index,
                        block.Hash,
                        Renderers.Count,
                        ActionRenderers.Count
                    );
                }
            }
            finally
            {
                _rwlock.ExitUpgradeableReadLock();
            }
        }
#pragma warning restore MEN003

        /// <summary>
        /// Render actions from block index of <paramref name="offset"/>.
        /// </summary>
        /// <param name="offset">Index of the block to start rendering from.</param>
        /// <param name="renderer">The renderer to render actions.</param>
        /// <param name="stateCompleters">The strategy to complement incomplete block states.
        /// <see cref="StateCompleterSet.Recalculate"/> by default.</param>
        /// <returns>The number of actions rendered.</returns>
        internal int RenderActionsInBlocks(
            long offset,
            IActionRenderer renderer,
            StateCompleterSet? stateCompleters = null)
        {
            // Since rendering process requires every step's states, if required block states
            // are incomplete they are complemented anyway:
            stateCompleters ??= StateCompleterSet.Recalculate;

            // FIXME: We should consider the case where block count is larger than int.MaxSize.
            int cnt = 0;
            foreach (var block in IterateBlocks((int)offset))
            {
                cnt += RenderActions(null, block, renderer, stateCompleters);
            }

            return cnt;
        }

        /// <summary>
        /// Render actions of the given <paramref name="block"/>.
        /// </summary>
        /// <param name="evaluations"><see cref="ActionEvaluation"/>s of the block.  If it is
        /// <c>null</c>, evaluate actions of the <paramref name="block"/> again.</param>
        /// <param name="block"><see cref="Block"/> to render actions.</param>
        /// <param name="renderer">The renderer to render actions.</param>
        /// <param name="stateCompleters">The strategy to complement incomplete block states.
        /// <see cref="StateCompleterSet.Recalculate"/> by default.</param>
        /// <returns>The number of actions rendered.</returns>
        internal int RenderActions(
            IReadOnlyList<ActionEvaluation> evaluations,
            Block block,
            IActionRenderer renderer,
            StateCompleterSet? stateCompleters = null
        )
        {
            _logger.Debug("Render actions in block {blockIndex}: {block}", block?.Index, block);

            // Since rendering process requires every step's states, if required block states
            // are incomplete they are complemented anyway:
            stateCompleters ??= StateCompleterSet.Recalculate;

            if (evaluations is null)
            {
                evaluations = ActionEvaluator.Evaluate(block, stateCompleters.Value);
            }

            int cnt = 0;
            foreach (var evaluation in evaluations)
            {
                if (evaluation.Exception is null)
                {
                    renderer.RenderAction(
                        evaluation.Action,
                        evaluation.InputContext.GetUnconsumedContext(),
                        evaluation.OutputStates
                    );
                }
                else
                {
                    renderer.RenderActionError(
                        evaluation.Action,
                        evaluation.InputContext.GetUnconsumedContext(),
                        evaluation.Exception
                    );
                }

                cnt++;
            }

            return cnt;
        }

        /// <summary>
        /// Evaluates actions in the given <paramref name="block"/> and fills states with the
        /// results.
        /// </summary>
        /// <param name="block">A block to execute.</param>
        /// <param name="stateCompleters">The strategy to complement incomplete previous block
        /// states.  <see cref="StateCompleterSet.Recalculate"/> by default.
        /// </param>
        /// <returns>The result of action evaluations of the given <paramref name="block"/>.
        /// </returns>
        /// <remarks>This method is idempotent (except for rendering).  If the given
        /// <paramref name="block"/> has executed before, it does not execute it nor mutate states.
        /// </remarks>
        internal IReadOnlyList<ActionEvaluation> ExecuteActions(
            Block block,
            StateCompleterSet? stateCompleters = null
        )
        {
            _logger.Debug(
                "Evaluating actions in the block #{BlockIndex} {BlockHash}...",
                block.Index,
                block.Hash
            );
            IReadOnlyList<ActionEvaluation> evaluations = null;
            DateTimeOffset evaluateActionStarted = DateTimeOffset.Now;
            evaluations = ActionEvaluator.Evaluate(
                block,
                stateCompleters ?? StateCompleterSet.Recalculate
            );
            const string evalEndMsg =
                "Evaluated actions in the block #{BlockIndex} {BlockHash} " +
                "(duration: {DurationMs}ms).";
            double evalDuration = (DateTimeOffset.Now - evaluateActionStarted).TotalMilliseconds;
            _logger.Debug(evalEndMsg, block.Index, block.Hash, evalDuration);

            _rwlock.EnterWriteLock();
            try
            {
                // Update states
                DateTimeOffset setStatesStarted = DateTimeOffset.Now;
                if (StateStore is TrieStateStore trieStateStore)
                {
                    var totalDelta =
                        evaluations.GetTotalDelta(ToStateKey, ToFungibleAssetKey);
                    const string deltaMsg =
                        "Summarized the states delta made by the block #{BlockIndex} {BlockHash}." +
                        "  Total {Keys} key(s) changed.";
                    _logger.Debug(deltaMsg, block.Index, block.Hash, totalDelta.Count);

                    HashDigest<SHA256> rootHash =
                        trieStateStore.EvalState(block, totalDelta);
                    const string rootHashMsg =
                        "Calculated the root hash of the states made by the block #{BlockIndex} " +
                        "{BlockHash} for " + nameof(TrieStateStore) + ": {StateRootHash}.";
                    _logger.Debug(rootHashMsg, block.Index, block.Hash, rootHash);

                    if (!rootHash.Equals(block.StateRootHash))
                    {
                        var message = $"The block #{block.Index} {block.Hash}'s state root hash " +
                                      $"is {block.StateRootHash?.ToString()}, but the execution " +
                                      $"result is {rootHash.ToString()}.";
                        throw new InvalidBlockStateRootHashException(
                            block.StateRootHash,
                            rootHash,
                            message);
                    }

                    trieStateStore.SetStates(block, rootHash);
                }
                else
                {
                    SetStates(block, evaluations);
                }

                const string endMsg =
                    "Finished to update states affected by the block #{BlockIndex} {BlockHash} " +
                    "(duration: {DurationMs}ms).";
                double duration = (DateTimeOffset.Now - setStatesStarted).TotalMilliseconds;
                _logger.Debug(endMsg, block.Index, block.Hash, duration);
            }
            finally
            {
                _rwlock.ExitWriteLock();
            }

            return evaluations;
        }

        /// <summary>
        /// Find an approximate to the topmost common ancestor between this
        /// <see cref="BlockChain"/> and a given <see cref="BlockLocator"/>.
        /// </summary>
        /// <param name="locator">A block locator that contains candidate common ancestors.</param>
        /// <returns>An approximate to the topmost common ancestor.  If it failed to find anything
        /// returns <c>null</c>.</returns>
        internal BlockHash? FindBranchpoint(BlockLocator locator)
        {
            try
            {
                _rwlock.EnterReadLock();

                _logger.Debug("Finding branchpoint (locator: {Locator}).", locator);
                foreach (BlockHash hash in locator)
                {
                    if (_blocks.ContainsKey(hash)
                        && _blocks[hash] is Block block
                        && hash.Equals(Store.IndexBlockHash(Id, block.Index)))
                    {
                        _logger.Debug(
                            "Found the branchpoint (locator: {Locator}): {Hash}.",
                            locator,
                            hash
                        );
                        return hash;
                    }
                }

                _logger.Debug("Failed to find the branchpoint (locator: {Locator}).", locator);
                return null;
            }
            finally
            {
                _rwlock.ExitReadLock();
            }
        }

        internal Tuple<long?, IReadOnlyList<BlockHash>> FindNextHashes(
            BlockLocator locator,
            BlockHash? stop = null,
            int count = 500)
        {
            try
            {
                _rwlock.EnterReadLock();

                BlockHash? tip = Store.IndexBlockHash(Id, -1);
                if (tip is null)
                {
                    return new Tuple<long?, IReadOnlyList<BlockHash>>(null, new BlockHash[0]);
                }

                BlockHash? branchpoint = FindBranchpoint(locator);
                var branchpointIndex = branchpoint is { } h ? (int)_blocks[h].Index : 0;

                // FIXME: Currently, increasing count by one to satisfy
                // the number defined by FindNextHashesChunkSize variable
                // when branchPointIndex didn't indicate genesis block.
                // Since branchPointIndex is same as the latest block of
                // requesting peer.
                if (branchpointIndex > 0)
                {
                    count++;
                }

                IEnumerable<BlockHash> hashes = Store.IterateIndexes(Id, branchpointIndex, count);

                var result = new List<BlockHash>();
                foreach (BlockHash hash in hashes)
                {
                    if (count == 0)
                    {
                        break;
                    }

                    result.Add(hash);

                    if (hash.Equals(stop))
                    {
                        break;
                    }

                    count--;
                }

                return new Tuple<long?, IReadOnlyList<BlockHash>>(branchpointIndex, result);
            }
            finally
            {
                _rwlock.ExitReadLock();
            }
        }

        internal BlockChain Fork(BlockHash point, bool inheritRenderers = true)
        {
            if (!ContainsBlock(point))
            {
                throw new ArgumentException(
                    $"The block [{point}] doesn't exist.",
                    nameof(point));
            }

            Block pointBlock = this[point];

            if (!point.Equals(this[pointBlock.Index].Hash))
            {
                throw new ArgumentException(
                    $"The block [{point}] doesn't exist in the chain index.",
                    nameof(point));
            }

            IEnumerable<IRenderer> renderers = inheritRenderers
                ? Renderers
                : Enumerable.Empty<IRenderer>();
            var forked = new BlockChain(
                Policy, StagePolicy, Store, StateStore, Guid.NewGuid(), Genesis, true, renderers);
            Guid forkedId = forked.Id;
            _logger.Debug(
                "Trying to fork chain at {branchPoint}" +
                "(prevId: {prevChainId}) (forkedId: {forkedChainId})",
                point,
                Id,
                forkedId);
            try
            {
                _rwlock.EnterReadLock();

                Store.ForkBlockIndexes(Id, forkedId, point);
                StateStore.ForkStates(Id, forked.Id, pointBlock);
                Store.ForkTxNonces(Id, forked.Id);

                for (Block block = Tip;
                     block.PreviousHash is { } hash && !block.Hash.Equals(point);
                     block = _blocks[hash])
                {
                    IEnumerable<(Address, int)> signers = block
                        .Transactions
                        .GroupBy(tx => tx.Signer)
                        .Select(g => (g.Key, g.Count()));

                    foreach ((Address address, int txCount) in signers)
                    {
                        Store.IncreaseTxNonce(forked.Id, address, -txCount);
                    }
                }
            }
            finally
            {
                _rwlock.ExitReadLock();
            }

            return forked;
        }

        internal BlockLocator GetBlockLocator(int threshold = 10)
        {
            try
            {
                _rwlock.EnterReadLock();

                return new BlockLocator(
                    indexBlockHash: idx => Store.IndexBlockHash(Id, idx),
                    indexByBlockHash: hash => _blocks[hash].Index,
                    sampleAfter: threshold
                );
            }
            finally
            {
                _rwlock.ExitReadLock();
            }
        }

        // FIXME it's very dangerous because replacing Id means
        // ALL blocks (referenced by MineBlock(), etc.) will be changed.
        // we need to add a synchronization mechanism to handle this correctly.
#pragma warning disable MEN003
        internal void Swap(
            BlockChain other,
            bool render,
            StateCompleterSet? stateCompleters = null
        )
        {
            if (other is null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            // As render/unrender processing requires every step's states from the branchpoint
            // to the new/stale tip, incomplete states need to be complemented anyway...
            StateCompleterSet completers = stateCompleters ?? StateCompleterSet.Recalculate;

            if (Tip.Equals(other.Tip))
            {
                // If it's swapped for a chain with the same tip, it means there is no state change.
                // Hence render is unnecessary.
                render = false;
            }
            else
            {
                _logger.Debug(
                    "The blockchain was reorged from " +
                    "{OldChainId} (#{OldTipIndex} {OldTipHash}) " +
                    "to {NewChainId} (#{NewTipIndex} {NewTipHash}).",
                    Id,
                    Tip.Index,
                    Tip.Hash,
                    other.Id,
                    other.Tip.Index,
                    other.Tip.Hash);
            }

            // Finds the branch point.
            Block topmostCommon = null;
            if (!(Tip is null))
            {
                long shorterHeight =
                    Math.Min(Count, other.Count) - 1;
                Block t = this[shorterHeight], o = other[shorterHeight];

                while (true)
                {
                    if (t.Equals(o))
                    {
                        topmostCommon = t;
                        break;
                    }

                    if (t.PreviousHash is { } tp && o.PreviousHash is { } op)
                    {
                        t = this[tp];
                        o = other[op];
                    }
                    else
                    {
                        break;
                    }
                }
            }

            if (topmostCommon is null)
            {
                const string msg =
                    "A chain cannot be reorged into a heterogeneous chain which has " +
                    "no common genesis at all.";
                throw new InvalidGenesisBlockException(Genesis.Hash, other.Genesis.Hash, msg);
            }

            _logger.Debug(
                "The branchpoint is #{BranchpointIndex} {BranchpointHash}.",
                topmostCommon.Index,
                topmostCommon
            );

            _rwlock.EnterUpgradeableReadLock();
            try
            {
                bool reorged = !Tip.Equals(topmostCommon);
                if (render && reorged)
                {
                    foreach (IRenderer renderer in Renderers)
                    {
                        renderer.RenderReorg(Tip, other.Tip, branchpoint: topmostCommon);
                    }
                }

                if (render && ActionRenderers.Any())
                {
                    // Unrender stale actions.
                    _logger.Debug("Unrendering abandoned actions...");
                    int cnt = 0;

                    for (
                        Block b = Tip;
                        !(b is null) && b.Index > (topmostCommon?.Index ?? -1) &&
                        b.PreviousHash is { } ph;
                        b = this[ph]
                    )
                    {
                        List<ActionEvaluation> evaluations =
                            ActionEvaluator.Evaluate(b, completers).ToList();
                        evaluations.Reverse();

                        foreach (var evaluation in evaluations)
                        {
                            _logger.Debug("Unrender an action: {Action}.", evaluation.Action);
                            if (evaluation.Exception is null)
                            {
                                foreach (IActionRenderer renderer in ActionRenderers)
                                {
                                    renderer.UnrenderAction(
                                        evaluation.Action,
                                        evaluation.InputContext.GetUnconsumedContext(),
                                        evaluation.OutputStates
                                    );
                                }
                            }
                            else
                            {
                                foreach (IActionRenderer renderer in ActionRenderers)
                                {
                                    renderer.UnrenderActionError(
                                        evaluation.Action,
                                        evaluation.InputContext.GetUnconsumedContext(),
                                        evaluation.Exception
                                    );
                                }
                            }

                            cnt++;
                        }
                    }

                    _logger.Debug(
                        $"{nameof(Swap)}() completed unrendering {{Actions}} actions.",
                        cnt);
                }

                Block oldTip = Tip ?? Genesis, newTip = other.Tip ?? other.Genesis;

                _rwlock.EnterWriteLock();
                try
                {
                    IEnumerable<Transaction>
                        GetTxsWithRange(BlockChain chain, Block start, Block end)
                        => Enumerable
                            .Range((int)start.Index + 1, (int)(end.Index - start.Index))
                            .SelectMany(x => chain[x].Transactions);

                    // It assumes reorg is small size. If it was big, this may be heavy task.
                    ImmutableHashSet<Transaction> unstagedTxs =
                        GetTxsWithRange(this, topmostCommon, Tip).ToImmutableHashSet();
                    ImmutableHashSet<Transaction> stageTxs =
                        GetTxsWithRange(other, topmostCommon, other.Tip).ToImmutableHashSet();
                    ImmutableHashSet<Transaction> restageTxs = unstagedTxs.Except(stageTxs);
                    foreach (Transaction restageTx in restageTxs)
                    {
                        StagePolicy.Stage(this, restageTx);
                    }

                    Guid obsoleteId = Id;
                    Id = other.Id;
                    Store.SetCanonicalChainId(Id);
                    _blocks = new BlockSet(Store);
                    TipChanged?.Invoke(this, (oldTip, newTip));

                    if (render)
                    {
                        foreach (IRenderer renderer in Renderers)
                        {
                            renderer.RenderBlock(oldTip: oldTip, newTip: newTip);
                        }
                    }

                    Store.DeleteChainId(obsoleteId);
                }
                finally
                {
                    _rwlock.ExitWriteLock();
                }

                if (render && ActionRenderers.Any())
                {
                    _logger.Debug("Rendering actions in new chain.");

                    // Render actions that had been behind.
                    long startToRenderIndex = topmostCommon is Block branchpoint
                        ? branchpoint.Index + 1
                        : 0;

                    foreach (IActionRenderer renderer in ActionRenderers)
                    {
                        int cnt = RenderActionsInBlocks(startToRenderIndex, renderer, completers);
                        _logger.Debug(
                            $"{nameof(Swap)}() completed rendering {{Count}} actions.",
                            cnt);

                        renderer.RenderBlockEnd(oldTip, newTip);
                    }
                }

                if (render && reorged)
                {
                    foreach (IRenderer renderer in Renderers)
                    {
                        renderer.RenderReorgEnd(oldTip, newTip, topmostCommon);
                    }
                }
            }
            finally
            {
                _rwlock.ExitUpgradeableReadLock();
            }
        }
#pragma warning restore MEN003

        internal ImmutableArray<Transaction> ListStagedTransactions()
        {
            Transaction[] txs = StagePolicy.Iterate(this).ToArray();

            Dictionary<Address, LinkedList<Transaction>> seats = txs
                .GroupBy(tx => tx.Signer)
                .Select(g => (g.Key, new LinkedList<Transaction>(g.OrderBy(tx => tx.Nonce))))
                .ToDictionary(pair => pair.Item1, pair => pair.Item2);

            return txs.Select(tx =>
            {
                LinkedList<Transaction> seat = seats[tx.Signer];
                Transaction first = seat.First.Value;
                seat.RemoveFirst();
                return first;
            }).ToImmutableArray();
        }

        internal void SetStates(
            Block block,
            IReadOnlyList<ActionEvaluation> actionEvaluations
        )
        {
           if (!StateStore.ContainsBlockStates(block.Hash))
           {
               var totalDelta = actionEvaluations.GetTotalDelta(ToStateKey, ToFungibleAssetKey);
               StateStore.SetStates(block, totalDelta);
           }
        }

        internal IEnumerable<Block> IterateBlocks(int offset = 0, int? limit = null)
        {
            _rwlock.EnterUpgradeableReadLock();

            try
            {
                foreach (BlockHash hash in IterateBlockHashes(offset, limit))
                {
                    yield return _blocks[hash];
                }
            }
            finally
            {
                _rwlock.ExitUpgradeableReadLock();
            }
        }

        internal IEnumerable<BlockHash> IterateBlockHashes(int offset = 0, int? limit = null)
        {
            _rwlock.EnterUpgradeableReadLock();

            try
            {
                IEnumerable<BlockHash> indices = Store.IterateIndexes(Id, offset, limit);

                // NOTE: The reason why this does not simply return indices, but iterates over
                // indices and yields hashes step by step instead, is that we need to ensure
                // the read lock held until the whole iteration completes.
                foreach (BlockHash hash in indices)
                {
                    yield return hash;
                }
            }
            finally
            {
                _rwlock.ExitUpgradeableReadLock();
            }
        }

        internal BlockHash? ActionEvaluationsToHash(IEnumerable<ActionEvaluation> actionEvaluations)
        {
            ActionEvaluation actionEvaluation;
            var evaluations = actionEvaluations.ToList();
            if (evaluations.Any())
            {
                actionEvaluation = evaluations.Last();
            }
            else
            {
                return null;
            }

            IImmutableSet<Address> updatedAddresses =
                actionEvaluation.OutputStates.UpdatedAddresses;
            var dict = Bencodex.Types.Dictionary.Empty;
            foreach (Address address in updatedAddresses)
            {
                dict.Add(address.ToHex(), actionEvaluation.OutputStates.GetState(address));
            }

            return Hashcash.Hash(new Codec().Encode(dict));
        }

        /// <summary>
        /// Calculates and complements a block's incomplete states on the fly.
        /// </summary>
        /// <param name="blockHash">The hash of a block which has incomplete states.</param>
        internal void ComplementBlockStates(BlockHash blockHash)
        {
            _logger.Verbose("Recalculates the block {BlockHash}'s states...", blockHash);

            // Prevent recursive trial to recalculate & complement incomplete block states by
            // mistake; if the below code works as intended, these state completers must never
            // be invoked.
            StateCompleterSet stateCompleters = StateCompleterSet.Reject;

            // Calculates and fills the incomplete states
            // on the fly.
            foreach (BlockHash hash in BlockHashes)
            {
                Block block = this[hash];
                if (StateStore.ContainsBlockStates(hash))
                {
                    continue;
                }

                IReadOnlyList<ActionEvaluation> evaluations = ActionEvaluator.Evaluate(
                    block,
                    stateCompleters
                );

                _rwlock.EnterWriteLock();
                try
                {
                    SetStates(block, evaluations);
                }
                finally
                {
                    _rwlock.ExitWriteLock();
                }
            }
        }

        private InvalidBlockException ValidateNextBlock(Block nextBlock)
        {
            int actualProtocolVersion = nextBlock.ProtocolVersion;
            const int currentProtocolVersion = Block.CurrentProtocolVersion;
            if (actualProtocolVersion > currentProtocolVersion)
            {
                string message =
                    $"The protocol version ({actualProtocolVersion}) of the block " +
                    $"#{nextBlock.Index} {nextBlock.Hash} is not supported by this node." +
                    $"The highest supported protocol version is {currentProtocolVersion}.";
                throw new InvalidBlockProtocolVersionException(
                    actualProtocolVersion,
                    message
                );
            }
            else if (Tip is { } tip && actualProtocolVersion < tip.ProtocolVersion)
            {
                string message =
                    "The protocol version is disallowed to be downgraded from the topmost block " +
                    $"in the chain ({actualProtocolVersion} < {tip.ProtocolVersion}).";
                throw new InvalidBlockProtocolVersionException(actualProtocolVersion, message);
            }

            InvalidBlockException e = Policy.ValidateNextBlock(this, nextBlock);

            if (!(e is null))
            {
                return e;
            }

            long index = this.Count;
            long difficulty = Policy.GetNextBlockDifficulty(this);
            BigInteger totalDifficulty = index >= 1
                    ? this[index - 1].TotalDifficulty + nextBlock.Difficulty
                    : nextBlock.Difficulty;

            Block lastBlock = index >= 1 ? this[index - 1] : null;
            BlockHash? prevHash = lastBlock?.Hash;
            DateTimeOffset? prevTimestamp = lastBlock?.Timestamp;

            if (nextBlock.Index != index)
            {
                return new InvalidBlockIndexException(
                    $"The expected index of block {nextBlock.Hash} is #{index}, " +
                    $"but its index is #{nextBlock.Index}.");
            }

            if (nextBlock.Difficulty < difficulty)
            {
                return new InvalidBlockDifficultyException(
                    $"The expected difficulty of the block #{index} {nextBlock.Hash} " +
                    $"is {difficulty}, but its difficulty is {nextBlock.Difficulty}.");
            }

            if (nextBlock.TotalDifficulty != totalDifficulty)
            {
                var msg = $"The expected total difficulty of the block #{index} " +
                          $"{nextBlock.Hash} is {totalDifficulty}, but its difficulty is " +
                          $"{nextBlock.TotalDifficulty}.";
                return new InvalidBlockTotalDifficultyException(
                    nextBlock.Difficulty,
                    nextBlock.TotalDifficulty,
                    msg);
            }

            if (!nextBlock.PreviousHash.Equals(prevHash))
            {
                if (prevHash is null)
                {
                    return new InvalidBlockPreviousHashException(
                        $"The genesis block {nextBlock.Hash} should not have previous hash, " +
                        $"but its value is {nextBlock.PreviousHash}.");
                }

                return new InvalidBlockPreviousHashException(
                    $"The block #{index} {nextBlock.Hash} is not continuous from the " +
                    $"block #{index - 1}; while previous block's hash is " +
                    $"{prevHash}, the block #{index} {nextBlock.Hash}'s pointer to " +
                    "the previous hash refers to " +
                    (nextBlock.PreviousHash?.ToString() ?? "nothing") + ".");
            }

            if (nextBlock.Timestamp < prevTimestamp)
            {
                return new InvalidBlockTimestampException(
                    $"The block #{index} {nextBlock.Hash}'s timestamp " +
                    $"({nextBlock.Timestamp}) is earlier than " +
                    $"the block #{index - 1}'s ({prevTimestamp}).");
            }

            return null;
        }

        private IValue GetRawState(
            string key,
            BlockHash? offset,
            Func<BlockChain, BlockHash, IValue> rawStateCompleter
        )
        {
            _rwlock.EnterUpgradeableReadLock();
            try
            {
                if (offset is null && Tip is null)
                {
                    return null;
                }

                offset ??= Tip.Hash;

                return StateStore.ContainsBlockStates(offset.Value)
                    ? StateStore.GetState(key, offset)
                    : rawStateCompleter(this, offset.Value);
            }
            finally
            {
                _rwlock.ExitUpgradeableReadLock();
            }
        }
    }
}
