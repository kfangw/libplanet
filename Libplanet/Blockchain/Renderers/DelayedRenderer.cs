#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Libplanet.Blockchain.Policies;
using Libplanet.Blocks;
using Libplanet.Store;
using Serilog;

namespace Libplanet.Blockchain.Renderers
{
    /// <summary>
    /// Decorates an <see cref="IRenderer"/> instance and delays the events until blocks
    /// are <em>confirmed</em> the certain number of blocks.  When blocks are recognized
    /// the delayed events relevant to these blocks are relayed to the decorated
    /// <see cref="IRenderer"/>.
    /// </summary>
    /// <example>
    /// <code><![CDATA[
    /// IStore store = GetStore();
    /// IBlockPolicy<ExampleAction> policy = GetPolicy();
    /// IRenderer<ExampleAction> renderer = new SomeRenderer();
    /// // Wraps the renderer with DelayedRenderer; the SomeRenderer instance becomes to receive
    /// // event messages only after the relevent blocks are confirmed by 3+ blocks.
    /// renderer = new DelayedRenderer<ExampleAction>(renderer, policy, store, confirmations: 3);
    /// // You must pass the same policy & store to the BlockChain() constructor:
    /// var chain = new BlockChain<ExampleAction>(
    ///     ...,
    ///     policy: policy,
    ///     store: store,
    ///     renderers: new[] { renderer });
    /// ]]></code>
    /// </example>
    /// <remarks>Since <see cref="IActionRenderer"/> is a subtype of <see cref="IRenderer"/>,
    /// <see cref="DelayedRenderer(IRenderer, IComparer{BlockPerception}, IStore, int)"/>
    /// constructor can take an <see cref="IActionRenderer"/> instance as well.
    /// However, even it takes an action renderer, action-level fine-grained events won't hear.
    /// For action renderers, please use <see cref="DelayedActionRenderer"/> instead.</remarks>
    public class DelayedRenderer : IRenderer
    {
        private Block? _tip;

        /// <summary>
        /// Creates a new <see cref="DelayedRenderer"/> instance decorating the given
        /// <paramref name="renderer"/>.
        /// </summary>
        /// <param name="renderer">The renderer to decorate which has the <em>actual</em>
        /// implementations and receives delayed events.</param>
        /// <param name="canonicalChainComparer">The same canonical chain comparer to
        /// <see cref="BlockChain.Policy"/>.</param>
        /// <param name="store">The same store to what <see cref="BlockChain"/> uses.</param>
        /// <param name="confirmations">The required number of confirmations to recognize a block.
        /// It must be greater than zero (note that zero <paramref name="confirmations"/> mean
        /// nothing is delayed so that it is equivalent to the bare <paramref name="renderer"/>).
        /// See also the <see cref="Confirmations"/> property.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the argument
        /// <paramref name="confirmations"/> is not greater than zero.</exception>
        public DelayedRenderer(
            IRenderer renderer,
            IComparer<BlockPerception> canonicalChainComparer,
            IStore store,
            int confirmations
        )
        {
            if (confirmations == 0)
            {
                string msg =
                    "Zero confirmations mean nothing is delayed so that it is equivalent to the " +
                    $"bare {nameof(renderer)}; configure it to more than zero.";
                throw new ArgumentOutOfRangeException(nameof(confirmations), msg);
            }
            else if (confirmations < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(confirmations),
                    $"Expected more than zero {nameof(confirmations)}."
                );
            }

            Logger = Log.ForContext(GetType());
            Renderer = renderer;
            CanonicalChainComparer = canonicalChainComparer;
            Store = store;
            Confirmations = confirmations;
            Confirmed = new ConcurrentDictionary<BlockHash, uint>();
        }

        /// <summary>
        /// The inner renderer which has the <em>actual</em> implementations and receives delayed
        /// events.
        /// </summary>
        public IRenderer Renderer { get; }

        /// <summary>
        /// The same canonical chain comparer to <see cref="BlockChain.Policy"/>.
        /// </summary>
        /// <seealso cref="IBlockPolicy.CanonicalChainComparer"/>
        public IComparer<BlockPerception> CanonicalChainComparer { get; }

        /// <summary>
        /// The same store to what <see cref="BlockChain"/> uses.
        /// </summary>
        public IStore Store { get; }

        /// <summary>
        /// The required number of confirmations to recognize a block.
        /// <para>For example, the required confirmations are 2, the block #N is recognized after
        /// the block #N+1 and the block #N+2 are discovered.</para>
        /// </summary>
        public int Confirmations { get; }

        /// <summary>
        /// The <em>recognized</em> topmost block.  If not enough blocks are discovered yet,
        /// this property can be <c>null</c>.
        /// </summary>
        public Block? Tip
        {
            get => _tip;
            private set
            {
                Block? newTip = value;
                if (newTip is null || newTip.Equals(_tip))
                {
                    return;
                }

                if (_tip is null)
                {
                    Logger.Verbose(
                        $"{nameof(DelayedRenderer)}.{nameof(Tip)} is tried to be updated to " +
                        "#{NewTipIndex} {NewTipHash} (from null).",
                        newTip.Index,
                        newTip.Hash
                    );
                }
                else
                {
                    Logger.Verbose(
                        $"{nameof(DelayedRenderer)}.{nameof(Tip)} is tried to be updated to " +
                        "#{NewTipIndex} {NewTipHash} (from #{OldTipIndex} {OldTipHash}).",
                        newTip.Index,
                        newTip.Hash,
                        _tip.Index,
                        _tip.Hash
                    );
                }

                Block? oldTip = _tip;
                _tip = newTip;
                if (oldTip is null)
                {
                    Logger.Debug(
                        $"{nameof(DelayedRenderer)}.{nameof(Tip)} was updated to " +
                        "#{NewTipIndex} {NewTipHash} (from null).",
                        newTip.Index,
                        newTip.Hash
                    );
                }
                else
                {
                    Logger.Debug(
                        $"{nameof(DelayedRenderer)}.{nameof(Tip)} was updated to " +
                        "#{NewTipIndex} {NewTipHash} (from #{OldTipIndex} {OldTipHash}).",
                        newTip.Index,
                        newTip.Hash,
                        oldTip.Index,
                        oldTip.Hash
                    );
                }

                if (oldTip is Block oldTip_ && !oldTip.Equals(newTip))
                {
                    Block? branchpoint = null;
                    if (!newTip.PreviousHash.Equals(oldTip_.Hash))
                    {
                        branchpoint = FindBranchpoint(oldTip, newTip);
                        if (branchpoint.Equals(oldTip) || branchpoint.Equals(newTip))
                        {
                            branchpoint = null;
                        }
                    }

                    OnTipChanged(oldTip, newTip, branchpoint);
                }
            }
        }

        /// <summary>
        /// The logger to record internal state changes.
        /// </summary>
        protected ILogger Logger { get; }

        protected ConcurrentDictionary<BlockHash, uint> Confirmed { get; }

        /// <inheritdoc cref="IRenderer.RenderBlock(Block, Block)"/>
        public virtual void RenderBlock(Block oldTip, Block newTip)
        {
            Confirmed.TryAdd(oldTip.Hash, 0);
            DiscoverBlock(oldTip, newTip);
        }

        /// <inheritdoc cref="IRenderer.RenderReorg(Block, Block, Block)"/>
        public virtual void RenderReorg(Block oldTip, Block newTip, Block branchpoint)
        {
            Confirmed.TryAdd(branchpoint.Hash, 0);
        }

        /// <inheritdoc cref="IRenderer.RenderReorgEnd(Block, Block, Block)"/>
        public virtual void RenderReorgEnd(Block oldTip, Block newTip, Block branchpoint)
        {
        }

        /// <summary>
        /// The callback method which is invoked when the new <see cref="Tip"/> is recognized and
        /// changed.
        /// </summary>
        /// <param name="oldTip">The previously recognized topmost block.</param>
        /// <param name="newTip">The topmost block recognized this time.</param>
        /// <param name="branchpoint">A branchpoint between <paramref name="oldTip"/> and
        /// <paramref name="newTip"/> if the tip change is a reorg.  Otherwise <c>null</c>.</param>
        protected virtual void OnTipChanged(Block oldTip, Block newTip, Block? branchpoint)
        {
            if (branchpoint is Block)
            {
                Renderer.RenderReorg(oldTip, newTip, branchpoint);
            }

            Renderer.RenderBlock(oldTip, newTip);

            if (branchpoint is Block)
            {
                Renderer.RenderReorgEnd(oldTip, newTip, branchpoint);
            }
        }

        protected void DiscoverBlock(Block oldTip, Block newTip)
        {
            if (Confirmed.ContainsKey(newTip.Hash))
            {
                return;
            }

            Block branchpoint = FindBranchpoint(oldTip, newTip);
            var maxDepth = branchpoint.Index < Confirmations
                ? 0
                : branchpoint.Index - Confirmations;

            Confirmed.TryAdd(newTip.Hash, 0U);

            var blocksToRender = new Stack<(long, BlockHash)>();

            long prevBlockIndex = newTip.Index - 1;
            uint accumulatedConfirmations = 0;
            BlockHash? prev = newTip.PreviousHash;
            while (
                prev is { } prevHash
                && Store.GetBlock(prevHash) is Block prevBlock
                && prevBlock.Index >= maxDepth)
            {
                uint c = Confirmed.GetOrAdd(prevHash, k => 0U);

                if (c >= Confirmations)
                {
                    break;
                }

                accumulatedConfirmations += 1;
                blocksToRender.Push((prevBlockIndex, prevHash));

                prevBlockIndex -= 1;
                prev = prevBlock.PreviousHash;
            }

            while (blocksToRender.Count > 0)
            {
                (long index, BlockHash hash) = blocksToRender.Pop();
                uint ac = accumulatedConfirmations;
                uint c = Confirmed.AddOrUpdate(hash, k => 0U, (k, v) => ac);

                Logger.Verbose(
                    "The block #{BlockIndex} {BlockHash} has {Confirmations} confirmations. ",
                    index,
                    hash,
                    c
                );

                if (accumulatedConfirmations > 0)
                {
                    accumulatedConfirmations -= 1;
                }

                if (c >= Confirmations)
                {
                    var confirmedBlock = Store.GetBlock(hash);

                    if (!(Tip is Block t))
                    {
                        Logger.Verbose(
                            "Promoting #{NewTipIndex} {NewTipHash} as a new tip since there is " +
                            "no tip yet...",
                            confirmedBlock.Index,
                            confirmedBlock.Hash
                        );
                        Tip = confirmedBlock;
                    }
                    else if (t.TotalDifficulty < confirmedBlock.TotalDifficulty)
                    {
                        Logger.Verbose(
                            "Promoting #{NewTipIndex} {NewTipHash} as a new tip since its total " +
                            "difficulty is more than the previous tip #{PreviousTipIndex} " +
                            "{PreviousTipHash} ({NewDifficulty} > {PreviousDifficulty}).",
                            confirmedBlock.Index,
                            confirmedBlock.Hash,
                            t.Index,
                            t.Hash,
                            confirmedBlock.TotalDifficulty,
                            t.TotalDifficulty
                        );
                        Tip = confirmedBlock;
                    }
                    else
                    {
                        Logger.Verbose(
                            "Although #{BlockIndex} {BlockHash} has been confirmed enough," +
                            "its difficulty is less than the current tip #{TipIndex} {TipHash} " +
                            "({Difficulty} < {TipDifficulty}).",
                            confirmedBlock.Index,
                            confirmedBlock.Hash,
                            t.Index,
                            t.Hash,
                            confirmedBlock.TotalDifficulty,
                            t.TotalDifficulty
                        );
                    }
                }
            }
        }

        private Block FindBranchpoint(Block a, Block b)
        {
            while (a is Block && a.Index > b.Index && a.PreviousHash is { } aPrev)
            {
                a = Store.GetBlock(aPrev);
            }

            while (b is Block && b.Index > a.Index && b.PreviousHash is { } bPrev)
            {
                b = Store.GetBlock(bPrev);
            }

            if (a is null || b is null || a.Index != b.Index)
            {
                throw new ArgumentException(
                    "Some previous blocks of two blocks are orphan.",
                    nameof(a)
                );
            }

            while (a.Index >= 0)
            {
                if (a.Equals(b))
                {
                    return a;
                }

                if (a.PreviousHash is { } aPrev &&
                    b.PreviousHash is { } bPrev)
                {
                    a = Store.GetBlock(aPrev);
                    b = Store.GetBlock(bPrev);
                    continue;
                }

                break;
            }

            throw new ArgumentException(
                "Two blocks do not have any ancestors in common.",
                nameof(a)
            );
        }
    }
}
