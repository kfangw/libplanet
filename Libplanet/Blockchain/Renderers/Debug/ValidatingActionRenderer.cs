#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Libplanet.Action;
using Libplanet.Blockchain.Policies;
using Libplanet.Blocks;
using Libplanet.Store;

namespace Libplanet.Blockchain.Renderers.Debug
{
    /// <summary>
    /// Validates if rendering events are in the correct order according to the documented automata
    /// (see also the docs for <see cref="IRenderer"/> and <see cref="IActionRenderer"/>)
    /// using profiling-guided analysis.
    /// </summary>
    public class ValidatingActionRenderer : RecordingActionRenderer
    {
        private readonly Action<InvalidRenderException>? _onError;

        /// <summary>
        /// Creates a new <see cref="ValidatingActionRenderer"/> instance.
        /// </summary>
        /// <param name="onError">An optional event handler which is triggered when invalid
        /// render events occur.</param>
        public ValidatingActionRenderer(Action<InvalidRenderException>? onError = null)
        {
            _onError = onError;
        }

        private enum RenderState
        {
            Ready,
            Reorg,
            Block,
            BlockEnd,
        }

        /// <summary>
        /// The chain that publishes the render events.  More stricter validations are conducted
        /// if it's configured.
        /// </summary>
        public BlockChain? BlockChain { get; set; }

        /// <inheritdoc cref="IRenderer.RenderReorg(Block, Block, Block)"/>
        public override void RenderReorg(Block oldTip, Block newTip, Block branchpoint)
        {
            base.RenderReorg(oldTip, newTip, branchpoint);
            Validate();
        }

        /// <inheritdoc
        /// cref="IActionRenderer.UnrenderAction(IAction, IActionContext, IAccountStateDelta)"/>
        public override void UnrenderAction(
            IAction action,
            IActionContext context,
            IAccountStateDelta nextStates
        )
        {
            base.UnrenderAction(action, context, nextStates);
            Validate();
        }

        /// <inheritdoc
        /// cref="IActionRenderer.UnrenderActionError(IAction, IActionContext, Exception)"/>
        public override void UnrenderActionError(
            IAction action,
            IActionContext context,
            Exception exception
        )
        {
            base.UnrenderActionError(action, context, exception);
            Validate();
        }

        /// <inheritdoc cref="IRenderer.RenderBlock(Block, Block)"/>
        public override void RenderBlock(Block oldTip, Block newTip)
        {
            base.RenderBlock(oldTip, newTip);
            Validate();
        }

        /// <inheritdoc
        /// cref="IActionRenderer.RenderAction(IAction, IActionContext, IAccountStateDelta)"/>
        public override void RenderAction(
            IAction action,
            IActionContext context,
            IAccountStateDelta nextStates
        )
        {
            base.RenderAction(action, context, nextStates);
            Validate();
        }

        /// <inheritdoc
        /// cref="IActionRenderer.RenderActionError(IAction, IActionContext, Exception)"/>
        public override void RenderActionError(
            IAction action,
            IActionContext context,
            Exception exception
        )
        {
            base.RenderActionError(action, context, exception);
            Validate();
        }

        /// <inheritdoc cref="IActionRenderer.RenderBlockEnd(Block, Block)"/>
        public override void RenderBlockEnd(Block oldTip, Block newTip)
        {
            base.RenderBlockEnd(oldTip, newTip);
            Validate();
        }

        /// <inheritdoc cref="IRenderer.RenderReorgEnd(Block, Block, Block)"/>
        public override void RenderReorgEnd(
            Block oldTip,
            Block newTip,
            Block branchpoint
        )
        {
            base.RenderReorgEnd(oldTip, newTip, branchpoint);
            Validate();

            ValidateReorgEnd(oldTip, newTip, branchpoint);
        }

        private void ValidateReorgEnd(
            Block oldTip,
            Block newTip,
            Block branchpoint)
        {
            if (!(BlockChain is BlockChain chain))
            {
                return;
            }

            IBlockPolicy policy = chain.Policy;
            IStore store = chain.Store;

            List<IAction> expectedUnrenderedActions = new List<IAction>();
            Block block = oldTip;
            while (!block.Equals(branchpoint))
            {
                if (policy.BlockAction is IAction blockAction)
                {
                    expectedUnrenderedActions.Add(blockAction);
                }

                expectedUnrenderedActions.AddRange(
                    block.Transactions.SelectMany(t => t.Actions).Reverse());
                block = store.GetBlock(block.PreviousHash ??
                    throw Error(Records, "Reorg occurred from the chain with different genesis."));
            }

            IEnumerable<IAction> expectedRenderedActionsBuffer = new List<IAction>();
            block = newTip;
            while (!block.Equals(branchpoint))
            {
                IEnumerable<IAction> actions =
                    block.Transactions.SelectMany(t => t.Actions);
                if (policy.BlockAction is IAction blockAction)
                {
#if NET472 || NET471 || NET47 || NET462 || NET461
                    // Even though .NET Framework 4.6.1 or higher supports .NET Standard 2.0,
                    // versions lower than 4.8 lacks Enumerable.Append(IEnumerable, T) method.
                    actions = actions.Concat(new IAction[] { blockAction });
#else
#pragma warning disable PC002
                    actions = actions.Append(blockAction);
#pragma warning restore PC002
#endif
                }

                expectedRenderedActionsBuffer = actions.Concat(expectedRenderedActionsBuffer);
                block = store.GetBlock(block.PreviousHash ??
                    throw Error(Records, "Reorg occurred from the chain with different genesis."));
            }

            IAction[] expectedRenderedActions = expectedRenderedActionsBuffer.ToArray();
            List<IAction> actualRenderedActions = new List<IAction>();
            List<IAction> actualUnrenderedActions = new List<IAction>();
            foreach (var record in Records.Reverse())
            {
                if (record is RenderRecord.Reorg b && b.Begin)
                {
                    break;
                }

                if (record is RenderRecord.ActionBase a)
                {
                    if (a.Render)
                    {
                        actualRenderedActions.Add(a.Action);
                    }
                    else
                    {
                        actualUnrenderedActions.Add(a.Action);
                    }
                }
            }

            actualRenderedActions.Reverse();
            actualUnrenderedActions.Reverse();

            string ReprAction(IAction? action)
            {
                if (action is null)
                {
                    return "[N/A]";
                }

                return action.PlainValue.Inspection
                    .Replace(" \n ", " ")
                    .Replace(" \n", " ")
                    .Replace("\n ", " ")
                    .Replace("\n", " ");
            }

            string MakeErrorMessage(string prefix, IList<IAction> expected, IList<IAction> actual)
            {
                int expectN = expected.Count;
                int actualN = actual.Count;
                if (expectN != actualN)
                {
                    prefix += $" (expected: {expectN} actions, actual: {actualN} actions):";
                }

                var buffer = new StringBuilder();
                for (int i = 0, count = Math.Max(expectN, actualN); i < count; i++)
                {
                    IAction? e = i < expectN ? expected[i] : null;
                    IAction? a = i < actualN ? actual[i] : null;
                    if (!(e is null || a is null) && e.PlainValue.Equals(a.PlainValue))
                    {
                        buffer.Append($"\n\t  {ReprAction(e)}");
                    }
                    else
                    {
                        buffer.Append($"\n\tE {ReprAction(e)}");
                        buffer.Append($"\n\tA {ReprAction(a)}");
                    }
                }

                return $"{prefix}:{buffer}";
            }

            if (!actualUnrenderedActions.Select(a => a.PlainValue)
                    .SequenceEqual(expectedUnrenderedActions.Select(a => a.PlainValue)))
            {
                const string message =
                    "The unrender action records do not match with actions in the block when " +
                    "reorg occurred";
                throw Error(
                    Records,
                    MakeErrorMessage(message, expectedUnrenderedActions, actualUnrenderedActions)
                );
            }

            if (!actualRenderedActions.Select(a => a.PlainValue)
                    .SequenceEqual(expectedRenderedActions.Select(a => a.PlainValue)))
            {
                const string message =
                    "The render action record does not match with actions in the block when " +
                    "reorg occurred";
                throw Error(
                    Records,
                    MakeErrorMessage(message, expectedRenderedActions, actualRenderedActions)
                );
            }
        }

        private void Validate()
        {
            var state = RenderState.Ready;
            RenderRecord.Reorg? reorgState = null;
            RenderRecord.Block? blockState = null;
            long previousActionBlockIndex = -1L;
            var records = new List<RenderRecord>(Records.Count);

            Exception BadRenderExc(string msg) => Error(records, msg);

            foreach (RenderRecord record in Records)
            {
                records.Add(record);
                switch (state)
                {
                    case RenderState.Ready:
                    {
                        if (!(reorgState is null && blockState is null))
                        {
                            throw BadRenderExc(
                                $"Unexpected reorg/block states: {reorgState}/{blockState}."
                            );
                        }
                        else if (record is RenderRecord.BlockBase blockBase && blockBase.Begin)
                        {
                            if (blockBase is RenderRecord.Reorg reorg)
                            {
                                reorgState = reorg;
                                state = RenderState.Reorg;
                                break;
                            }
                            else if (blockBase is RenderRecord.Block block)
                            {
                                blockState = block;
                                state = RenderState.Block;
                                break;
                            }
                        }

                        throw BadRenderExc(
                            $"Expected {nameof(IRenderer.RenderReorg)} or " +
                            $"{nameof(IRenderer.RenderBlock)}."
                        );
                    }

                    case RenderState.Reorg:
                    {
#pragma warning disable S2589
                        if (reorgState is null || !(blockState is null))
#pragma warning restore S2589
                        {
                            throw BadRenderExc(
                                $"Unexpected reorg/block states: {reorgState}/{blockState}."
                            );
                        }
                        else if (record is RenderRecord.Block block && block.Begin)
                        {
                            if (block.OldTip != reorgState.OldTip ||
                                block.NewTip != reorgState.NewTip)
                            {
                                throw BadRenderExc(
                                    $"{nameof(IRenderer.RenderReorg)} and " +
                                    $"{nameof(IRenderer.RenderBlock)} which follows it should " +
                                    "have the same oldTip and newTip."
                                );
                            }

                            blockState = block;
                            state = RenderState.Block;
                            break;
                        }
                        else if (record is RenderRecord.ActionBase actionBase &&
                                 actionBase.Unrender)
                        {
                            long idx = actionBase.Context.BlockIndex;
                            long minIdx = reorgState.Branchpoint.Index + 1;
                            long maxIdx = previousActionBlockIndex < 0
                                ? reorgState.OldTip.Index
                                : previousActionBlockIndex;
                            if (idx < minIdx || idx > maxIdx)
                            {
                                throw BadRenderExc(
                                    "An action is from a block which has an unexpected index " +
                                    $"#{idx} (expected min: #{minIdx}; max: #{maxIdx})."
                                );
                            }

                            previousActionBlockIndex = idx;
                            break;
                        }

                        throw BadRenderExc(
                            $"Expected {nameof(IRenderer.RenderBlock)} or " +
                            $"{nameof(IActionRenderer.UnrenderAction)} or " +
                            $"{nameof(IActionRenderer.UnrenderActionError)}."
                        );
                    }

                    case RenderState.Block:
                    {
                        if (blockState is null)
                        {
                            throw BadRenderExc("Unexpected block state: null.");
                        }
                        else if (record is RenderRecord.Block block && block.End)
                        {
                            if (block.OldTip != blockState.OldTip ||
                                block.NewTip != blockState.NewTip)
                            {
                                throw BadRenderExc(
                                    $"{nameof(IRenderer.RenderBlock)} and " +
                                    $"{nameof(IActionRenderer.RenderBlockEnd)} which matches " +
                                    "to it should have the same oldTip and newTip."
                                );
                            }

#pragma warning disable S2583
                            state = reorgState is null ? RenderState.Ready : RenderState.BlockEnd;
#pragma warning restore S2583
                            blockState = null;
                            break;
                        }
                        else if (record is RenderRecord.ActionBase actionBase &&
                                 actionBase.Render)
                        {
                            long idx = actionBase.Context.BlockIndex;
                            if (reorgState is RenderRecord.Reorg reorg)
                            {
                                long minIdx = previousActionBlockIndex >= 0
                                    ? previousActionBlockIndex
                                    : reorg.Branchpoint.Index + 1;
                                long maxIdx = reorg.NewTip.Index;
                                if (idx < minIdx || idx > maxIdx)
                                {
                                    throw BadRenderExc(
                                        "An action is from a block which has an unexpected index " +
                                        $"#{idx} (expected min: #{minIdx}; max: #{maxIdx})."
                                    );
                                }
                            }
                            else if (idx != blockState.NewTip.Index)
                            {
                                throw BadRenderExc(
                                    "An action is from a block which has an unexpected index " +
                                    $"#{idx} (expected: #{blockState.NewTip.Index}."
                                );
                            }

                            previousActionBlockIndex = idx;
                            break;
                        }

                        throw BadRenderExc(
                            $"Expected {nameof(IActionRenderer.RenderBlockEnd)} or " +
                            $"{nameof(IActionRenderer.RenderAction)} or " +
                            $"{nameof(IActionRenderer.RenderActionError)}"
                        );
                    }

                    case RenderState.BlockEnd:
                    {
#pragma warning disable S2589
                        if (reorgState is null || !(blockState is null))
#pragma warning restore S2589
                        {
                            throw BadRenderExc(
                                $"Unexpected reorg/block states: {reorgState}/{blockState}."
                            );
                        }
                        else if (record is RenderRecord.Reorg reorg && reorg.End)
                        {
                            if (reorg.OldTip != reorgState.OldTip ||
                                reorg.NewTip != reorgState.NewTip ||
                                reorg.Branchpoint != reorgState.Branchpoint)
                            {
                                throw BadRenderExc(
                                    $"{nameof(IRenderer.RenderReorgEnd)} should match to " +
                                    $"{nameof(IActionRenderer.RenderReorg)}; they should have " +
                                    "the same oldTip, newTip, and branchpoint."
                                );
                            }

                            state = RenderState.Ready;
                            reorgState = null;
                            break;
                        }

                        throw BadRenderExc(
                            $"Expected {nameof(IActionRenderer.RenderReorgEnd)}."
                        );
                    }
                }
            }
        }

        private InvalidRenderException Error(IReadOnlyList<RenderRecord> records, string msg)
        {
            var exception = new InvalidRenderException(records, msg);
            _onError?.Invoke(exception);
            return exception;
        }
    }
}
