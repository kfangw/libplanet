#nullable enable
using System;
using System.Collections.Generic;
using Libplanet.Action;
using Libplanet.Blocks;
using Libplanet.Tx;

namespace Libplanet.Blockchain.Renderers
{
    /// <summary>
    /// A middleware to make action render events to satisfy transactions' atomicity.
    /// <para>Decorates an <see cref="IActionRenderer"/> instance and filters out render events
    /// made by unsuccessful transactions (i.e., transactions with one or more exception-throwing
    /// actions).</para>
    /// </summary>
    /// <remarks>The wrapped <see cref="ActionRenderer"/> will not receive any
    /// <see cref="IActionRenderer.RenderActionError"/> and
    /// <see cref="IActionRenderer.UnrenderActionError"/> events except for block actions,
    /// which do not belong to any transactions.
    /// </remarks>
    public sealed class AtomicActionRenderer : IActionRenderer
    {
        private readonly List<(IAction, IActionContext, IAccountStateDelta)> _eventBuffer;
        private TxId? _lastTxId;
        private bool _errored;

        /// <summary>
        /// Creates a new <see cref="AtomicActionRenderer"/> instance decorating the given
        /// <paramref name="actionRenderer"/>.
        /// </summary>
        /// <param name="actionRenderer">The inner action renderer which has the <em>actual</em>
        /// implementations and expects to receive no <see cref="RenderActionError"/>/<see
        /// cref="UnrenderActionError"/> events.</param>
        public AtomicActionRenderer(IActionRenderer actionRenderer)
        {
            ActionRenderer = actionRenderer;
            _lastTxId = null;
            _eventBuffer = new List<(IAction, IActionContext, IAccountStateDelta)>();
            _errored = false;
        }

        /// <summary>
        /// The inner action renderer which has the <em>actual</em> implementations and expects to
        /// receive no <see cref="RenderActionError"/>/<see cref="UnrenderActionError"/> events.
        /// </summary>
        public IActionRenderer ActionRenderer { get; }

        /// <inheritdoc cref="IRenderer.RenderBlock(Block, Block)"/>
        public void RenderBlock(Block oldTip, Block newTip)
        {
            FlushBuffer(null, ActionRenderer.UnrenderAction);
            ActionRenderer.RenderBlock(oldTip, newTip);
        }

        /// <inheritdoc cref="IActionRenderer.RenderBlockEnd(Block, Block)"/>
        public void RenderBlockEnd(Block oldTip, Block newTip)
        {
            FlushBuffer(null, ActionRenderer.RenderAction);
            ActionRenderer.RenderBlockEnd(oldTip, newTip);
        }

        /// <inheritdoc cref="IRenderer.RenderReorg(Block, Block, Block)"/>
        public void RenderReorg(Block oldTip, Block newTip, Block branchpoint)
        {
            ActionRenderer.RenderReorg(oldTip, newTip, branchpoint);
        }

        /// <inheritdoc cref="IRenderer.RenderReorgEnd(Block, Block, Block)"/>
        public void RenderReorgEnd(Block oldTip, Block newTip, Block branchpoint)
        {
            ActionRenderer.RenderReorgEnd(oldTip, newTip, branchpoint);
        }

        /// <inheritdoc
        /// cref="IActionRenderer.RenderAction(IAction, IActionContext, IAccountStateDelta)"/>
        public void RenderAction(
            IAction action,
            IActionContext context,
            IAccountStateDelta nextStates
        )
        {
            if (!context.TxId.Equals(_lastTxId))
            {
                FlushBuffer(context.TxId, ActionRenderer.RenderAction);
            }

            if (context.TxId is null)
            {
                ActionRenderer.RenderAction(action, context, nextStates);
            }
            else if (!_errored)
            {
                _eventBuffer.Add((action, context, nextStates));
            }
        }

        /// <inheritdoc
        /// cref="IActionRenderer.UnrenderAction(IAction, IActionContext, IAccountStateDelta)"/>
        public void UnrenderAction(
            IAction action,
            IActionContext context,
            IAccountStateDelta nextStates
        )
        {
            if (!context.TxId.Equals(_lastTxId))
            {
                FlushBuffer(context.TxId, ActionRenderer.UnrenderAction);
            }

            if (context.TxId is null)
            {
                ActionRenderer.UnrenderAction(action, context, nextStates);
            }
            else if (!_errored)
            {
                _eventBuffer.Add((action, context, nextStates));
            }
        }

        /// <inheritdoc
        /// cref="IActionRenderer.RenderActionError(IAction, IActionContext, Exception)"/>
        public void RenderActionError(IAction action, IActionContext context, Exception exception)
        {
            if (!context.TxId.Equals(_lastTxId))
            {
                FlushBuffer(context.TxId, ActionRenderer.RenderAction);
            }

            if (context.TxId is null)
            {
                ActionRenderer.RenderActionError(action, context, exception);
            }
            else
            {
                _errored = true;
            }
        }

        /// <inheritdoc
        /// cref="IActionRenderer.UnrenderActionError(IAction, IActionContext, Exception)"/>
        public void UnrenderActionError(IAction action, IActionContext context, Exception exception)
        {
            if (!context.TxId.Equals(_lastTxId))
            {
                FlushBuffer(context.TxId, ActionRenderer.UnrenderAction);
            }

            if (context.TxId is null)
            {
                ActionRenderer.RenderActionError(action, context, exception);
            }
            else
            {
                _errored = true;
            }
        }

        private void FlushBuffer(
            TxId? newTxId,
            Action<IAction, IActionContext, IAccountStateDelta> render
        )
        {
            if (!_errored)
            {
                foreach (var (act, ctx, delta) in _eventBuffer)
                {
                    render(act, ctx, delta);
                }
            }

            _lastTxId = newTxId;
            _errored = false;
            _eventBuffer.Clear();
        }
    }
}
