#nullable enable
using System;
using Libplanet.Blocks;
using Serilog;
using Serilog.Events;

namespace Libplanet.Blockchain.Renderers
{
    /// <summary>
    /// Decorates an <see cref="IRenderer"/> so that all event messages are logged.
    /// <para>Every single event message causes two log messages: one is logged <em>before</em>
    /// rendering, and other one is logged <em>after</em> rendering.  If any exception is thrown
    /// it is also logged with the log level <see cref="LogEventLevel.Error"/> (regardless of
    /// <see cref="Level"/> configuration).</para>
    /// </summary>
    /// <example>
    /// <code>
    /// IRenderer&lt;ExampleAction&gt; renderer = new SomeRenderer();
    /// // Wraps the renderer with LoggedRenderer:
    /// renderer = new LoggedRenderer&lt;ExampleAction&gt;(
    ///     renderer,
    ///     Log.Logger,
    ///     LogEventLevel.Information,
    /// );
    /// </code>
    /// </example>
    /// <remarks>Since <see cref="IActionRenderer"/> is a subtype of <see cref="IRenderer"/>,
    /// <see cref="LoggedRenderer(IRenderer, ILogger, LogEventLevel)"/> constructor can take
    /// an <see cref="IActionRenderer"/> instance as well.  However, even it takes an action
    /// renderer, action-level fine-grained events will not be logged.  For action renderers,
    /// please use <see cref="LoggedActionRenderer"/> instead.</remarks>
    public class LoggedRenderer : IRenderer
    {
        /// <summary>
        /// Creates a new <see cref="LoggedRenderer"/> instance which decorates the given
        /// <paramref name="renderer"/>.
        /// </summary>
        /// <param name="renderer">The actual renderer to forward all event messages to and actually
        /// render things.</param>
        /// <param name="logger">The logger to write log messages to.  Note that all log messages
        /// this decorator writes become in the context of the <paramref name="renderer"/>'s
        /// type (with the context property <c>SourceContext</c>).</param>
        /// <param name="level">The log event level.  All log messages become this level.</param>
        public LoggedRenderer(
            IRenderer renderer,
            ILogger logger,
            LogEventLevel level = LogEventLevel.Debug
        )
        {
            Renderer = renderer;
            Logger = logger.ForContext(renderer.GetType());
            Level = level;
        }

        /// <summary>
        /// The inner renderer to forward all event messages to and actually render things.
        /// </summary>
        public IRenderer Renderer { get; }

        /// <summary>
        /// The log event level.  All log messages become this level.
        /// </summary>
        public LogEventLevel Level { get; }

        /// <summary>
        /// The logger to write log messages to.  Note that all log messages this decorator writes
        /// become in the context of the <see cref="Renderer"/>'s type (with the context
        /// property <c>SourceContext</c>).
        /// </summary>
        protected ILogger Logger { get; }

        /// <inheritdoc cref="IRenderer.RenderBlock(Block, Block)"/>
        public void RenderBlock(
            Block oldTip,
            Block newTip
        ) =>
            LogBlockRendering(
                nameof(RenderBlock),
                oldTip,
                newTip,
                Renderer.RenderBlock
            );

        /// <inheritdoc cref="IRenderer.RenderReorg(Block, Block, Block)"/>
        public void RenderReorg(
            Block oldTip,
            Block newTip,
            Block branchpoint
        ) =>
            LogReorgRendering(
                nameof(RenderReorg),
                oldTip,
                newTip,
                branchpoint,
                Renderer.RenderReorg
            );

        /// <inheritdoc cref="IRenderer.RenderReorg(Block, Block, Block)"/>
        public void RenderReorgEnd(
            Block oldTip,
            Block newTip,
            Block branchpoint
        ) =>
            LogReorgRendering(
                nameof(RenderReorgEnd),
                oldTip,
                newTip,
                branchpoint,
                Renderer.RenderReorgEnd
            );

        protected void LogBlockRendering(
            string methodName,
            Block oldTip,
            Block newTip,
            System.Action<Block, Block> callback
        )
        {
            Logger.Write(
                Level,
                "Invoking {MethodName}() for #{NewIndex} {NewHash} (was #{OldIndex} {OldHash})...",
                methodName,
                newTip.Index,
                newTip.Hash,
                oldTip.Index,
                oldTip.Hash
            );

            try
            {
                callback(oldTip, newTip);
            }
            catch (Exception e)
            {
                const string errorMessage =
                    "An exception was thrown during {MethodName}() for #{NewIndex} {NewHash} " +
                    "(was #{OldIndex} {OldHash}): {Exception}";
                Logger.Error(
                    e,
                    errorMessage,
                    methodName,
                    newTip.Index,
                    newTip.Hash,
                    oldTip.Index,
                    oldTip.Hash,
                    e
                );
                throw;
            }

            Logger.Write(
                Level,
                "Invoked {MethodName}() for #{NewIndex} {NewHash} (was #{OldIndex} {OldHash}).",
                methodName,
                newTip.Index,
                newTip.Hash,
                oldTip.Index,
                oldTip.Hash
            );
        }

        private void LogReorgRendering(
            string methodName,
            Block oldTip,
            Block newTip,
            Block branchpoint,
            System.Action<Block, Block, Block> callback
        )
        {
            const string startMessage =
                "Invoking {MethodName}() for #{NewIndex} {NewHash} (was #{OldIndex} {OldHash} " +
                "through #{BranchpointIndex} {BranchpointHash})...";
            Logger.Write(
                Level,
                startMessage,
                methodName,
                newTip.Index,
                newTip.Hash,
                oldTip.Index,
                oldTip.Hash,
                branchpoint.Index,
                branchpoint.Hash
            );

            try
            {
                callback(oldTip, newTip, branchpoint);
            }
            catch (Exception e)
            {
                const string errorMessage =
                    "An exception was thrown during {MethodName}() for #{NewIndex} {NewHash} " +
                    "(was #{OldIndex} {OldHash} through #{BranchpointIndex} {BranchpointHash}): " +
                    "{Exception}";
                Logger.Error(
                    e,
                    errorMessage,
                    methodName,
                    newTip.Index,
                    newTip.Hash,
                    oldTip.Index,
                    oldTip.Hash,
                    branchpoint.Index,
                    branchpoint.Hash,
                    e
                );
                throw;
            }

            const string endMessage =
                "Invoked {MethodName}() for #{NewIndex} {NewHash} (was #{OldIndex} {OldHash} " +
                "through #{BranchpointIndex} {BranchpointHash}).";
            Logger.Write(
                Level,
                endMessage,
                methodName,
                newTip.Index,
                newTip.Hash,
                oldTip.Index,
                oldTip.Hash,
                branchpoint.Index,
                branchpoint.Hash
            );
        }
    }
}
