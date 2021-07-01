#nullable enable
using System;
using Libplanet.Action;
using Libplanet.Blocks;

namespace Libplanet.Blockchain.Renderers
{
    /// <summary>
    /// A renderer that invokes its callbacks.
    /// <para>This class is useful when you want an one-use ad-hoc implementation (i.e., Java-style
    /// anonymous class) of <see cref="IRenderer"/> interface.</para>
    /// </summary>
    /// <example>
    /// With object initializers, you can easily make an one-use renderer:
    /// <code>
    /// var renderer = new AnonymousRenderer&lt;ExampleAction&gt;
    /// {
    ///     BlockRenderer = (oldTip, newTip) =>
    ///     {
    ///         // Implement RenderBlock() here.
    ///     };
    /// };
    /// </code>
    /// </example>
    public class AnonymousRenderer : IRenderer
    {
        /// <summary>
        /// A callback function to be invoked together with
        /// <see cref="RenderBlock(Block, Block)"/>.
        /// </summary>
        public Action<Block, Block>? BlockRenderer { get; set; }

        /// <summary>
        /// A callback function to be invoked together with
        /// <see cref="RenderReorg(Block, Block, Block)"/>.
        /// </summary>
        public Action<Block, Block, Block>? ReorgRenderer { get; set; }

        /// <summary>
        /// A callback function to be invoked together with
        /// <see cref="RenderReorgEnd(Block, Block, Block)"/>.
        /// </summary>
        public Action<Block, Block, Block>? ReorgEndRenderer { get; set; }

        /// <inheritdoc cref="IRenderer.RenderBlock(Block, Block)"/>
        public void RenderBlock(Block oldTip, Block newTip) =>
            BlockRenderer?.Invoke(oldTip, newTip);

        /// <inheritdoc cref="IRenderer.RenderReorg(Block, Block, Block)"/>
        public void RenderReorg(Block oldTip, Block newTip, Block branchpoint) =>
            ReorgRenderer?.Invoke(oldTip, newTip, branchpoint);

        /// <inheritdoc cref="IRenderer.RenderReorgEnd(Block, Block, Block)"/>
        public void RenderReorgEnd(Block oldTip, Block newTip, Block branchpoint) =>
            ReorgEndRenderer?.Invoke(oldTip, newTip, branchpoint);
    }
}
