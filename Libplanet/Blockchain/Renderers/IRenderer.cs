#nullable enable
using Libplanet.Action;
using Libplanet.Blocks;

namespace Libplanet.Blockchain.Renderers
{
    /// <summary>
    /// Listens state changes on a <see cref="BlockChain"/>.
    /// <para>Usually, implementations of this interface purpose to update the in-memory game states
    /// (if exist), or send a signal to the UI thread (usually the main thread) so that the graphics
    /// on the display is redrawn.</para>
    /// <para>The invocation order of methods for each <see cref="Block"/> are:</para>
    /// <list type="number">
    /// <item><description><see cref="RenderReorg(Block, Block, Block)"/> (one time)
    /// </description></item>
    /// <item><description><see cref="RenderBlock(Block, Block)"/> (one time)</description>
    /// </item>
    /// <item><description><see cref="RenderReorgEnd(Block, Block, Block)"/> (one time)
    /// </description></item>
    /// </list>
    /// </summary>
    public interface IRenderer
    {
        /// <summary>
        /// Does things that should be done right after a new <see cref="Block"/> is appended to
        /// a <see cref="BlockChain"/> (so that its <see cref="BlockChain.Tip"/> has changed).
        /// </summary>
        /// <remarks>It is guaranteed to be called only once for a block, and only after applied to
        /// the blockchain, unless it has been stale due to reorg (for that case, <see
        /// cref="RenderReorg(Block, Block, Block)"/> is called in advance).</remarks>
        /// <param name="oldTip">The previous <see cref="BlockChain.Tip"/>.</param>
        /// <param name="newTip">The current <see cref="BlockChain.Tip"/>.</param>
        void RenderBlock(Block oldTip, Block newTip);

        /// <summary>
        /// Does things that should be done right before reorg happens to a <see
        /// cref="BlockChain"/>.
        /// </summary>
        /// <remarks>For every call to this method, calls to
        /// <see cref="RenderBlock(Block, Block)"/> and
        /// <see cref="RenderReorgEnd(Block, Block, Block)" /> methods with the same
        /// <paramref name="newTip"/> is made too.  Note that this method is guaranteed to be called
        /// before <see cref="RenderBlock(Block, Block)"/> method for the same
        /// <paramref name="newTip"/>.</remarks>
        /// <param name="oldTip">The <see cref="BlockChain.Tip"/> right before reorg.</param>
        /// <param name="newTip">The <see cref="BlockChain.Tip"/> after reorg.</param>
        /// <param name="branchpoint">The highest common <see cref="Block"/> between
        /// <paramref name="oldTip"/> and <paramref name="newTip"/>.</param>
        void RenderReorg(Block oldTip, Block newTip, Block branchpoint);

        /// <summary>
        /// Does things that should be done right after reorg happens to a <see
        /// cref="BlockChain"/>.
        /// </summary>
        /// <remarks>Note that this method is guaranteed to be called after
        /// <see cref="RenderReorg(Block, Block, Block)"/> and
        /// <see cref="RenderBlock(Block, Block)"/> methods for the same
        /// <paramref name="newTip"/>.</remarks>
        /// <param name="oldTip">The <see cref="BlockChain.Tip"/> right before reorg.</param>
        /// <param name="newTip">The <see cref="BlockChain.Tip"/> after reorg.</param>
        /// <param name="branchpoint">The highest common <see cref="Block"/> between
        /// <paramref name="oldTip"/> and <paramref name="newTip"/>.</param>
        void RenderReorgEnd(Block oldTip, Block newTip, Block branchpoint);
    }
}
