#nullable enable
using Bencodex.Types;
using Libplanet.Action;
using Libplanet.Blocks;

namespace Libplanet.Blockchain
{
    /// <summary>
    /// A delegate to be called when <see cref="BlockChain.GetState"/> method encounters a block
    /// having incomplete dirty states. <see cref="BlockChain.GetState"/> method returns this
    /// delegate's return value instead for such case.
    /// </summary>
    /// <param name="blockChain">The blockchain to query.</param>
    /// <param name="blockHash">The hash of a block to lacks its dirty states.</param>
    /// <param name="address">The address to query its state value.</param>
    /// <returns>A complement state.  This can be <c>null</c>.</returns>
    /// <seealso cref="StateCompleters"/>
    public delegate IValue StateCompleter(
        BlockChain blockChain,
        BlockHash blockHash,
        Address address
    );
}
