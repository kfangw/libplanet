#nullable enable
using Bencodex.Types;
using Libplanet.Blockchain;
using Libplanet.Blocks;

namespace Libplanet.Action
{
    /// <summary>
    /// An intermediary delegate for <see cref="AccountStateGetter"/>.
    /// </summary>
    /// <param name="address">The address of an account to read the state from.</param>
    /// <param name="hashDigest">The address of a block to read the state from.</param>
    /// <param name="stateCompleter">The <see cref="StateCompleter"/> to use.</param>
    /// <returns>The account state if exists.  Otherwise <c>null</c>.
    /// </returns>
    /// <remarks>
    /// If an account state at the given <paramref name="address"/> has never been set
    /// by the evaluation up to the <see cref="Block"/> with <see cref="Block.Hash"/>
    /// of <paramref name="hashDigest"/>, returns <c>null</c> instead of throwing an exception.
    /// </remarks>
    public delegate IValue? StateGetter(
        Address address,
        BlockHash? hashDigest,
        StateCompleter stateCompleter);
}
