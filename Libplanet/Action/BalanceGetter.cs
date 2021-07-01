#nullable enable
using Libplanet.Assets;
using Libplanet.Blockchain;
using Libplanet.Blocks;

namespace Libplanet.Action
{
    /// <summary>
    /// An intermediary delegate for <see cref="AccountBalanceGetter"/>.
    /// </summary>
    /// <param name="address">The address of an account to read the state from.</param>
    /// <param name="currency">The currency to look up.</param>
    /// <param name="hashDigest">The address of a block to read the state from.</param>
    /// <param name="fungibleAssetStateCompleter">The <see cref="FungibleAssetStateCompleter"/>
    /// to use.</param>
    /// <returns>
    /// The balance of the <paramref name="currency"/> at <paramref name="address"/>.
    /// </returns>
    /// <remarks>
    /// If the account state at <paramref name="address"/> has never been set by the evaluation
    /// up to the <see cref="Block"/> with <see cref="Block.Hash"/>
    /// of <paramref name="hashDigest"/>, returns <c>null</c> instead of throwing an exception.
    /// </remarks>
    public delegate FungibleAssetValue BalanceGetter(
        Address address,
        Currency currency,
        BlockHash? hashDigest,
        FungibleAssetStateCompleter fungibleAssetStateCompleter);
}
