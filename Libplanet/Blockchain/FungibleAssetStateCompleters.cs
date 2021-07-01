#nullable enable
using System;
using Bencodex.Types;
using Libplanet.Action;
using Libplanet.Assets;
using Libplanet.Blocks;

namespace Libplanet.Blockchain
{
    /// <summary>
    /// Predefined built-in state completers that satisfy
    /// <see cref="FungibleAssetStateCompleter"/> delegate.
    /// </summary>
    public static class FungibleAssetStateCompleters
    {
        /// <summary>
        /// Recalculates and complements a block's incomplete states on the fly.
        /// Incomplete states are filled with the recalculated states and the states are
        /// permanently remained in the store.
        /// </summary>
        public static readonly FungibleAssetStateCompleter Recalculate =
            (blockChain, blockHash, address, currency) =>
            {
                blockChain.ComplementBlockStates(blockHash);
                return blockChain.GetBalance(address, currency, blockHash);
            };

        /// <summary>
        /// Rejects to complement incomplete state and throws
        /// an <see cref="IncompleteBlockStatesException"/>.
        /// </summary>
        public static readonly FungibleAssetStateCompleter Reject =
            (chain, blockHash, address, currency) =>
                throw new IncompleteBlockStatesException(blockHash);

        internal static Func<BlockChain, BlockHash, IValue> ToRawStateCompleter(
            FungibleAssetStateCompleter stateCompleter,
            Address address,
            Currency currency
        ) =>
            (blockChain, hash) =>
            {
                FungibleAssetValue balance = stateCompleter(blockChain, hash, address, currency);
                return (Bencodex.Types.Integer)balance.RawValue;
            };
    }
}
