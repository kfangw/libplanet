using System;
using Bencodex.Types;
using Libplanet.Blocks;

namespace Libplanet.Blockchain
{
    /// <summary>
    /// Predefined built-in state completers that satisfy <see cref="StateCompleter"/> delegate.
    /// </summary>
    public static class StateCompleters
    {
        /// <summary>
        /// Recalculates and complements a block's incomplete states on the fly.
        /// Incomplete states are filled with the recalculated states and the states are
        /// permanently remained in the store.
        /// </summary>
        public static readonly StateCompleter Recalculate = (blockChain, blockHash, address) =>
        {
            blockChain.ComplementBlockStates(blockHash);
            return blockChain.GetState(address, blockHash);
        };

        /// <summary>
        /// Rejects to complement incomplete state and throws
        /// an <see cref="IncompleteBlockStatesException"/>.
        /// </summary>
        public static readonly StateCompleter Reject = (chain, blockHash, address) =>
            throw new IncompleteBlockStatesException(blockHash);

        internal static Func<BlockChain, BlockHash, IValue> ToRawStateCompleter(
            StateCompleter stateCompleter,
            Address address
        ) =>
            (blockChain, hash) => stateCompleter(blockChain, hash, address);
    }
}
