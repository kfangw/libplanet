#nullable enable
using System;
using System.Collections.Immutable;
using Bencodex.Types;
using Libplanet.Blockchain;
using Libplanet.Blocks;

namespace Libplanet.Store
{
    /// <summary>
    /// An interface to store states.
    /// </summary>
    public interface IStateStore
    {
        /// <summary>
        /// Sets states mapped as relation <see cref="Block.Hash"/> → states.
        /// It guarantees <see cref="GetState"/> will return the same state if you passed same
        /// <paramref name="block"/> unless it has overwritten.
        /// </summary>
        /// <param name="block">The <see cref="Block"/> to set states.</param>
        /// <param name="states">The dictionary of state keys to states.</param>
        void SetStates(
            Block block,
            IImmutableDictionary<string, IValue> states);

        /// <summary>
        /// Gets state queried by <paramref name="stateKey"/> in the point,
        /// <paramref name="blockHash"/>.
        /// </summary>
        /// <param name="stateKey">The key to query state.</param>
        /// <param name="blockHash">The <see cref="Block.Hash"/> which the point to query by
        /// <paramref name="stateKey"/> at.</param>
        /// <returns>The state queried from <paramref name="blockHash"/> and
        /// <paramref name="stateKey"/>. If it couldn't find state, returns <c>null</c>.</returns>
        IValue? GetState(string stateKey, BlockHash? blockHash = null);

        /// <summary>
        /// Checks if the states corresponded to the block derived from <paramref name="blockHash"/>
        /// exist.
        /// </summary>
        /// <param name="blockHash">The <see cref="Block.Hash"/> of <see cref="Block"/>.
        /// </param>
        /// <returns>Whether it contains the block states corresponded to
        /// <paramref name="blockHash"/>.
        /// </returns>
        bool ContainsBlockStates(BlockHash blockHash);

        /// <summary>
        /// Copies metadata related to states from <paramref name="sourceChainId"/> to
        /// <paramref name="destinationChainId"/>, with <paramref name="branchpoint"/>.
        /// </summary>
        /// <param name="sourceChainId">The <see cref="BlockChain.Id"/> of the chain which
        /// copies from.</param>
        /// <param name="destinationChainId">The <see cref="BlockChain.Id"/> of the chain which
        /// copies to.</param>
        /// <param name="branchpoint">The branchpoint to begin coping.</param>
        void ForkStates(
            Guid sourceChainId,
            Guid destinationChainId,
            Block branchpoint);
    }
}
