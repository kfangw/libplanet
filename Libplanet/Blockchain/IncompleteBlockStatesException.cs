#nullable enable
using System;
using Libplanet.Blocks;

namespace Libplanet.Blockchain
{
    /// <summary>
    /// The exception that is thrown when a <see cref="BlockChain"/> have
    /// not calculated the complete states for all blocks, but an operation
    /// that needs some lacked states is requested.
    /// </summary>
    public class IncompleteBlockStatesException : Exception
    {
        /// <summary>
        /// Creates a new <see cref="IncompleteBlockStatesException"/> object.
        /// </summary>
        /// <param name="blockHash">Specifies <see cref="BlockHash"/>.
        /// It is automatically included to the <see cref="Exception.Message"/>
        /// string.</param>
        /// <param name="message">Specifies the <see cref="Exception.Message"/>.
        /// </param>
        public IncompleteBlockStatesException(
            BlockHash blockHash,
            string? message = null)
            : base(
                message is null
                    ? $"The block {blockHash} lacks its states"
                    : $"{message}\nThe Block that lacks its states: {blockHash}")
        {
            BlockHash = blockHash;
        }

        /// <summary>
        /// The <see cref="Block.Hash"/> of <see cref="Block"/> that
        /// a <see cref="BlockChain"/> lacks the states.
        /// </summary>
        public BlockHash BlockHash { get; }
    }
}
