#nullable enable
using System.Collections.Generic;
using Libplanet.Tx;

namespace Libplanet.Blockchain.Policies
{
    /// <summary>
    /// The interface to configure a <see cref="BlockChain"/>'s strategy to deal with staged
    /// transactions.
    /// </summary>
    /// <remarks>Every operation on an object implementing this must be thread-safe.</remarks>
    public interface IStagePolicy
    {
        /// <summary>
        /// Stages a <paramref name="transaction"/>.
        /// </summary>
        /// <param name="blockChain">The chain to stage the <paramref name="transaction"/>.
        /// </param>
        /// <param name="transaction">The <seealso cref="Transaction"/> to be staged.</param>
        /// <remarks>It does not throw any exception even if the <paramref name="transaction"/> has
        /// already been staged.  It does nothing either if the <paramref name="transaction"/> has
        /// marked as ignored (using <see cref="Ignore(BlockChain, TxId)"/> method).</remarks>
        public void Stage(BlockChain blockChain, Transaction transaction);

        /// <summary>
        /// Removes a transaction <paramref name="id"/> from the stage.
        /// </summary>
        /// <param name="blockChain">The chain to unstage the <paramref name="id"/>.</param>
        /// <param name="id">The <seealso cref="Transaction.Id"/> to remove.</param>
        /// <remarks>It does not throw any exception even if the <paramref name="id"/> has never
        /// been staged.</remarks>
        public void Unstage(BlockChain blockChain, TxId id);

        /// <summary>
        /// Marks a transaction as ignored, so that it will be never staged.
        /// </summary>
        /// <param name="blockChain">The chain that the stage belongs to.</param>
        /// <param name="id">The <see cref="Transaction.Id"/> to ignore.</param>
        /// <remarks>If the transaction is already in the stage, this method does nothing.</remarks>
        public void Ignore(BlockChain blockChain, TxId id);

        /// <summary>
        /// Checks if a transaction should be ignored for any reasons (for example, it is already
        /// staged or marked as ignored).
        /// </summary>
        /// <param name="blockChain">The chain that the stage belongs to.</param>
        /// <param name="id">The <see cref="Transaction.Id"/> to check.</param>
        /// <returns><c>true</c> if a transaction should be ignored.
        /// Otherwise, <c>false</c>.</returns>
        public bool Ignores(BlockChain blockChain, TxId id);

        /// <summary>
        /// Gets a staged <see cref="Transaction"/> by its <paramref name="id"/>.
        /// </summary>
        /// <param name="blockChain">The chain that the stage belongs to.</param>
        /// <param name="id">The <see cref="Transaction.Id"/> to get.</param>
        /// <param name="includeUnstaged">Whether to include transactions that had once staged but
        /// unstaged then.</param>
        /// <returns>A staged transaction if found.  If it had never staged <c>null</c> is
        /// returned.</returns>
        public Transaction? Get(BlockChain blockChain, TxId id, bool includeUnstaged);

        /// <summary>
        /// Enumerates all staged transaction IDs.
        /// </summary>
        /// <param name="blockChain">The chain of the stage to enumerate.</param>
        /// <returns>Staged transactions.  The earliest staged transaction goes first,
        /// and the latest staged transaction goes last.</returns>
        public IEnumerable<Transaction> Iterate(BlockChain blockChain);
    }
}
