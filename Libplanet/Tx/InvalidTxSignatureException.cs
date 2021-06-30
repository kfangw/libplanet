#nullable enable
using System;

namespace Libplanet.Tx
{
    /// <summary>
    /// The exception that is thrown when a <see cref="Transaction"/>'s
    /// <see cref="Transaction.Signature"/> is invalid.
    /// </summary>
    [Serializable]
    public class InvalidTxSignatureException : InvalidTxException
    {
        /// <summary>
        /// Creates a new <see cref="InvalidTxSignatureException"/> object.
        /// </summary>
        /// <param name="txid">The invalid <see cref="Transaction"/>'s
        /// <see cref="Transaction.Id"/>.  It is automatically included to
        /// the <see cref="Exception.Message"/> string.</param>
        /// <param name="message">Specifies an <see cref="Exception.Message"/>.
        /// </param>
        public InvalidTxSignatureException(TxId txid, string message)
            : base(txid, message)
        {
        }
    }
}
