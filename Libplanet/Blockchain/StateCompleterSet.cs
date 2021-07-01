using Libplanet.Action;

namespace Libplanet.Blockchain
{
    /// <summary>
    /// Groups two kinds of state completers.
    /// </summary>
    public struct StateCompleterSet
    {
        /// <summary>
        /// Recalculates and complements a block's incomplete states on the fly.
        /// Incomplete states are filled with the recalculated states and the states are
        /// permanently remained in the store.
        /// </summary>
        public static readonly StateCompleterSet Recalculate = new StateCompleterSet
        {
            StateCompleter = StateCompleters.Recalculate,
            FungibleAssetStateCompleter = FungibleAssetStateCompleters.Recalculate,
        };

        /// <summary>
        /// Rejects to complement incomplete state and throws
        /// an <see cref="IncompleteBlockStatesException"/>.
        /// </summary>
        public static readonly StateCompleterSet Reject = new StateCompleterSet
        {
            StateCompleter = StateCompleters.Reject,
            FungibleAssetStateCompleter = FungibleAssetStateCompleters.Reject,
        };

        /// <summary>
        /// Holds a <see cref="StateCompleter"/>.
        /// </summary>
        public StateCompleter StateCompleter { get; set; }

        /// <summary>
        /// Holds a <see cref="FungibleAssetStateCompleter"/>.
        /// </summary>
        public FungibleAssetStateCompleter FungibleAssetStateCompleter { get; set; }
    }
}
