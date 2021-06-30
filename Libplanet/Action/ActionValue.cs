using Bencodex.Types;

namespace Libplanet.Action
{
    public class ActionValue : IAction
    {
        public IValue PlainValue
        {
            get;
            private set;
        }

        public void LoadPlainValue(
            IValue plainValue)
        {
            PlainValue = plainValue;
        }

        public IAccountStateDelta Execute(IActionContext context)
        {
            return context.PreviousStates;
        }
    }
}
